import mysql.connector
from resources.dev import config


def get_mysql_connection():
    """
    Establishes and returns an active connection to the MySQL database 
    using configuration parameters.
    """
    try:
        connection = mysql.connector.connect(
            host=config.mysql_host,
            user=config.mysql_user,
            password=config.mysql_password,
            database=config.mysql_database
        )
        return connection
    except Exception as e:
        print(f"❌ Failed to connect to MySQL database: {e}")
        raise e


def execute_query(query, params=None):
    """
    Executes a DDL/DML SQL query (INSERT, UPDATE, DELETE) with optional parameters.
    """
    connection = None
    cursor = None
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        cursor.execute(query, params or ())
        connection.commit()
        return cursor.rowcount
    except Exception as e:
        if connection:
            connection.rollback()
        print(f"❌ Error executing query: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def expire_old_records(table_name, pk_column, month_column):
    """
    SCD Type 2 Record Expiration:
    Updates historical active records (is_current = 'Y') to expired (is_current = 'N')
    and sets eff_end_date when a newer active record exists for the same entity/month.
    """
    connection = None
    cursor = None
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()

        # Removed dependency on t1.id so it runs on tables without an 'id' primary key
        update_query = f"""
            UPDATE {table_name} t1
            JOIN {table_name} t2 
                ON t1.{pk_column} = t2.{pk_column} 
                AND t1.{month_column} = t2.{month_column}
            SET t1.is_current = 'N', 
                t1.eff_end_date = COALESCE(t2.eff_start_date, CURRENT_DATE())
            WHERE t2.is_current = 'Y' 
              AND t1.is_current = 'Y' 
              AND t1.eff_start_date < t2.eff_start_date;
        """

        cursor.execute(update_query)
        connection.commit()
        print(f"✅ SCD2: Successfully expired old records in {table_name}. Rows affected: {cursor.rowcount}")

    except Exception as e:
        if connection:
            connection.rollback()
        print(f"❌ Error during SCD2 expiry in {table_name}: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()