import base64
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
import os, sys
from resources.dev import config

try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise Exception(f"Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"Error occurred. Details : {e}")
    sys.exit(0)

BS = 16
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')
unpad = lambda s: s[0:-ord(s[-1:])]

def get_private_key():
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, Salt, 64, 1000)
    key32 = kdf[:32]
    return key32

def encrypt(raw):
    raw = pad(raw)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))

def decrypt(enc):
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode('utf8')

# --- RUNS ONLY WHEN EXECUTED DIRECTLY ---
if __name__ == "__main__":
    raw_access_key = "YOUR_AWS_ACCESS_KEY"
    raw_secret_key = "YOUR_AWS_ACCESS_KEY"

    print("--- COPY THESE TO YOUR CONFIG.PY ---")
    print(f'aws_access_key = "{encrypt(raw_access_key).decode("utf-8")}"')
    print(f'aws_secret_key = "{encrypt(raw_secret_key).decode("utf-8")}"')