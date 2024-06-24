from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding, hashes, hmac
from cryptography.hazmat.backends import default_backend
import os

# 生成密钥
key = os.urandom(32)

# 生成初始化向量(IV)
iv = os.urandom(16)

cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
encryptor = cipher.encryptor()
decryptor = cipher.decryptor()
data = b"The quick brown fox jumps over the lazy dog"
padder = padding.PKCS7(128).padder()
padded_data = padder.update(data) + padder.finalize()
encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
print("encrypted data:", encrypted_data)
decrypted_padded_data = decryptor.update(encrypted_data) + decryptor.finalize()
unpadder = padding.PKCS7(128).unpadder()
decrypted_data = unpadder.update(decrypted_padded_data) + unpadder.finalize()
print("Original:", data.decode())
print("Decrypted:", decrypted_data.decode())
