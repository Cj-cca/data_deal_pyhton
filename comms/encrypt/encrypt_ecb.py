import base64
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding, serialization
from cryptography.hazmat.backends import default_backend


def _read_key(key_file_path):
    try:
        # 指定文件路径
        file_path = key_file_path

        # 使用with语句打开文件，这样可以确保文件在操作完成后自动关闭
        with open(file_path, 'r') as file:
            # 读取文件全部内容
            content = file.read()

            # 打印文件内容
            return content
    except FileNotFoundError:
        print(f"错误：文件 {key_file_path} 未找到。")
    except IOError as e:
        print(f"读取文件时发生错误：{e}")
    except Exception as e:
        print(f"发生未知错误：{e}")


class AESUtil:
    def __init__(self):
        self.ALGORITHM = "AES"
        self.CHARSET = "utf-8"
        self.KEY = _read_key('/data1/datax/conf/info.secret').encode(self.CHARSET)

    def _pad(self, text):
        padder = padding.PKCS7(128).padder()
        padded_text = padder.update(text.encode(self.CHARSET)) + padder.finalize()
        return padded_text

    def _unpad(self, padded_text):
        unpadder = padding.PKCS7(128).unpadder()
        text = unpadder.update(padded_text) + unpadder.finalize()
        return text.decode(self.CHARSET)

    def encrypt(self, content):
        backend = default_backend()
        cipher = Cipher(algorithms.AES(self.KEY), modes.ECB(), backend=backend)
        encryptor = cipher.encryptor()
        padded_content = self._pad(content)
        ciphertext = encryptor.update(padded_content) + encryptor.finalize()
        return base64.b64encode(ciphertext).decode(self.CHARSET)

    def decrypt(self, content):
        backend = default_backend()
        cipher = Cipher(algorithms.AES(self.KEY), modes.ECB(), backend=backend)
        decryptor = cipher.decryptor()
        ciphertext = base64.b64decode(content)
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
        return self._unpad(padded_plaintext)


if __name__ == "__main__":
    aes_util = AESUtil()
    encrypted = aes_util.encrypt("P@ss1234567890")
    print(f"Encrypted: {encrypted}")
    decrypted = aes_util.decrypt(encrypted)
    print(f"Decrypted: {decrypted}")
