from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
import base64
from configs.crypto_conf import AES_KEY, AES_IV


def encrypt_password(password: str) -> str:
    """
    비밀번호를 AES로 암호화하고 base64로 인코딩합니다.

    :param password: 암호화할 비밀번호
    :return: base64로 인코딩된 암호화된 비밀번호
    """
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(password.encode()) + padder.finalize()

    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

    return base64.urlsafe_b64encode(encrypted_data).decode()


def decrypt_password(encrypted_password: str) -> str:
    """
    base64로 인코딩된 암호화된 비밀번호를 복호화합니다.

    :param encrypted_password: base64로 인코딩된 암호화된 비밀번호
    :return: 복호화된 비밀번호 또는 오류 시 None
    """
    try:
        encrypted_data = base64.urlsafe_b64decode(encrypted_password)

        cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(AES_IV), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_padded_data = decryptor.update(encrypted_data) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        decrypted_data = unpadder.update(decrypted_padded_data) + unpadder.finalize()

        return decrypted_data.decode()
    except Exception as e:
        print(f"Error in decrypt_password: {e}")
        return None


# 사용 예시
if __name__ == "__main__":
    original_password = "MySecurePassword123!"
    encrypted = encrypt_password(original_password)
    decrypted = decrypt_password(encrypted)
    print(f"Original: {original_password}")
    print(f"Encrypted: {encrypted}")
    print(f"Decrypted: {decrypted}")