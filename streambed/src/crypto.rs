//! Helpers to encrypt and decrypt things, particularly message payloads.

use aes::{
    cipher::{generic_array::GenericArray, BlockEncrypt},
    Aes128,
};
use rand::RngCore;
use sha2::{Digest, Sha256, Sha512};

use hmac::Hmac;
type HmacSha256 = Hmac<Sha256>;

const BLOCKSIZE: usize = 16;

pub const KEY_SIZE: usize = 16;
pub const SALT_SIZE: usize = 12;

/// Generate a random salt for the required SALT_SIZE.
pub fn salt<T>(rng: &mut T) -> [u8; SALT_SIZE]
where
    T: RngCore,
{
    let mut bytes = [0; SALT_SIZE];
    rng.fill_bytes(&mut bytes);
    bytes
}

/// Performs an AES-128 CTR-blockmode-style decryption as used by
/// LoRaWAN FRMPayloads. For more information on CTR:
/// https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_(CTR)
/// This is the same as the `encrypt` function, but named differently
/// to convey intent.
#[inline]
pub fn decrypt(bytes: &mut [u8], key: &[u8; KEY_SIZE], salt: &[u8; SALT_SIZE]) {
    encrypt(bytes, key, salt)
}

/// Performs an AES-128 CTR-blockmode-style encryption as used by
/// LoRaWAN FRMPayloads. For more information on CTR:
/// https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_(CTR)
pub fn encrypt(bytes: &mut [u8], key: &[u8; KEY_SIZE], salt: &[u8; SALT_SIZE]) {
    use aes::cipher::KeyInit;
    let key = GenericArray::from_slice(key);
    let cipher = Aes128::new(key);

    let bytes_len = bytes.len();

    let block_count = (bytes_len.max(1) - 1) / BLOCKSIZE + 1;
    let mut blocks = Vec::with_capacity(block_count);
    let mut block = Vec::<u8>::with_capacity(BLOCKSIZE);
    for i in 1..=block_count {
        block.clear();
        let bytes = (i as u32).to_be_bytes();
        block.extend_from_slice(&bytes);
        block.extend_from_slice(salt);
        blocks.push(GenericArray::clone_from_slice(&block));
    }
    cipher.encrypt_blocks(&mut blocks);

    let mut offset = 0;
    'outer: for block in blocks {
        for b in block {
            if offset < bytes_len {
                bytes[offset] ^= b;
                offset += 1;
            } else {
                break 'outer;
            }
        }
    }
}

/// Sign an array of bytes using a key. Given the same bytes and key, this will always
/// result in the same output.
pub fn sign(bytes: &[u8], key: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

/// Verify that a signed value is equal to an original array of bytes
pub fn verify(bytes: &[u8], key: &[u8], compare_bytes: &[u8]) -> bool {
    use hmac::Mac;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(bytes);
    mac.verify_slice(compare_bytes).is_ok()
}

/// Hash an array of bytes with a salt. Yields a SHA-512 hash.
pub fn hash(bytes: &[u8], salt: &[u8; SALT_SIZE]) -> Vec<u8> {
    let mut hasher = Sha512::new();
    hasher.update(salt);
    hasher.update(bytes);
    let mut hash = salt.to_vec();
    hash.extend(hasher.finalize());
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt() {
        let bytes = b"The quick brown fox jumps over the lazy dog.";
        let key = [0_u8; KEY_SIZE];
        let salt = [0_u8; SALT_SIZE];
        let mut encrypted_bytes = *bytes;
        encrypt(&mut encrypted_bytes, &key, &salt);
        assert_eq!(hex::encode(
          encrypted_bytes
        ), "bafc2e0f979c95ebeb6202ffc61631558b5b052e6b5d836a00b2cea50fc5aadd461c7ee09b333a0761a0796e");
        let mut decrypted_bytes = encrypted_bytes;
        decrypt(&mut decrypted_bytes, &key, &salt);
        assert_eq!(&decrypted_bytes, bytes);
    }

    #[test]
    fn test_sign() {
        let bytes = b"The quick brown fox jumps over the lazy dog.";
        let key = b"changeme";
        let hashed = sign(bytes, key);
        assert_eq!(
            hex::encode(hashed.clone()),
            "ed31e94c161aea6ff2300c72b17741f71b616463f294dac0542324bbdbf8a2de"
        );
        assert!(verify(bytes, key, &hashed));
        assert!(!verify(bytes, key, b"rubbish"));
    }

    #[test]
    fn test_hash() {
        let bytes = b"The quick brown fox jumps over the lazy dog.";
        let salt = [0_u8; SALT_SIZE];
        let hashed = hash(bytes, &salt);
        assert_eq!(hex::encode(
            hashed
        ), "00000000000000000000000027cbc61af1b821ec863e77076df08016f41eb583d668426520b8ce5c85c150ada36f89061955c1bd12340764ee471f370528326dfa060a8c98978fb25774b35d");
    }
}
