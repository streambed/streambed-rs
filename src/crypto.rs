use aes::{
    cipher::{generic_array::GenericArray, BlockEncrypt, KeyInit},
    Aes128,
};
use rand::RngCore;

const BLOCKSIZE: usize = 16;

pub const SALT_SIZE: usize = 12;

/// Generate a random salt for the required SALT_SIZE.
pub fn salt<T>(rng: &mut T) -> Vec<u8>
where
    T: RngCore,
{
    let mut bytes = vec![0; SALT_SIZE];
    rng.fill_bytes(&mut bytes);
    bytes
}

/// Performs an AES-128 ECB decryption using a salt and a 16 byte key
#[inline]
pub fn decrypt(bytes: &mut [u8], key: &[u8], salt: &[u8]) {
    encrypt(bytes, key, salt)
}

/// Performs an AES-128 ECB encryption using a salt and a 16 byte key
pub fn encrypt(bytes: &mut [u8], key: &[u8], salt: &[u8]) {
    debug_assert!(salt.len() == SALT_SIZE);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt() {
        let bytes = b"The quick brown fox jumps over the lazy dog.";
        let key = [0_u8; 16];
        let salt = [0_u8; 12];
        let mut encrypted_bytes = *bytes;
        encrypt(&mut encrypted_bytes, &key, &salt);
        assert_eq!(hex::encode(
          encrypted_bytes
        ), "bafc2e0f979c95ebeb6202ffc61631558b5b052e6b5d836a00b2cea50fc5aadd461c7ee09b333a0761a0796e");
        let mut decrypted_bytes = encrypted_bytes;
        decrypt(&mut decrypted_bytes, &key, &salt);
        assert_eq!(&decrypted_bytes, bytes);
    }
}
