use std::hash::{BuildHasher, Hasher};

pub struct RandomizingHasher {
    base_hasher: std::collections::hash_map::DefaultHasher,
}

impl Hasher for RandomizingHasher {
    fn finish(&self) -> u64 {
        self.base_hasher.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() == 8 {
            let id = u64::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);

            let randomized = id.wrapping_mul(0x9E3779B97F4A7C15);
            self.base_hasher.write(&randomized.to_ne_bytes());
        } else if bytes.len() == 32 {
            let mut mixed = [0; 32];
            for i in 0..32 {
                mixed[i] = bytes[(i * 13) % 32];
            }

            self.base_hasher.write(&mixed);
        } else {
            self.base_hasher.write(bytes);
        }
    }
}

#[derive(Clone)]
pub struct RandomizingHasherBuilder;

impl BuildHasher for RandomizingHasherBuilder {
    type Hasher = RandomizingHasher;

    fn build_hasher(&self) -> Self::Hasher {
        RandomizingHasher {
            base_hasher: std::collections::hash_map::DefaultHasher::new(),
        }
    }
}
