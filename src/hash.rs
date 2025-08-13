use {rapidhash::v3::rapidhash_v3, std::hash::Hasher};

/// Default hasher for the keyspace.
///
/// This uses the rapidhash V3 algorithm for hashing keys.
/// For C++ compatibility, relies on the default seed and secrets.
///
/// The output is portable across platforms and major releases.
#[derive(Default)]
pub struct DefaultHasher(Vec<u8>);

impl Hasher for DefaultHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }

    fn finish(&self) -> u64 {
        rapidhash_v3(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::hash::{BuildHasher, BuildHasherDefault, Hasher},
    };

    #[test]
    fn sanity_checks() {
        // Ensure that the hasher produces consistent results.
        let data = b"hello world";
        let mut hasher1 = DefaultHasher(Vec::new());
        hasher1.write(data);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher(Vec::new());
        hasher2.write(data);
        let hash2 = hasher2.finish();
        assert_eq!(hash1, hash2, "Hashes should be equal for the same input");

        // Ensure that output stays the same across releases.
        let builder = BuildHasherDefault::<DefaultHasher>::default();
        assert_eq!(builder.hash_one("hello world"), 11123828800333028832);
        assert_eq!(builder.hash_one(42), 6826880404968503204);

        #[derive(Hash)]
        struct MyStruct {
            field1: u32,
            field2: String,
        }
        let my_struct = MyStruct {
            field1: 123,
            field2: "test".to_string(),
        };
        assert_eq!(builder.hash_one(my_struct), 17347315807818014607);
    }
}
