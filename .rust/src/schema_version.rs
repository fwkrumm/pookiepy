use std::sync::OnceLock;

use prost::Message as _;
use prost_types::FileDescriptorSet;
use sha2::{Digest, Sha256};

pub const SCHEMA_VERSION_METADATA_KEY: &str = "x-schema-version";

static SCHEMA_VERSION: OnceLock<String> = OnceLock::new();

/// Return proto schema fingerprint compatible with Python implementation.
pub fn schema_version() -> &'static str {
    SCHEMA_VERSION.get_or_init(|| {
        let descriptor_bytes = include_bytes!(concat!(env!("OUT_DIR"), "/message_descriptor.bin"));
        let file_set = match FileDescriptorSet::decode(descriptor_bytes.as_ref()) {
            Ok(value) => value,
            Err(err) => panic!("failed to decode descriptor set: {err}"),
        };

        let file_proto = match file_set
            .file
            .iter()
            .find(|f| f.name.as_deref().is_some_and(|name| name.ends_with("message.proto")))
        {
            Some(value) => value,
            None => panic!("message.proto descriptor not found in descriptor set"),
        };

        let serialized = file_proto.encode_to_vec();
        let mut hasher = Sha256::new();
        hasher.update(serialized);
        let digest = hasher.finalize();
        format!("{:x}", digest)[0..16].to_owned()
    })
}
