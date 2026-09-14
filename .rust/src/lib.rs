pub mod data_register;
pub mod schema_version;
pub mod server;

pub mod pb {
    tonic::include_proto!("message.proto.v3");
}
