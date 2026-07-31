use std::env;
use std::path::Path;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protobuf_include = protoc_bin_vendored::include_path()?;
    env::set_var("PROTOC", protoc);

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("message_descriptor.bin");

    let upstream_proto = Path::new("../grpchook/message.proto");
    let vendored_proto = Path::new("grpchook/message.proto");

    let (proto_file, project_include): (&str, PathBuf) = if upstream_proto.exists() {
        ("../grpchook/message.proto", PathBuf::from(".."))
    } else if vendored_proto.exists() {
        ("grpchook/message.proto", PathBuf::from("."))
    } else {
        return Err(
            "message.proto not found; expected ../grpchook/message.proto or grpchook/message.proto"
                .into(),
        );
    };

    let include_dirs = [project_include, protobuf_include];

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(&[proto_file], &include_dirs)?;

    println!("cargo:rerun-if-changed=../grpchook/message.proto");
    println!("cargo:rerun-if-changed=grpchook/message.proto");

    Ok(())
}
