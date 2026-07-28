use std::env;
use std::path::PathBuf;

fn main() {
    // Tell Cargo to rerun if proto files change
    println!("cargo:rerun-if-changed=proto/message.proto");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    // Compile the proto file using tonic-build
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&["proto/message.proto"], &["proto"])
        .expect("Failed to compile proto files");

    println!("cargo:rerun-if-changed=proto/message.proto");
}
