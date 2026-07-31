fn main() {
    // Tell Cargo to rerun if proto files change
    println!("cargo:rerun-if-changed=proto/message.proto");

    // Compile the proto file using tonic-build
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&["proto/message.proto"], &["proto"])
        .expect("Failed to compile proto files");

    println!("cargo:rerun-if-changed=proto/message.proto");
}
