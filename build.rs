//! Build script for Shinka gRPC code generation
//!
//! Compiles the proto/shinka.proto file into Rust code using tonic-build.
//! Only runs when the "grpc" feature is enabled and protoc is available.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        use std::process::Command;

        // Tell Cargo to rerun this if the proto file changes
        println!("cargo:rerun-if-changed=proto/shinka.proto");

        // Check if protoc is available
        let protoc_available = Command::new("protoc").arg("--version").output().is_ok();

        if protoc_available {
            // Create output directory if it doesn't exist
            std::fs::create_dir_all("src/api/generated")?;

            // Configure and compile the proto file
            tonic_build::configure()
                .build_server(true)
                .build_client(true)
                .out_dir("src/api/generated")
                .compile_protos(&["proto/shinka.proto"], &["proto"])?;
        } else {
            println!("cargo:warning=protoc not found, skipping gRPC code generation");
            println!("cargo:warning=Install protoc to enable gRPC support");
        }
    }

    Ok(())
}
