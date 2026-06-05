fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/latiq/v1/control.proto", "proto/latiq/v1/admin.proto"],
            &["proto"],
        )
        .expect("compile protos");
}
