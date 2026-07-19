fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(
        &[
            "proto/xray/app/stats/command/command.proto",
            "proto/xray/app/proxyman/command/command.proto",
            "proto/xray/proxy/vless/account.proto",
        ],
        &["proto/xray"],
    )?;
    Ok(())
}
