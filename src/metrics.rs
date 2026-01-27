use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

pub fn start_metrics_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;

    println!("📊 Configuring metrics server on port {}...", port);

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    println!("✅ Metrics server ready at http://{}/metrics", addr);
    Ok(())
}