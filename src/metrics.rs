use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tracing::info;

pub fn start_metrics_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;

    info!("📊 Configuring metrics server on port {}...", port);

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    info!("✅ Metrics server ready at http://{}/metrics", addr);
    Ok(())
}
