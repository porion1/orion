// src/metrics_helpers.rs
pub fn gauge(name: &'static str, value: f64, labels: &[(&'static str, String)]) {
    if labels.is_empty() {
        metrics::gauge!(name, value);
    } else {
        // Convert labels to the format your metrics version expects
        let label_vec: Vec<(&'static str, String)> = labels.to_vec();
        // Implementation depends on your metrics version
    }
}

pub fn counter(name: &'static str, value: u64, labels: &[(&'static str, String)]) {
    if labels.is_empty() {
        metrics::counter!(name, value);
    } else {
        // Similar implementation
    }
}