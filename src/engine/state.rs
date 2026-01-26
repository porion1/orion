#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    Init,
    Running,
    Draining,
    Stopped,
}
