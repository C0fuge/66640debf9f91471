//! Setup-only native file selection; no paths or dialog options enter the renderer.
use crate::SaveDialogCallback;

/// Shares one native provider reservation with the existing SaveDialog.
pub trait OpenFileDialog: Send + Sync {
    fn choose_open(&self, callback: SaveDialogCallback);
}
