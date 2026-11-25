use ch_app::{App, Invalidation};
use serde_json::Value;
use std::sync::Arc;
use tauri::{Emitter, Manager, RunEvent};
use tokio::sync::broadcast::error::RecvError;

#[cfg(target_os = "linux")]
mod save_dialog;

#[tauri::command]
async fn core_request(request: Value, app: tauri::State<'_, Arc<App>>) -> Result<Value, ()> {
    Ok(app.request(request).await)
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let demo = match (args.next(), args.next(), args.next()) {
        (None, None, None) => None,
        (Some(flag), Some(root), None) if flag == "--demo" => Some(std::path::PathBuf::from(root)),
        _ => {
            eprintln!("usage: contact-hub [--demo <rebuild-root>]");
            std::process::exit(2);
        }
    };
    let app=tauri::Builder::default()
        .setup(move|tauri_app|{
            let plugins=tauri_app.path().resource_dir()?.join("plugins");
            #[cfg(target_os = "linux")]
            let is_demo = demo.is_some();
            #[allow(unused_mut)] // Provider injection is Linux-only.
            let mut app=tauri::async_runtime::block_on(async{
                match demo{Some(root)=>App::demo(&root).await,None=>App::desktop(plugins).await}
            }).map_err(|_|std::io::Error::other("Could not open Contact Hub. Check the data directory, running instance, plugin installation and secret service."))?;
            #[cfg(target_os = "linux")]
            if !is_demo {
                app.set_save_dialog(Arc::new(save_dialog::PortalSaveDialog::default()));
            }
            let mut rx=app.subscribe();
            let stop=app.shutdown_signal();
            let handle=tauri_app.handle().clone();
            tauri::async_runtime::spawn(async move{
                loop{
                    let next=tokio::select!{_=stop.cancelled()=>break, result=rx.recv()=>result};
                    let event=match next{Ok(e)=>e,Err(RecvError::Lagged(_))=>Invalidation::Reset,Err(RecvError::Closed)=>break};
                    let _=handle.emit("core-change",event);
                }
            });
            tauri_app.manage(Arc::new(app));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![core_request])
        .build(tauri::generate_context!());
    let app = match app {
        Ok(app) => app,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    app.run(|handle, event| {
        if let RunEvent::Exit = event
            && let Some(app) = handle.try_state::<Arc<App>>()
        {
            tauri::async_runtime::block_on(app.shutdown());
        }
    });
}
