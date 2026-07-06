// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::is_dev;

fn main() {
    if is_dev() {
        // これが無いとGnome+Wayland+Nvidia環境ではwebkit関連のエラーにより起動できない。
        std::env::set_var("__NV_DISABLE_EXPLICIT_SYNC", "1");
    }
    deilephila_lib::run()
}
