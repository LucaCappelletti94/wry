---
"wry": patch:bug
---

On Android, run the IPC handler on the thread that built the webview instead of a Java thread. That thread must be a non-UI thread with a polled looper, as `tao` provides, or building the webview fails with `Error::AndroidIpcRequiresOwnerLooper`.
