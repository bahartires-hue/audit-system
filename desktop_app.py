import webview
import threading
import uvicorn
import main

def start_server():
    uvicorn.run(main.app, host="127.0.0.1", port=8000)

# شغل السيرفر في الخلفية
threading.Thread(target=start_server, daemon=True).start()

# افتح نافذة برنامج
webview.create_window("Audit System", "http://127.0.0.1:8000")

webview.start()