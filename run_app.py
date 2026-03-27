import webbrowser
import threading
import time
import uvicorn
import main

def open_browser():
    time.sleep(2)
    webbrowser.open("http://127.0.0.1:8000")

threading.Thread(target=open_browser).start()

uvicorn.run(main.app, host="127.0.0.1", port=8000)