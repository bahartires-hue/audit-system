Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /c cd /d C:\Users\DELL\OneDrive\Desktop\audit-system && python -m uvicorn main:app --host 127.0.0.1 --port 8000", 0