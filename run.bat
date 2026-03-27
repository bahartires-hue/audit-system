@echo off
cd /d C:\Users\DELL\OneDrive\Desktop\audit-system

python -m uvicorn main:app --host 127.0.0.1 --port 8000

echo.
echo ===== لو فيه خطأ بيظهر فوق =====
pause