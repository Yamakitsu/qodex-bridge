@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist "config.toml" (
    copy /y "config.example.toml" "config.toml" >nul
    if errorlevel 1 (
        echo 无法创建 config.toml，请确认当前目录可写。
        pause
        exit /b 1
    )
    echo 已创建 config.toml，请先完成配置，保存后再次双击本文件。
    start "" notepad.exe "config.toml"
    pause
    exit /b 0
)

echo 正在启动 Qodex Bridge ...
start "Qodex Bridge" "%~dp0QodexBridge.exe" --config "%~dp0config.toml"

timeout /t 4 /nobreak >nul

set TOKEN=
for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "Get-Content '%~dp0data/server.token' -ErrorAction SilentlyContinue"`) do set TOKEN=%%i

if "%TOKEN%"=="" (
    echo 服务可能仍在启动。请稍后访问 http://127.0.0.1:8765
) else (
    start "" "http://127.0.0.1:8765/#token=%TOKEN%"
)
