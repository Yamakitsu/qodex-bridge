@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo 未找到 .venv，请先运行安装命令：
    echo   python -m venv .venv
    echo   .venv\Scripts\python.exe -m pip install -e .
    pause
    exit /b 1
)

if not exist "config.toml" (
    copy /y "config.example.toml" "config.toml" >nul
    if errorlevel 1 (
        echo 无法从 config.example.toml 创建 config.toml。
        pause
        exit /b 1
    )
    echo 已自动创建 config.toml，请填写 QQ 白名单、NapCat 连接信息和项目路径。
    start "" notepad.exe "config.toml"
    pause
    exit /b 0
)

title QQ-Codex Bridge

echo 启动 QQ-Codex Bridge ...
start "QQ-Codex Bridge" ".venv\Scripts\python.exe" -m qq_codex_bridge --config config.toml

timeout /t 4 /nobreak >nul

set TOKEN=
for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "Get-Content data/server.token -ErrorAction SilentlyContinue"`) do set TOKEN=%%i

if "%TOKEN%"=="" (
    echo 未读取到 WebUI token，请手动访问 http://127.0.0.1:8765
) else (
    echo 正在打开 WebUI ...
    start "" "http://127.0.0.1:8765/#token=%TOKEN%"
)
