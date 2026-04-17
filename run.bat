@echo off
REM ============================================================
REM CFTC 持仓报告 — Windows 一键运行脚本
REM 用法：双击运行，或在命令行：run.bat [选项]
REM ============================================================

cd /d "%~dp0"

python --version >nul 2>&1 || (
    echo [ERROR] Python not found. Please install Python 3.9+ and add to PATH.
    pause
    exit /b 1
)

python -c "import pandas, numpy, requests, yfinance" >nul 2>&1 || (
    echo [INFO] Installing dependencies...
    pip install -r requirements.txt
)

echo.
echo ==================================================
echo   CFTC 期货持仓报告生成器
echo ==================================================
echo.

python cftc.py %*

REM 找最新的报告并打开
for /f "delims=" %%f in ('dir /b /o-d "cftc_持仓报告_*.html" 2^>nul') do (
    echo.
    echo [DONE] 报告已生成: %%f
    start "" "%%f"
    goto :done
)
:done
pause
