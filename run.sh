#!/bin/bash
# ============================================================
# CFTC 持仓报告 — 一键运行脚本
# 用法：bash run.sh [选项]
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 检查 Python
if ! command -v python3 &>/dev/null; then
  echo "[ERROR] python3 not found. Please install Python 3.9+."
  exit 1
fi

# 检查依赖
python3 -c "import pandas, numpy, requests, yfinance" 2>/dev/null || {
  echo "[INFO] Installing dependencies..."
  pip3 install -r requirements.txt
}

echo ""
echo "=================================================="
echo "  CFTC 期货持仓报告生成器"
echo "=================================================="
echo ""

python3 cftc.py "$@"

# 如果生成成功，尝试在浏览器打开（macOS / Linux）
HTML=$(ls -t cftc_持仓报告_*.html 2>/dev/null | head -1)
if [ -n "$HTML" ]; then
  echo ""
  echo "[DONE] 报告已生成: $HTML"
  if command -v open &>/dev/null; then        # macOS
    open "$HTML"
  elif command -v xdg-open &>/dev/null; then  # Linux
    xdg-open "$HTML" &
  fi
fi
