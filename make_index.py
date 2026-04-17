import os

files = sorted(
    [f for f in os.listdir("reports") if f.endswith(".html") and f != "index.html"],
    reverse=True
)
latest = files[0] if files else ""
items = "\n".join(f'<li><a href="{f}">{f}</a></li>' for f in files)
html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta http-equiv="refresh" content="0; url={latest}">
<title>CFTC 持仓报告</title></head>
<body><h3>CFTC 持仓报告列表</h3><ul>
{items}
</ul></body></html>"""
with open("reports/index.html", "w", encoding="utf-8") as f:
    f.write(html)
print(f"index.html 生成成功，最新报告={latest}，共 {len(files)} 份")
