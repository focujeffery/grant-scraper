import json
import mimetypes
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USERNAME = os.environ["SMTP_USERNAME"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
EMAIL_TO = os.environ["EMAIL_TO"]
EMAIL_FROM = os.environ.get("EMAIL_FROM", SMTP_USERNAME)
ATTACH_MAIN = Path(os.environ.get("ATTACH_MAIN", "outputs/daysee_grants.xlsx"))
ATTACH_DELTA = Path(os.environ.get("ATTACH_DELTA", "outputs/daysee_grants_delta_only.xlsx"))
STATS_JSON = os.environ.get("STATS_JSON", "{}")

stats = json.loads(STATS_JSON)
subject = f"[補助爬蟲週報] 新增{stats.get('new_count', 0)}筆 / 更新{stats.get('updated_count', 0)}筆 / 移除{stats.get('removed_count', 0)}筆"
body = f"""您好，

本週補助爬蟲已完成。

本週摘要：
- 目前總計畫數：{stats.get('current_count', 0)}
- 本週新增：{stats.get('new_count', 0)}
- 本週更新：{stats.get('updated_count', 0)}
- 本週移除：{stats.get('removed_count', 0)}

附件說明：
1. daysee_grants.xlsx：完整資料（含本週異動工作表）
2. daysee_grants_delta_only.xlsx：只有本週新增/更新/移除資訊

此信件由 GitHub Actions 自動寄出。
"""

msg = EmailMessage()
msg["Subject"] = subject
msg["From"] = EMAIL_FROM
msg["To"] = EMAIL_TO
msg.set_content(body)

for attachment in [ATTACH_MAIN, ATTACH_DELTA]:
    if attachment.exists():
        ctype, encoding = mimetypes.guess_type(str(attachment))
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(attachment, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=attachment.name)

with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as smtp:
    smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
    smtp.send_message(msg)
