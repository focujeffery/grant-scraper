import json
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path


def getenv_clean(name: str, default: str = "") -> str:
    value = os.environ.get(name, default)
    if value is None:
        return default
    return value.strip()


def parse_port(value: str, default: int = 465) -> int:
    value = (value or "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


SMTP_HOST = getenv_clean("SMTP_HOST")
SMTP_PORT = parse_port(getenv_clean("SMTP_PORT"), 465)
SMTP_USERNAME = getenv_clean("SMTP_USERNAME")
SMTP_PASSWORD = getenv_clean("SMTP_PASSWORD")
EMAIL_TO = getenv_clean("EMAIL_TO")
EMAIL_FROM = getenv_clean("EMAIL_FROM")
ATTACH_MAIN = getenv_clean("ATTACH_MAIN", "outputs/daysee_grants.xlsx")
ATTACH_DELTA = getenv_clean("ATTACH_DELTA", "outputs/daysee_grants_delta_only.xlsx")
STATS_JSON = getenv_clean("STATS_JSON", "{}")


required = {
    "SMTP_HOST": SMTP_HOST,
    "SMTP_USERNAME": SMTP_USERNAME,
    "SMTP_PASSWORD": SMTP_PASSWORD,
    "EMAIL_TO": EMAIL_TO,
    "EMAIL_FROM": EMAIL_FROM,
}
missing = [k for k, v in required.items() if not v]

if missing:
    print(
        "Email not configured. Skipping send. Missing:",
        ", ".join(missing),
    )
    raise SystemExit(0)

stats = {}
try:
    stats = json.loads(STATS_JSON or "{}")
except json.JSONDecodeError:
    stats = {}

msg = EmailMessage()
msg["Subject"] = "Weekly grant crawler report"
msg["From"] = EMAIL_FROM
msg["To"] = EMAIL_TO

body = [
    "Weekly grant crawler report attached.",
    "",
    f"Current plans: {stats.get('current_count', 'N/A')}",
    f"New this week: {stats.get('new_count', 'N/A')}",
    f"Updated this week: {stats.get('updated_count', 'N/A')}",
    f"Removed this week: {stats.get('removed_count', 'N/A')}",
]
msg.set_content("\n".join(body))

for path_str in [ATTACH_MAIN, ATTACH_DELTA]:
    if not path_str:
        continue
    path = Path(path_str)
    if not path.exists() or not path.is_file():
        print(f"Attachment not found, skipping: {path}")
        continue
    data = path.read_bytes()
    msg.add_attachment(
        data,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=path.name,
    )

with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
    server.login(SMTP_USERNAME, SMTP_PASSWORD)
    server.send_message(msg)

print("Email sent successfully.")
