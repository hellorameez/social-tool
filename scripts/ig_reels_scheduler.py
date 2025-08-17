import csv
import os
import time
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v20.0")
IG_USER_ID = os.environ["IG_USER_ID"]
ACCESS_TOKEN = os.environ["IG_ACCESS_TOKEN"]

MANIFEST = os.getenv("MANIFEST_PATH", "posts/manifest.csv")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "8"))
POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT_SECONDS", "180"))  # max ~3 minutes

IST = ZoneInfo("Asia/Kolkata")

BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"


def create_reel_container(video_url: str, caption: str, share_to_feed: bool, cover_url: str | None):
    """Create an IG container for a Reel."""
    url = f"{BASE}/{IG_USER_ID}/media"
    payload = {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": caption,
        "share_to_feed": "true" if share_to_feed else "false",
        "access_token": ACCESS_TOKEN,
    }
    if cover_url:
        payload["cover_url"] = cover_url

    r = requests.post(url, data=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data["id"]  # container ID


def poll_container_ready(container_id: str) -> bool:
    """Poll container until status_code == FINISHED or timeout."""
    url = f"{BASE}/{container_id}"
    params = {"fields": "status_code", "access_token": ACCESS_TOKEN}
    start = time.time()
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        status = r.json().get("status_code")
        if status == "FINISHED":
            return True
        if status in {"ERROR", "EXPIRED"}:
            raise RuntimeError(f"Container {container_id} failed with status: {status}")
        if time.time() - start > POLL_TIMEOUT:
            raise TimeoutError(f"Container {container_id} not ready within timeout.")
        time.sleep(POLL_INTERVAL)


def publish_container(container_id: str) -> str:
    url = f"{BASE}/{IG_USER_ID}/media_publish"
    payload = {"creation_id": container_id, "access_token": ACCESS_TOKEN}
    r = requests.post(url, data=payload, timeout=60)
    r.raise_for_status()
    return r.json().get("id", "")  # IG Media ID


def parse_bool(s: str) -> bool:
    return str(s).strip().lower() in {"1", "true", "yes", "y"}


def main():
    # Load manifest
    rows = []
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    now_ist = datetime.now(IST).replace(second=0, microsecond=0)

    changed = False
    for row in rows:
        if parse_bool(row.get("posted", "false")):
            continue

        # due?
        sched_str = row.get("scheduled_time_ist", "").strip()
        if not sched_str:
            continue
        try:
            sched_dt = IST.localize(datetime.strptime(sched_str, "%Y-%m-%d %H:%M")) if sched_str and sched_str.endswith(":00Z-legacy") else datetime.strptime(sched_str, "%Y-%m-%d %H:%M").replace(tzinfo=IST)
        except Exception:
            print(f"[WARN] Bad time format: {sched_str}")
            continue

        if now_ist < sched_dt:
            continue  # not due yet

        video_url = row.get("video_url", "").strip()
        caption = row.get("caption", "").strip()
        share_to_feed = parse_bool(row.get("share_to_feed", "true"))
        cover_url = row.get("cover_url", "").strip() or None

        print(f"[INFO] Creating container for due reel: {video_url}")
        container_id = create_reel_container(video_url, caption, share_to_feed, cover_url)
        print(f"[OK] Container: {container_id}")

        print("[INFO] Polling container status...")
        poll_container_ready(container_id)
        print("[OK] Container ready, publishing…")

        media_id = publish_container(container_id)
        print(f"[OK] Published IG Media ID: {media_id}")

        row["posted"] = "true"
        changed = True

    # Save manifest if changed
    if changed:
        fieldnames = rows[0].keys() if rows else []
        with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print("[OK] Manifest updated.")
    else:
        print("[INFO] Nothing due to publish.")


if __name__ == "__main__":
    main()
