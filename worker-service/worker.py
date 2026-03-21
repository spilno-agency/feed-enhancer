"""
Feed Enhancer — Worker сервіс
Отримує задачі від Cloud Tasks і обробляє фіди
"""

import os
import gc
import io
import json
import hashlib
import shutil
import threading
from pathlib import Path
from datetime import datetime

import requests as http_requests
from flask import Flask, request, jsonify
from google.cloud import storage as gcs

# ─── GCS ──────────────────────────────────────────────────────────────────────
GCS_BUCKET = os.getenv("GCS_BUCKET", "feed-enhancer-490908-data")
gcs_client = gcs.Client()
bucket     = gcs_client.bucket(GCS_BUCKET)

def gcs_upload_file(local_path, gcs_path, content_type="application/octet-stream"):
    bucket.blob(gcs_path).upload_from_filename(local_path, content_type=content_type)

def gcs_upload_bytes(data, gcs_path, content_type="application/octet-stream"):
    bucket.blob(gcs_path).upload_from_string(data, content_type=content_type)

def gcs_download_bytes(gcs_path):
    return bucket.blob(gcs_path).download_as_bytes()

def gcs_download_to_file(gcs_path, local_path):
    bucket.blob(gcs_path).download_to_filename(local_path)

def gcs_exists(gcs_path):
    return bucket.blob(gcs_path).exists()

def gcs_delete(gcs_path):
    try: bucket.blob(gcs_path).delete()
    except: pass

def gcs_public_url(gcs_path):
    return f"https://storage.googleapis.com/{GCS_BUCKET}/{gcs_path}"

# ─── БД ───────────────────────────────────────────────────────────────────────
DB_GCS_PATH = "db/feeds.json"
db_lock     = threading.Lock()

def load_db():
    try:
        return json.loads(gcs_download_bytes(DB_GCS_PATH))
    except:
        return {"feeds": {}}

def save_db(db):
    gcs_upload_bytes(
        json.dumps(db, indent=2, ensure_ascii=False).encode(),
        DB_GCS_PATH, "application/json"
    )

def update_feed(feed_id, data):
    with db_lock:
        db = load_db()
        if feed_id not in db["feeds"]:
            db["feeds"][feed_id] = {}
        db["feeds"][feed_id].update(data)
        save_db(db)

def get_feed(feed_id):
    with db_lock:
        return load_db()["feeds"].get(feed_id)

def log_event(feed_id, level, message):
    with db_lock:
        db = load_db()
        if feed_id not in db["feeds"]: return
        logs = db["feeds"][feed_id].get("logs", [])
        logs.append({"ts": datetime.now().isoformat(), "level": level, "message": message})
        db["feeds"][feed_id]["logs"] = logs[-200:]
        save_db(db)

# ─── Утиліти ──────────────────────────────────────────────────────────────────
TMP_DIR = Path("/tmp/feed-worker")
TMP_DIR.mkdir(parents=True, exist_ok=True)

def download_feed_from_url(url, dest_path):
    resp = http_requests.get(url, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    ct = resp.headers.get("content-type", "")
    if resp.content[:5] not in (b"<?xml", b"<rss ", b"<feed", b"\xef\xbb\xbf"):
        if b"<item>" not in resp.content[:4096] and b"<entry" not in resp.content[:4096]:
            if "html" in ct and "xml" not in ct:
                raise ValueError("URL повернув HTML замість XML")
    with open(dest_path, "wb") as f:
        f.write(resp.content)

def public_feed_url(feed_id):
    server_url = os.getenv("SERVER_URL", "")
    return f"{server_url.rstrip('/')}/feeds/{feed_id}.xml"

# ─── Обробка ──────────────────────────────────────────────────────────────────
def run_processing(feed_id, input_gcs_path, cfg, source_url=None):
    local_input  = str(TMP_DIR / f"{feed_id}_input.xml")
    local_output = str(TMP_DIR / f"{feed_id}_output.xml")
    local_images = TMP_DIR / f"{feed_id}_images"
    local_images.mkdir(exist_ok=True)

    try:
        if source_url:
            update_feed(feed_id, {"status": "processing", "progress": "Завантаження фіду..."})
            log_event(feed_id, "info", f"Завантаження фіду з URL: {source_url}")
            download_feed_from_url(source_url, local_input)
            gcs_upload_file(local_input, input_gcs_path, "application/xml")
            update_feed(feed_id, {"last_fetched": datetime.now().isoformat()})
            log_event(feed_id, "success", "Фід завантажено")
            cfg["source_url"] = source_url
        else:
            gcs_download_to_file(input_gcs_path, local_input)

        update_feed(feed_id, {
            "status": "processing",
            "progress": "Обробка зображень...",
            "progress_current": 0,
            "progress_total": 0
        })
        log_event(feed_id, "info", f"Запуск обробки (стиль: {cfg.get('banner_style','classic')})")

        from feed_processor import DEFAULT_CONFIG, process_feed
        cfg["output_feed"] = local_output
        cfg["output_dir"]  = str(local_images)
        cfg["base_url"]    = gcs_public_url("images/")

        def on_progress(current, total, success, skipped):
            update_feed(feed_id, {
                "progress": f"Обробка: {current}/{total}",
                "progress_current": current,
                "progress_total":   total,
                "progress_success": success,
                "progress_skipped": skipped,
            })
            record = get_feed(feed_id)
            return bool(record and record.get("stop_requested"))

        stats = process_feed(local_input, cfg, progress_callback=on_progress)

        # Завантажуємо зображення в GCS батчами
        update_feed(feed_id, {"progress": "Завантаження зображень у хмару..."})
        log_event(feed_id, "info", "Завантаження зображень у GCS...")
        
        img_files = list(local_images.iterdir())
        total_imgs = len(img_files)
        for idx, img_file in enumerate(img_files, 1):
            if img_file.suffix in (".jpg", ".jpeg", ".png"):
                gcs_upload_file(str(img_file), f"images/{img_file.name}", "image/jpeg")
                img_file.unlink()  # видаляємо одразу після завантаження
            if idx % 50 == 0:
                log_event(feed_id, "info", f"Завантажено {idx}/{total_imgs} зображень у GCS")

        # XML результат у GCS
        result_gcs = f"results/{feed_id}.xml"
        gcs_upload_file(local_output, result_gcs, "application/xml")

        final_record = get_feed(feed_id)
        was_stopped  = final_record and final_record.get("stop_requested")

        update_feed(feed_id, {
            "status":           "done",
            "result_gcs_path":  result_gcs,
            "progress":         f"{'Зупинено' if was_stopped else 'Готово'}: {stats['success']}/{stats['total']}",
            "progress_current": stats["success"],
            "progress_total":   stats["total"],
            "finished_at":      datetime.now().isoformat(),
            "stats":            stats,
            "public_url":       public_feed_url(feed_id),
            "stop_requested":   False,
        })

        if was_stopped:
            log_event(feed_id, "warning", f"Зупинено: {stats['success']} з {stats['total']}")
        else:
            log_event(feed_id, "success",
                f"✅ Готово! {stats['success']} оброблено, {stats['skipped']} пропущено з {stats['total']}")

    except Exception as e:
        update_feed(feed_id, {"status": "error", "progress": str(e)})
        log_event(feed_id, "error", f"Помилка: {e}")
    finally:
        try: Path(local_input).unlink(missing_ok=True)
        except: pass
        try: Path(local_output).unlink(missing_ok=True)
        except: pass
        try: shutil.rmtree(str(local_images), ignore_errors=True)
        except: pass
        gc.collect()

# ─── Flask Worker App ──────────────────────────────────────────────────────────
worker_app = Flask(__name__)

@worker_app.route("/process", methods=["POST"])
def process_task():
    """Отримує задачу від Cloud Tasks і запускає обробку."""
    # Cloud Tasks надсилає JSON з параметрами
    data        = request.get_json(silent=True) or {}
    feed_id     = data.get("feed_id")
    source_url  = data.get("source_url")
    cfg         = data.get("cfg", {})
    input_gcs   = data.get("input_gcs_path", f"feeds/{feed_id}_input.xml")

    if not feed_id:
        return jsonify({"error": "feed_id required"}), 400

    try:
        run_processing(feed_id, input_gcs, cfg, source_url)
        return jsonify({"ok": True, "feed_id": feed_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@worker_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8081))
    worker_app.run(host="0.0.0.0", port=port, debug=False)
