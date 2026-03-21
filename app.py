"""
Feed Enhancer — API сервіс
Приймає запити від браузера, створює задачі в Cloud Tasks для обробки
"""

import os
import io
import uuid
import json
import time
import random
import hashlib
import threading
from pathlib import Path
from datetime import datetime

import requests as http_requests
from flask import Flask, request, jsonify, send_from_directory, Response
from google.cloud import storage as gcs
from google.cloud import tasks_v2
from google.protobuf import duration_pb2

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

# ─── Cloud Tasks ──────────────────────────────────────────────────────────────
TASKS_CLIENT   = tasks_v2.CloudTasksClient()
GCP_PROJECT    = os.getenv("GCP_PROJECT",    "feed-enhancer-490908")
GCP_LOCATION   = os.getenv("GCP_LOCATION",   "europe-west1")
TASKS_QUEUE    = os.getenv("TASKS_QUEUE",    "feed-processing-queue")
WORKER_URL     = os.getenv("WORKER_URL",     "")  # URL воркер-сервісу

def enqueue_processing(feed_id, input_gcs_path, cfg, source_url=None):
    """Додає задачу обробки фіду в Cloud Tasks."""
    parent = TASKS_CLIENT.queue_path(GCP_PROJECT, GCP_LOCATION, TASKS_QUEUE)
    
    payload = json.dumps({
        "feed_id":        feed_id,
        "input_gcs_path": input_gcs_path,
        "cfg":            cfg,
        "source_url":     source_url,
    }).encode()

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url":         f"{WORKER_URL}/process",
            "headers":     {"Content-Type": "application/json"},
            "body":        payload,
            "oidc_token":  {"service_account_email": os.getenv("SERVICE_ACCOUNT", "")},
        },
        "dispatch_deadline": duration_pb2.Duration(seconds=3600),
    }

    TASKS_CLIENT.create_task(request={"parent": parent, "task": task})

# ─── Конфігурація ─────────────────────────────────────────────────────────────
app        = Flask(__name__, static_folder="static")
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8080")
TMP_DIR    = Path("/tmp/feed-api")
TMP_DIR.mkdir(parents=True, exist_ok=True)

# ─── БД у GCS ─────────────────────────────────────────────────────────────────
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

def get_feed(feed_id):
    with db_lock:
        return load_db()["feeds"].get(feed_id)

def update_feed(feed_id, data):
    with db_lock:
        db = load_db()
        if feed_id not in db["feeds"]:
            db["feeds"][feed_id] = {}
        db["feeds"][feed_id].update(data)
        save_db(db)

def delete_feed_from_db(feed_id):
    with db_lock:
        db = load_db()
        db["feeds"].pop(feed_id, None)
        save_db(db)

def log_event(feed_id, level, message):
    with db_lock:
        db = load_db()
        if feed_id not in db["feeds"]: return
        logs = db["feeds"][feed_id].get("logs", [])
        logs.append({"ts": datetime.now().isoformat(), "level": level, "message": message})
        db["feeds"][feed_id]["logs"] = logs[-200:]
        save_db(db)

def public_feed_url(feed_id):
    return f"{SERVER_URL.rstrip('/')}/feeds/{feed_id}.xml"

# ─── Утиліти ──────────────────────────────────────────────────────────────────
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

# ─── Планувальник автооновлення ───────────────────────────────────────────────
scheduler_started = False
scheduler_lock    = threading.Lock()

def scheduler_loop():
    while True:
        time.sleep(60)
        try:
            db  = load_db()
            now = datetime.now()
            for feed_id, record in db["feeds"].items():
                sched = record.get("schedule", {})
                if not sched.get("enabled"): continue
                if record.get("status") == "processing": continue
                if record.get("source_type") != "url": continue
                run_time = sched.get("run_time", "06:00")
                try:    run_h, run_m = map(int, run_time.split(":"))
                except: run_h, run_m = 6, 0
                if now.hour != run_h or now.minute != run_m: continue
                last_run = sched.get("last_run")
                if last_run:
                    from datetime import datetime as dt2
                    if dt2.fromisoformat(last_run).date() == now.date(): continue
                cfg = {**record.get("config", {})}
                update_feed(feed_id, {
                    "status": "processing",
                    "progress": f"Автооновлення о {run_time}...",
                    "schedule": {**sched, "last_run": now.isoformat()},
                })
                log_event(feed_id, "info", f"Автооновлення о {run_time}")
                enqueue_processing(feed_id, record.get("input_gcs_path",""), cfg, record.get("source_url"))
        except Exception as e:
            print(f"[scheduler] {e}")

def start_scheduler():
    global scheduler_started
    with scheduler_lock:
        if not scheduler_started:
            scheduler_started = True
            threading.Thread(target=scheduler_loop, daemon=True).start()

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/feeds/<feed_id>.xml")
def serve_feed(feed_id):
    record = get_feed(feed_id)
    if not record or record.get("status") != "done":
        return "Фід не готовий", 404
    gcs_path = record.get("result_gcs_path")
    if not gcs_path or not gcs_exists(gcs_path):
        return "XML не знайдено", 404
    xml_bytes = gcs_download_bytes(gcs_path)
    return Response(xml_bytes, mimetype="application/xml",
                    headers={"Content-Disposition": f"inline; filename={feed_id}.xml"})

@app.route("/api/feeds", methods=["GET"])
def list_feeds():
    with db_lock:
        db = load_db()
    feeds = sorted(db["feeds"].values(), key=lambda x: x.get("created_at",""), reverse=True)
    return jsonify(feeds)

@app.route("/api/feeds", methods=["POST"])
def add_feed():
    feed_url = request.form.get("feed_url","").strip()
    has_file = "file" in request.files and request.files["file"].filename
    if not feed_url and not has_file:
        return jsonify({"error": "Надішліть файл або URL"}), 400

    feed_id        = str(uuid.uuid4())[:8]
    input_gcs_path = f"feeds/{feed_id}_input.xml"

    if feed_url:
        feed_name = request.form.get("name") or feed_url.split("/")[-1].split("?")[0] or "feed.xml"
    else:
        file = request.files["file"]
        if not file.filename.endswith(".xml"):
            return jsonify({"error": "Потрібен XML файл"}), 400
        feed_name = request.form.get("name") or file.filename
        gcs_upload_bytes(file.read(), input_gcs_path, "application/xml")
        feed_url = None

    from feed_processor import DEFAULT_CONFIG
    cfg = {**DEFAULT_CONFIG}
    cfg["border_color"]    = request.form.get("border_color",    cfg["border_color"])
    cfg["border_width"]    = int(request.form.get("border_width", cfg["border_width"]))
    cfg["badge_position"]  = request.form.get("badge_position",  cfg["badge_position"])
    cfg["badge_font_size"] = int(request.form.get("badge_font_size", cfg["badge_font_size"]))
    cfg["banner_style"]    = request.form.get("banner_style",    "classic")
    cfg["domain"]          = request.form.get("domain",          "")
    cfg["domain_position"] = request.form.get("domain_position", "bottom-left")

    record = {
        "id":              feed_id,
        "name":            feed_name,
        "source_type":     "url" if feed_url else "file",
        "source_url":      feed_url,
        "status":          "pending",
        "progress":        "Очікує обробки",
        "input_gcs_path":  input_gcs_path,
        "result_gcs_path": None,
        "public_url":      None,
        "created_at":      datetime.now().isoformat(),
        "finished_at":     None,
        "last_fetched":    None,
        "schedule":        {"enabled": False, "run_time": "06:00", "interval_hours": 24, "last_run": None},
        "config":          {k: cfg[k] for k in ["border_color","border_width","badge_position",
                            "badge_font_size","banner_style","domain","domain_position"]},
    }
    update_feed(feed_id, record)
    log_event(feed_id, "info", f"Фід створено: {feed_name}")

    if request.form.get("auto_process","true").lower() == "true":
        update_feed(feed_id, {"status": "processing", "progress": "Задача створена..."})
        log_event(feed_id, "info", f"Стиль: {cfg['banner_style']}, домен: {cfg.get('domain','авто')}")
        enqueue_processing(feed_id, input_gcs_path, cfg, feed_url)

    return jsonify(record), 201

@app.route("/api/feeds/<feed_id>/process", methods=["POST"])
def process_feed_route(feed_id):
    record = get_feed(feed_id)
    if not record:   return jsonify({"error": "Фід не знайдено"}), 404
    if record["status"] == "processing": return jsonify({"error": "Вже обробляється"}), 409

    body = request.get_json(silent=True) or {}
    from feed_processor import DEFAULT_CONFIG
    cfg  = {**DEFAULT_CONFIG, **record.get("config", {})}
    for k in ("border_color","badge_position","banner_style","domain","domain_position"):
        if k in body: cfg[k] = body[k]
    for k in ("border_width","badge_font_size"):
        if k in body: cfg[k] = int(body[k])

    update_feed(feed_id, {
        "status": "processing", "progress": "Задача створена...",
        "config": {k: cfg[k] for k in ["border_color","border_width","badge_position",
                   "badge_font_size","banner_style","domain","domain_position"]},
    })
    log_event(feed_id, "info", f"Запущено обробку — стиль: {cfg['banner_style']}")
    enqueue_processing(feed_id, record["input_gcs_path"], cfg, None)
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/refresh", methods=["POST"])
def refresh_feed(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Фід не знайдено"}), 404
    if record.get("source_type") != "url" or not record.get("source_url"):
        return jsonify({"error": "Цей фід — файл"}), 400
    if record["status"] == "processing": return jsonify({"error": "Вже обробляється"}), 409

    body = request.get_json(silent=True) or {}
    from feed_processor import DEFAULT_CONFIG
    cfg  = {**DEFAULT_CONFIG, **record.get("config", {})}
    for k in ("border_color","badge_position","banner_style","domain","domain_position"):
        if k in body: cfg[k] = body[k]
    for k in ("border_width","badge_font_size"):
        if k in body: cfg[k] = int(body[k])

    update_feed(feed_id, {
        "status": "processing", "progress": "Задача створена...",
        "config": {k: cfg[k] for k in ["border_color","border_width","badge_position",
                   "badge_font_size","banner_style","domain","domain_position"]},
    })
    log_event(feed_id, "info", f"Оновлення з URL: {record['source_url']}")
    enqueue_processing(feed_id, record["input_gcs_path"], cfg, record["source_url"])
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/stop", methods=["POST"])
def stop_feed(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    update_feed(feed_id, {"stop_requested": True})
    log_event(feed_id, "warning", "Зупинка запрошена")
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/preview-one", methods=["POST"])
def preview_one(feed_id):
    import gc
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Фід не знайдено"}), 404
    if record.get("status") == "processing":
        return jsonify({"error": "Фід обробляється"}), 409

    input_gcs_path = record.get("input_gcs_path","")
    local_input    = str(TMP_DIR / f"{feed_id}_preview_input.xml")

    if not gcs_exists(input_gcs_path):
        source_url = record.get("source_url")
        if not source_url:
            return jsonify({"error": "XML не знайдено і URL не вказано"}), 404
        try:
            log_event(feed_id, "info", "Завантаження XML для превʼю...")
            download_feed_from_url(source_url, local_input)
            gcs_upload_file(local_input, input_gcs_path, "application/xml")
            update_feed(feed_id, {"last_fetched": datetime.now().isoformat()})
        except Exception as e:
            return jsonify({"error": f"Не вдалося завантажити: {e}"}), 502
    else:
        gcs_download_to_file(input_gcs_path, local_input)

    body = request.get_json(silent=True) or {}
    from feed_processor import DEFAULT_CONFIG
    cfg  = {**DEFAULT_CONFIG, **record.get("config", {})}
    for k in ("border_color","badge_position","banner_style","domain","domain_position"):
        if k in body and body[k]: cfg[k] = body[k]
    for k in ("border_width","badge_font_size"):
        if k in body and body[k]: cfg[k] = int(body[k])

    if not cfg.get("domain") and record.get("source_url"):
        try:
            from urllib.parse import urlparse
            cfg["domain"] = urlparse(record["source_url"]).netloc.replace("www.","")
        except: pass

    cfg["output_dir"] = str(TMP_DIR)
    cfg["base_url"]   = gcs_public_url("images/")

    try:
        from feed_processor import parse_feed, extract_item_data, download_image, enhance_image, save_image, safe_str
        _, items = parse_feed(local_input)
        if not items: return jsonify({"error": "Товари не знайдено"}), 404

        img_url = price = raw_id = ""
        for _ in range(20):
            item    = random.choice(items)
            data    = extract_item_data(item)
            img_url = safe_str(data.get("image_link",""))
            price   = safe_str(data.get("price",""))
            raw_id  = safe_str(data.get("id",""))
            if img_url: break

        if not img_url: return jsonify({"error": "Немає товару із зображенням"}), 404

        item_id      = raw_id or hashlib.md5(img_url.encode()).hexdigest()[:8]
        filename     = f"preview_{feed_id}_{item_id}.jpg"
        local_preview = str(TMP_DIR / filename)

        img = download_image(img_url, cfg)
        if img is None: return jsonify({"error": "Не вдалося завантажити зображення"}), 502

        enhanced = enhance_image(img, price, cfg)
        img.close()
        save_image(enhanced, filename, cfg)
        enhanced.close()

        gcs_upload_file(local_preview, f"images/{filename}", "image/jpeg")
        Path(local_preview).unlink(missing_ok=True)
        gc.collect()

        image_url = gcs_public_url(f"images/{filename}")
        log_event(feed_id, "info", f"Превʼю — ID={item_id}, стиль={cfg.get('banner_style','classic')}")
        return jsonify({"image_url": image_url, "item_id": item_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        Path(local_input).unlink(missing_ok=True)

@app.route("/api/feeds/<feed_id>/logs", methods=["DELETE"])
def clear_logs(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    update_feed(feed_id, {"logs": []})
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/schedule", methods=["PATCH"])
def set_schedule(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    if record.get("source_type") != "url":
        return jsonify({"error": "Тільки для URL-фідів"}), 400
    body    = request.get_json(silent=True) or {}
    current = record.get("schedule", {})
    new_s   = {
        "enabled":        bool(body.get("enabled", current.get("enabled", False))),
        "run_time":       body.get("run_time", current.get("run_time","06:00")),
        "interval_hours": 24,
        "last_run":       current.get("last_run"),
    }
    update_feed(feed_id, {"schedule": new_s})
    if new_s["enabled"]:
        log_event(feed_id, "success", f"Автооновлення увімкнено — щодня о {new_s['run_time']}")
    else:
        log_event(feed_id, "warning", "Автооновлення вимкнено")
    return jsonify({"ok": True, "schedule": new_s})

@app.route("/api/feeds/<feed_id>/status", methods=["GET"])
def feed_status(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    return jsonify({
        "id":               record["id"],
        "status":           record["status"],
        "progress":         record["progress"],
        "finished_at":      record.get("finished_at"),
        "stats":            record.get("stats"),
        "public_url":       record.get("public_url"),
        "logs":             record.get("logs", []),
        "progress_current": record.get("progress_current", 0),
        "progress_total":   record.get("progress_total", 0),
        "progress_success": record.get("progress_success", 0),
        "progress_skipped": record.get("progress_skipped", 0),
    })

@app.route("/api/feeds/<feed_id>/download", methods=["GET"])
def download_feed(feed_id):
    record = get_feed(feed_id)
    if not record or record["status"] != "done":
        return jsonify({"error": "Не готовий"}), 409
    gcs_path = record.get("result_gcs_path")
    if not gcs_path: return jsonify({"error": "Файл не знайдено"}), 404
    xml_bytes = gcs_download_bytes(gcs_path)
    log_event(feed_id, "info", "Файл скачано")
    return Response(xml_bytes, mimetype="application/xml",
                    headers={"Content-Disposition": f"attachment; filename=enhanced_{record['name']}"})

@app.route("/api/feeds/<feed_id>/preview", methods=["GET"])
def preview_feed(feed_id):
    record = get_feed(feed_id)
    if not record or record.get("status") != "done":
        return jsonify({"error": "Не готовий"}), 404
    gcs_path = record.get("result_gcs_path")
    if not gcs_path: return jsonify({"error": "Файл не знайдено"}), 404
    try:
        import xml.etree.ElementTree as ET2
        xml_bytes  = gcs_download_bytes(gcs_path)
        root       = ET2.fromstring(xml_bytes)
        ns_g       = "http://base.google.com/ns/1.0"
        image_urls = []
        for tag in [f"{{{ns_g}}}image_link","image_link"]:
            for el in root.iter(tag):
                if el.text and "storage.googleapis.com" in el.text:
                    image_urls.append(el.text.strip())
        if not image_urls: return jsonify({"error": "Зображень не знайдено"}), 404
        return jsonify({"image_url": random.choice(image_urls), "total": len(image_urls)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/feeds/<feed_id>", methods=["DELETE"])
def delete_feed(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    for p in [record.get("input_gcs_path"), record.get("result_gcs_path")]:
        if p: gcs_delete(p)
    delete_feed_from_db(feed_id)
    return jsonify({"ok": True})

@app.route("/")
def index():
    return send_from_directory("static","index.html")

# ─── Старт ────────────────────────────────────────────────────────────────────
start_scheduler()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
