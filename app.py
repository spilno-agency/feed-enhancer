"""
Feed Enhancer — Web Service з повним керуванням фідами
======================================================
GET    /                          — веб-інтерфейс
GET    /api/feeds                  — список всіх фідів
POST   /api/feeds                  — додати новий фід (файл або URL)
DELETE /api/feeds/<id>             — видалити фід
POST   /api/feeds/<id>/process     — обробити фід вручну
POST   /api/feeds/<id>/refresh     — перезавантажити з URL і обробити
PATCH  /api/feeds/<id>/schedule    — налаштувати автооновлення
GET    /api/feeds/<id>/status      — статус обробки
GET    /api/feeds/<id>/download    — скачати фід (як attachment)
GET    /feeds/<id>.xml             — публічний URL фіду (для Google/Meta)
GET    /images/<filename>          — роздача зображень
"""

import os
import uuid
import json
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta

import requests as http_requests
from flask import Flask, request, jsonify, send_file, send_from_directory, Response
from feed_processor import process_feed, DEFAULT_CONFIG

app = Flask(__name__, static_folder="static")

# ─── Конфігурація ─────────────────────────────────────────────────────────────
BASE_URL    = os.getenv("BASE_URL",   "http://localhost:8080/images/")
SERVER_URL  = os.getenv("SERVER_URL", "http://localhost:8080")
DATA_DIR    = Path(os.getenv("DATA_DIR", "data"))
IMAGES_DIR  = DATA_DIR / "images"
FEEDS_DIR   = DATA_DIR / "feeds"
RESULTS_DIR = DATA_DIR / "results"
DB_PATH     = DATA_DIR / "feeds.json"

for d in [DATA_DIR, IMAGES_DIR, FEEDS_DIR, RESULTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ─── БД ───────────────────────────────────────────────────────────────────────
db_lock = threading.Lock()

def load_db():
    if DB_PATH.exists():
        with open(DB_PATH) as f:
            return json.load(f)
    return {"feeds": {}}

def save_db(db):
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)

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

def log_event(feed_id: str, level: str, message: str):
    """Додає запис до лога фіду. level: info | success | warning | error"""
    with db_lock:
        db = load_db()
        if feed_id not in db["feeds"]:
            return
        logs = db["feeds"][feed_id].get("logs", [])
        logs.append({
            "ts":      datetime.now().isoformat(),
            "level":   level,
            "message": message,
        })
        # Зберігаємо максимум 200 записів
        db["feeds"][feed_id]["logs"] = logs[-200:]
        save_db(db)

# ─── Утиліти ──────────────────────────────────────────────────────────────────
def download_feed_from_url(url, dest_path):
    resp = http_requests.get(url, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    ct = resp.headers.get("content-type", "")
    if resp.content[:5] not in (b"<?xml", b"<rss ", b"<feed", b"\xef\xbb\xbf"):
        if b"<item>" not in resp.content[:4096] and b"<entry" not in resp.content[:4096]:
            if "html" in ct and "xml" not in ct:
                raise ValueError("URL повернув HTML замість XML. Перевір посилання.")
    with open(dest_path, "wb") as f:
        f.write(resp.content)

def public_feed_url(feed_id):
    return f"{SERVER_URL.rstrip('/')}/feeds/{feed_id}.xml"

# ─── Обробка ──────────────────────────────────────────────────────────────────
def run_processing(feed_id, input_path, cfg, source_url=None):
    try:
        if source_url:
            update_feed(feed_id, {"status": "processing", "progress": f"Завантаження фіду з {source_url}..."})
            log_event(feed_id, "info", f"Завантаження фіду з URL: {source_url}")
            download_feed_from_url(source_url, input_path)
            update_feed(feed_id, {"last_fetched": datetime.now().isoformat()})
            log_event(feed_id, "success", "Фід успішно завантажено з URL")
            cfg["source_url"] = source_url

        update_feed(feed_id, {"status": "processing", "progress": "Обробка зображень...", "progress_current": 0, "progress_total": 0})
        log_event(feed_id, "info", f"Запуск обробки зображень (стиль: {cfg.get('banner_style','classic')}, колір: {cfg.get('border_color','#FF0000')})")

        output_path = str(RESULTS_DIR / f"{feed_id}.xml")
        cfg["output_feed"] = output_path
        cfg["output_dir"]  = str(IMAGES_DIR)
        cfg["base_url"]    = BASE_URL

        # Передаємо колбек для оновлення прогресу
        def on_progress(current, total, success, skipped):
            update_feed(feed_id, {
                "progress":         f"Обробка: {current}/{total}",
                "progress_current": current,
                "progress_total":   total,
                "progress_success": success,
                "progress_skipped": skipped,
            })

        stats = process_feed(input_path, cfg, progress_callback=on_progress)

        update_feed(feed_id, {
            "status":           "done",
            "result_path":      output_path,
            "progress":         f"Готово: оброблено {stats['success']} з {stats['total']}",
            "progress_current": stats["success"],
            "progress_total":   stats["total"],
            "finished_at":      datetime.now().isoformat(),
            "stats":            stats,
            "public_url":       public_feed_url(feed_id),
        })
        log_event(feed_id, "success",
            f"Обробку завершено: {stats['success']} оброблено, {stats['skipped']} пропущено з {stats['total']} товарів")

    except Exception as e:
        update_feed(feed_id, {"status": "error", "progress": str(e)})
        log_event(feed_id, "error", f"Помилка обробки: {e}")

# ─── Планувальник ─────────────────────────────────────────────────────────────
scheduler_started = False
scheduler_lock    = threading.Lock()

def scheduler_loop():
    while True:
        time.sleep(60)
        try:
            db = load_db()
            now = datetime.now()
            for feed_id, record in db["feeds"].items():
                sched = record.get("schedule", {})
                if not sched.get("enabled"):
                    continue
                if record.get("status") == "processing":
                    continue
                if record.get("source_type") != "url" or not record.get("source_url"):
                    continue

                # Перевіряємо час запуску (HH:MM)
                run_time = sched.get("run_time", "06:00")
                try:
                    run_h, run_m = map(int, run_time.split(":"))
                except Exception:
                    run_h, run_m = 6, 0

                # Запускаємо якщо поточний час збігається з потрібним (з точністю до хвилини)
                if now.hour != run_h or now.minute != run_m:
                    continue

                # Перевіряємо чи вже запускали сьогодні
                last_run = sched.get("last_run")
                if last_run:
                    last_dt = datetime.fromisoformat(last_run)
                    if last_dt.date() == now.date():
                        continue  # вже запускали сьогодні

                cfg = {**DEFAULT_CONFIG, **record.get("config", {})}
                update_feed(feed_id, {
                    "status":   "processing",
                    "progress": f"Автооновлення о {run_time}...",
                    "schedule": {**sched, "last_run": now.isoformat()},
                })
                threading.Thread(
                    target=run_processing,
                    args=(feed_id, record["input_path"], cfg, record["source_url"]),
                    daemon=True,
                ).start()
        except Exception as e:
            print(f"[scheduler] {e}")

def start_scheduler():
    global scheduler_started
    with scheduler_lock:
        if not scheduler_started:
            scheduler_started = True
            threading.Thread(target=scheduler_loop, daemon=True).start()

# ═══════════════════════════════════════════════════════════
# Маршрути
# ═══════════════════════════════════════════════════════════

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

# Публічний URL фіду — вставляй в Google Merchant / Meta
@app.route("/feeds/<feed_id>.xml")
def serve_feed_xml(feed_id):
    record = get_feed(feed_id)
    if not record:
        return Response("Feed not found", status=404, mimetype="text/plain")
    if record.get("status") != "done" or not record.get("result_path"):
        return Response("Feed not ready yet", status=202, mimetype="text/plain")
    path = Path(record["result_path"])
    if not path.exists():
        return Response("Feed file missing", status=404, mimetype="text/plain")
    return send_file(str(path), mimetype="application/xml")

@app.route("/images/<filename>")
def serve_image(filename):
    if not (IMAGES_DIR / filename).exists():
        return jsonify({"error": "Not found"}), 404
    return send_from_directory(str(IMAGES_DIR), filename, mimetype="image/jpeg")


# ── GET /api/feeds/<id>/preview — випадковий оброблений банер ─────────────────
@app.route("/api/feeds/<feed_id>/preview", methods=["GET"])
def feed_preview(feed_id):
    """
    Повертає URL випадкового обробленого зображення з фіду.
    Парсить result XML і обирає випадковий image_link.
    """
    import random
    import xml.etree.ElementTree as ET

    record = get_feed(feed_id)
    if not record:
        return jsonify({"error": "Фід не знайдено"}), 404
    if record.get("status") != "done" or not record.get("result_path"):
        return jsonify({"error": "Фід ще не оброблено"}), 409

    result_path = record["result_path"]
    if not Path(result_path).exists():
        return jsonify({"error": "Файл фіду не знайдено"}), 404

    try:
        tree = ET.parse(result_path)
        root = tree.getroot()

        # Збираємо всі image_link з результуючого фіду
        image_urls = []
        ns_g = "http://base.google.com/ns/1.0"

        for tag in [f"{{{ns_g}}}image_link", "image_link"]:
            for el in root.iter(tag):
                if el.text and el.text.strip().startswith("http"):
                    image_urls.append(el.text.strip())

        if not image_urls:
            return jsonify({"error": "Оброблених зображень не знайдено у фіді"}), 404

        chosen = random.choice(image_urls)
        total  = len(image_urls)
        return jsonify({"image_url": chosen, "total": total})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/feeds", methods=["GET"])
def list_feeds():
    with db_lock:
        db = load_db()
    feeds = sorted(db["feeds"].values(), key=lambda x: x.get("created_at", ""), reverse=True)
    return jsonify(feeds)

@app.route("/api/feeds", methods=["POST"])
def add_feed():
    feed_url = request.form.get("feed_url", "").strip()
    has_file = "file" in request.files and request.files["file"].filename

    if not feed_url and not has_file:
        return jsonify({"error": "Надішліть файл або вкажіть URL фіду"}), 400

    feed_id    = str(uuid.uuid4())[:8]
    input_path = str(FEEDS_DIR / f"{feed_id}_input.xml")

    if feed_url:
        feed_name = request.form.get("name") or feed_url.split("/")[-1].split("?")[0] or "feed.xml"
        Path(input_path).touch()
    else:
        file = request.files["file"]
        if not file.filename.endswith(".xml"):
            return jsonify({"error": "Потрібен XML файл"}), 400
        feed_name = request.form.get("name") or file.filename
        file.save(input_path)
        feed_url = None

    cfg = {**DEFAULT_CONFIG}
    cfg["border_color"]    = request.form.get("border_color",    cfg["border_color"])
    cfg["border_width"]    = int(request.form.get("border_width", cfg["border_width"]))
    cfg["badge_position"]  = request.form.get("badge_position",  cfg["badge_position"])
    cfg["badge_font_size"] = int(request.form.get("badge_font_size", cfg["badge_font_size"]))
    cfg["banner_style"]    = request.form.get("banner_style",    cfg.get("banner_style", "classic"))
    cfg["domain"]          = request.form.get("domain",          cfg.get("domain", ""))

    record = {
        "id":           feed_id,
        "name":         feed_name,
        "source_type":  "url" if feed_url else "file",
        "source_url":   feed_url,
        "status":       "pending",
        "progress":     "Очікує обробки",
        "input_path":   input_path,
        "result_path":  None,
        "public_url":   None,
        "created_at":   datetime.now().isoformat(),
        "finished_at":  None,
        "last_fetched": None,
        "schedule":     {"enabled": False, "interval_hours": 24, "last_run": None},
        "config":       {
            "border_color":    cfg["border_color"],
            "border_width":    cfg["border_width"],
            "badge_position":  cfg["badge_position"],
            "badge_font_size": cfg["badge_font_size"],
            "banner_style":    cfg["banner_style"],
            "domain":          cfg["domain"],
        },
    }
    update_feed(feed_id, record)
    log_event(feed_id, "info", f"Фід створено: {feed_name} ({'URL' if feed_url else 'файл'})")

    if request.form.get("auto_process", "true").lower() == "true":
        update_feed(feed_id, {"status": "processing"})
        log_event(feed_id, "info", f"Стиль: {cfg['banner_style']}, колір: {cfg['border_color']}, домен: {cfg.get('domain','авто')}")
        threading.Thread(
            target=run_processing,
            args=(feed_id, input_path, cfg, feed_url),
            daemon=True,
        ).start()

    return jsonify(record), 201

@app.route("/api/feeds/<feed_id>/process", methods=["POST"])
def process_feed_route(feed_id):
    record = get_feed(feed_id)
    if not record:   return jsonify({"error": "Фід не знайдено"}), 404
    if record["status"] == "processing": return jsonify({"error": "Вже обробляється"}), 409

    body = request.get_json(silent=True) or {}
    cfg  = {**DEFAULT_CONFIG, **record.get("config", {})}
    for k in ("border_color", "badge_position", "banner_style", "domain"):
        if k in body: cfg[k] = body[k]
    for k in ("border_width", "badge_font_size"):
        if k in body: cfg[k] = int(body[k])

    # Логуємо зміни конфігурації якщо вони відрізняються від збережених
    old_cfg = record.get("config", {})
    changes = []
    if body.get("banner_style") and body["banner_style"] != old_cfg.get("banner_style"):
        changes.append(f"стиль: {old_cfg.get('banner_style','?')} → {body['banner_style']}")
    if body.get("border_color") and body["border_color"] != old_cfg.get("border_color"):
        changes.append(f"колір: {old_cfg.get('border_color','?')} → {body['border_color']}")
    if body.get("border_width") and str(body["border_width"]) != str(old_cfg.get("border_width")):
        changes.append(f"рамка: {old_cfg.get('border_width','?')}px → {body['border_width']}px")
    if body.get("domain") is not None and body["domain"] != old_cfg.get("domain",""):
        changes.append(f"домен: '{old_cfg.get('domain','авто')}' → '{body['domain'] or 'авто'}'")
    if changes:
        log_event(feed_id, "info", f"Зміна налаштувань: {', '.join(changes)}")

    update_feed(feed_id, {"status": "processing", "progress": "Запущено", "config": {
        "border_color": cfg["border_color"], "border_width": cfg["border_width"],
        "badge_position": cfg["badge_position"], "badge_font_size": cfg["badge_font_size"],
        "banner_style": cfg["banner_style"], "domain": cfg["domain"],
    }})
    log_event(feed_id, "info", f"Запущено обробку вручну — стиль: {cfg['banner_style']}, колір: {cfg['border_color']}")
    threading.Thread(target=run_processing, args=(feed_id, record["input_path"], cfg, None), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/refresh", methods=["POST"])
def refresh_feed(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Фід не знайдено"}), 404
    if record.get("source_type") != "url" or not record.get("source_url"):
        return jsonify({"error": "Цей фід завантажений як файл"}), 400
    if record["status"] == "processing": return jsonify({"error": "Вже обробляється"}), 409

    body = request.get_json(silent=True) or {}
    cfg  = {**DEFAULT_CONFIG, **record.get("config", {})}
    for k in ("border_color", "badge_position", "banner_style", "domain"):
        if k in body: cfg[k] = body[k]
    for k in ("border_width", "badge_font_size"):
        if k in body: cfg[k] = int(body[k])

    update_feed(feed_id, {"status": "processing", "progress": "Завантаження оновленого фіду...", "config": {
        "border_color": cfg["border_color"], "border_width": cfg["border_width"],
        "badge_position": cfg["badge_position"], "badge_font_size": cfg["badge_font_size"],
        "banner_style": cfg["banner_style"], "domain": cfg["domain"],
    }})
    log_event(feed_id, "info", f"Оновлення з URL: {record['source_url']}")
    threading.Thread(target=run_processing, args=(feed_id, record["input_path"], cfg, record["source_url"]), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/log", methods=["POST"])
def add_log_entry(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    body    = request.get_json(silent=True) or {}
    level   = body.get("level", "info")
    message = body.get("message", "")
    if message:
        log_event(feed_id, level, message)
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/logs", methods=["DELETE"])
def clear_logs(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Фід не знайдено"}), 404
    update_feed(feed_id, {"logs": []})
    return jsonify({"ok": True})

@app.route("/api/feeds/<feed_id>/schedule", methods=["PATCH"])
def set_schedule(feed_id):
    """
    Body JSON: { "enabled": true, "interval_hours": 6 }
    Доступні інтервали: 1, 3, 6, 12, 24, 48, 168
    """
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Фід не знайдено"}), 404
    if record.get("source_type") != "url":
        return jsonify({"error": "Автооновлення доступне лише для URL-фідів"}), 400

    body    = request.get_json(silent=True) or {}
    current = record.get("schedule", {})
    new_sched = {
        "enabled":        bool(body.get("enabled", current.get("enabled", False))),
        "run_time":       body.get("run_time", current.get("run_time", "06:00")),
        "interval_hours": 24,
        "last_run":       current.get("last_run"),
    }
    update_feed(feed_id, {"schedule": new_sched})
    if new_sched["enabled"]:
        log_event(feed_id, "success", f"Автооновлення увімкнено — щодня о {new_sched['run_time']}")
    else:
        log_event(feed_id, "warning", "Автооновлення вимкнено")
    return jsonify({"ok": True, "schedule": new_sched})

@app.route("/api/feeds/<feed_id>/status", methods=["GET"])
def feed_status(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    return jsonify({
        "id":               record["id"],
        "status":           record["status"],
        "progress":         record["progress"],
        "progress_current": record.get("progress_current", 0),
        "progress_total":   record.get("progress_total", 0),
        "progress_success": record.get("progress_success", 0),
        "progress_skipped": record.get("progress_skipped", 0),
        "finished_at":      record.get("finished_at"),
        "stats":            record.get("stats"),
        "public_url":       record.get("public_url"),
        "logs":             record.get("logs", []),
    })

@app.route("/api/feeds/<feed_id>/download", methods=["GET"])
def download_feed(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    if record["status"] != "done": return jsonify({"error": "Фід ще не оброблено"}), 409
    log_event(feed_id, "info", f"Файл фіду скачано: enhanced_{record['name']}")
    return send_file(record["result_path"], mimetype="application/xml",
                     as_attachment=True, download_name=f"enhanced_{record['name']}")

@app.route("/api/feeds/<feed_id>", methods=["DELETE"])
def delete_feed(feed_id):
    record = get_feed(feed_id)
    if not record: return jsonify({"error": "Не знайдено"}), 404
    name = record.get("name", feed_id)
    for key in ("input_path", "result_path"):
        p = record.get(key)
        if p and Path(p).exists():
            Path(p).unlink(missing_ok=True)
    delete_feed_from_db(feed_id)
    return jsonify({"ok": True})

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

# ─── Старт ────────────────────────────────────────────────────────────────────
start_scheduler()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
