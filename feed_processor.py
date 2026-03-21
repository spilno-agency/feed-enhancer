"""
Feed Image Enhancer — feed_processor.py
=======================================
5 стилів банерів + домен сайту на зображенні.
"""

import os
import io
import time
import hashlib
import logging
import math
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse

import requests
from PIL import Image, ImageDraw, ImageFont, ImageFilter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    # Стиль: "classic" | "neon" | "luxury" | "minimal" | "gradient" | "dark"
    "banner_style":    "classic",
    "border_color":    "#FF0000",
    "border_width":    18,
    "badge_font_size": 52,
    "badge_position":  "bottom-right",
    "domain":          "",
    "output_size":     (800, 800),   # 800px економить ~2x RAM vs 1200px
    "output_quality":  85,
    "output_dir":      "enhanced_images",
    "request_timeout": 15,
    "retry_count":     2,            # зменшено з 3 до 2 для швидшої обробки
    "retry_delay":     1,
    "output_feed":     "enhanced_feed.xml",
    "base_url":        "",
}

NS_G    = "http://base.google.com/ns/1.0"
NS_ATOM = "http://www.w3.org/2005/Atom"
ET.register_namespace("",  NS_ATOM)
ET.register_namespace("g", NS_G)

BANNER_STYLES = ["classic", "neon", "luxury", "minimal", "gradient", "dark"]


# ─── Утиліти ──────────────────────────────────────────────────────────────────

def safe_str(v) -> str:
    return "" if v is None else str(v).strip()

def hex_to_rgba(hex_color: str, alpha: int = 255) -> tuple:
    h = safe_str(hex_color).lstrip("#")
    if len(h) != 6:
        return (255, 0, 0, alpha)
    try:
        return (int(h[0:2],16), int(h[2:4],16), int(h[4:6],16), alpha)
    except Exception:
        return (255, 0, 0, alpha)

def lighten(hex_color: str, factor: float = 0.3) -> tuple:
    r,g,b,a = hex_to_rgba(hex_color)
    return (min(255,int(r+(255-r)*factor)), min(255,int(g+(255-g)*factor)), min(255,int(b+(255-b)*factor)), a)

def find_text(element, *tags) -> str:
    if element is None: return ""
    for tag in tags:
        for prefix in ["", f"{{{NS_G}}}", f"{{{NS_ATOM}}}"]:
            el = element.find(f"{prefix}{tag}")
            if el is not None and el.text:
                return safe_str(el.text)
        for el in element.iter():
            local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if local == tag and el.text:
                return safe_str(el.text)
    return ""

def extract_item_data(item) -> dict:
    return {
        "id":         find_text(item, "id"),
        "title":      find_text(item, "title"),
        "price":      find_text(item, "price"),
        "image_link": find_text(item, "image_link", "link"),
    }

def get_font(size: int):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arialbd.ttf",
        "arial.ttf", "DejaVuSans-Bold.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try: return ImageFont.truetype(path, size)
            except: continue
    return ImageFont.load_default()

def get_font_regular(size: int):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arial.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try: return ImageFont.truetype(path, size)
            except: continue
    return ImageFont.load_default()

def download_image(url: str, cfg: dict):
    if not url or not url.startswith("http"): return None
    for attempt in range(1, cfg["retry_count"] + 1):
        try:
            resp = requests.get(url, timeout=cfg["request_timeout"], stream=True)
            resp.raise_for_status()
            # Читаємо контент одразу у BytesIO без зберігання в пам'яті Python
            buf = io.BytesIO()
            for chunk in resp.iter_content(chunk_size=8192):
                buf.write(chunk)
            resp.close()
            buf.seek(0)
            img = Image.open(buf).convert("RGBA")
            buf.close()
            return img
        except Exception as e:
            log.warning(f"Спроба {attempt} — {url}: {e}")
            if attempt < cfg["retry_count"]: time.sleep(cfg["retry_delay"])
    return None


# ═════════════════════════════════════════════════════════════════════════════
# СТИЛІ БАНЕРІВ
# ═════════════════════════════════════════════════════════════════════════════

def style_classic(img: Image.Image, price: str, domain: str, cfg: dict) -> Image.Image:
    """Класика: кольорова рамка + заокруглений бейдж ціни + домен знизу."""
    draw = ImageDraw.Draw(img)
    W, H = img.size
    bw   = int(cfg.get("border_width", 18))
    color = safe_str(cfg.get("border_color", "#FF0000"))
    rgba  = hex_to_rgba(color)

    # Рамка
    for i in range(bw):
        draw.rectangle([i, i, W-1-i, H-1-i], outline=rgba)

    # Бейдж ціни
    if price:
        font  = get_font(int(cfg.get("badge_font_size", 52)))
        bbox  = draw.textbbox((0,0), price, font=font)
        tw, th = bbox[2]-bbox[0], bbox[3]-bbox[1]
        px, py = 28, 14
        bw2, bh = tw+px*2, th+py*2
        margin = bw + 10
        pos = safe_str(cfg.get("badge_position","bottom-right"))
        if pos == "bottom-right":  x,y = W-bw2-margin, H-bh-margin
        elif pos == "bottom-left": x,y = margin, H-bh-margin
        elif pos == "top-right":   x,y = W-bw2-margin, margin
        else:                      x,y = margin, margin
        draw.rounded_rectangle([x,y,x+bw2,y+bh], radius=14, fill=hex_to_rgba(color,230))
        draw.text((x+px-bbox[0], y+py-bbox[1]), price, font=font, fill=(255,255,255,255))

    # Домен
    if domain:
        df = get_font_regular(28)
        db = draw.textbbox((0,0), domain, font=df)
        dw = db[2]-db[0]
        dx = (W - dw) // 2
        dy = H - bw - 36
        draw.rectangle([dx-10, dy-4, dx+dw+10, dy+(db[3]-db[1])+4],
                        fill=hex_to_rgba(color, 180))
        draw.text((dx-db[0], dy-db[1]), domain, font=df, fill=(255,255,255,255))

    return img


def style_neon(img: Image.Image, price: str, domain: str, cfg: dict) -> Image.Image:
    """Неон: яскрава рамка з glow-ефектом + неонова ціна."""
    W, H = img.size
    color = safe_str(cfg.get("border_color", "#FF0000"))
    bw    = int(cfg.get("border_width", 18))
    rgba  = hex_to_rgba(color)

    # Glow: малюємо рамку на окремому шарі і розмиваємо
    glow = Image.new("RGBA", img.size, (0,0,0,0))
    gd   = ImageDraw.Draw(glow)
    for i in range(bw*2):
        alpha = int(180 * (1 - i/(bw*2)))
        gd.rectangle([i, i, W-1-i, H-1-i], outline=(*rgba[:3], alpha))
    glow = glow.filter(ImageFilter.GaussianBlur(radius=6))
    img  = Image.alpha_composite(img, glow)

    draw = ImageDraw.Draw(img)
    # Тверда рамка поверх
    for i in range(bw):
        draw.rectangle([i, i, W-1-i, H-1-i], outline=rgba)

    # Ціна — неоновий прямокутник на всю ширину знизу
    if price:
        font  = get_font(int(cfg.get("badge_font_size", 52)))
        strip_h = int(cfg.get("badge_font_size", 52)) + 36
        bar = Image.new("RGBA", (W, strip_h), (*rgba[:3], 220))
        bar = bar.filter(ImageFilter.GaussianBlur(radius=2))
        img.alpha_composite(bar, (0, H - strip_h))
        draw = ImageDraw.Draw(img)
        bbox = draw.textbbox((0,0), price, font=font)
        tx = (W - (bbox[2]-bbox[0])) // 2
        ty = H - strip_h + (strip_h - (bbox[3]-bbox[1])) // 2
        # Тінь тексту
        draw.text((tx+2-bbox[0], ty+2-bbox[1]), price, font=font, fill=(0,0,0,160))
        draw.text((tx-bbox[0], ty-bbox[1]), price, font=font, fill=(255,255,255,255))

    # Домен — верхній рядок
    if domain:
        df   = get_font_regular(26)
        db   = draw.textbbox((0,0), domain, font=df)
        dw   = db[2]-db[0]
        dx   = W - dw - bw - 14
        dy   = bw + 10
        draw.text((dx-db[0]+1, dy-db[1]+1), domain, font=df, fill=(*rgba[:3], 180))
        draw.text((dx-db[0], dy-db[1]), domain, font=df, fill=(255,255,255,230))

    return img


def style_luxury(img: Image.Image, price: str, domain: str, cfg: dict) -> Image.Image:
    """Люкс: золота подвійна рамка + елегантний напис ціни."""
    W, H  = img.size
    color = safe_str(cfg.get("border_color", "#FF0000"))
    bw    = int(cfg.get("border_width", 18))
    rgba  = hex_to_rgba(color)
    gold  = (212, 175, 55, 255)
    draw  = ImageDraw.Draw(img)

    # Зовнішня рамка кольором користувача
    for i in range(bw):
        draw.rectangle([i, i, W-1-i, H-1-i], outline=rgba)

    # Внутрішня тонка золота рамка
    offset = bw + 6
    for i in range(3):
        draw.rectangle([offset+i, offset+i, W-1-offset-i, H-1-offset-i], outline=gold)

    # Нижня темна смуга з ціною
    if price:
        font    = get_font(int(cfg.get("badge_font_size", 52)))
        strip_h = int(cfg.get("badge_font_size", 52)) + 48

        # Напівпрозорий чорний фон
        overlay = Image.new("RGBA", (W, strip_h), (0,0,0,200))
        img.alpha_composite(overlay, (0, H - strip_h))
        draw = ImageDraw.Draw(img)

        # Золота лінія зверху смуги
        ly = H - strip_h
        for i in range(2):
            draw.line([(0, ly+i), (W, ly+i)], fill=gold, width=1)

        bbox = draw.textbbox((0,0), price, font=font)
        tx = (W - (bbox[2]-bbox[0])) // 2
        ty = H - strip_h + (strip_h - (bbox[3]-bbox[1])) // 2

        # Золотий текст
        draw.text((tx-bbox[0], ty-bbox[1]), price, font=font, fill=gold)

    # Домен — угорі зліва, стильно
    if domain:
        df  = get_font_regular(24)
        db  = draw.textbbox((0,0), domain, font=df)
        px2 = 16
        dw2 = db[2]-db[0]
        # Темний фон
        draw.rectangle([bw+offset, bw+offset, bw+offset+dw2+px2*2, bw+offset+(db[3]-db[1])+12],
                        fill=(0,0,0,160))
        draw.text((bw+offset+px2-db[0], bw+offset+6-db[1]), domain, font=df, fill=gold)

    return img


def style_minimal(img: Image.Image, price: str, domain: str, cfg: dict) -> Image.Image:
    """Мінімалізм: тонка лінія по периметру + чистий бейдж з тінню."""
    W, H  = img.size
    color = safe_str(cfg.get("border_color", "#FF0000"))
    bw    = max(4, int(cfg.get("border_width", 18)) // 3)
    rgba  = hex_to_rgba(color)
    draw  = ImageDraw.Draw(img)

    # Тонка рамка
    for i in range(bw):
        draw.rectangle([i, i, W-1-i, H-1-i], outline=rgba)

    # Кути — акцентні квадрати
    corner = 40
    thick  = bw + 4
    for cx, cy in [(0,0),(W-corner,0),(0,H-corner),(W-corner,H-corner)]:
        draw.rectangle([cx, cy, cx+corner, cy+thick], fill=rgba)
        draw.rectangle([cx, cy, cx+thick, cy+corner], fill=rgba)

    if price:
        font  = get_font(int(cfg.get("badge_font_size", 52)))
        bbox  = draw.textbbox((0,0), price, font=font)
        tw, th = bbox[2]-bbox[0], bbox[3]-bbox[1]
        px, py = 32, 16
        bw2, bh = tw+px*2, th+py*2
        margin  = bw + 14

        pos = safe_str(cfg.get("badge_position","bottom-right"))
        if pos == "bottom-right":  x,y = W-bw2-margin, H-bh-margin
        elif pos == "bottom-left": x,y = margin, H-bh-margin
        elif pos == "top-right":   x,y = W-bw2-margin, margin
        else:                      x,y = margin, margin

        # Тінь
        shadow = Image.new("RGBA", img.size, (0,0,0,0))
        sd = ImageDraw.Draw(shadow)
        sd.rounded_rectangle([x+4,y+4,x+bw2+4,y+bh+4], radius=8, fill=(0,0,0,100))
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=4))
        img    = Image.alpha_composite(img, shadow)
        draw   = ImageDraw.Draw(img)

        draw.rounded_rectangle([x,y,x+bw2,y+bh], radius=8, fill=(*rgba[:3], 240))
        # Тонка біла лінія всередині
        draw.rounded_rectangle([x+2,y+2,x+bw2-2,y+bh-2], radius=6, outline=(255,255,255,120), width=1)
        draw.text((x+px-bbox[0], y+py-bbox[1]), price, font=font, fill=(255,255,255,255))

    if domain:
        df  = get_font_regular(24)
        db  = draw.textbbox((0,0), domain, font=df)
        dw2 = db[2]-db[0]
        dx  = (W - dw2) // 2
        dy  = H - bw - 34
        draw.text((dx-db[0]+1, dy-db[1]+1), domain, font=df, fill=(0,0,0,100))
        draw.text((dx-db[0], dy-db[1]), domain, font=df, fill=(*rgba[:3], 220))

    return img


def style_gradient(img: Image.Image, price: str, domain: str, cfg: dict) -> Image.Image:
    """Градієнт: рамка-градієнт + широка нижня смуга з градієнтом."""
    W, H  = img.size
    color = safe_str(cfg.get("border_color", "#FF0000"))
    bw    = int(cfg.get("border_width", 18))
    rgba  = hex_to_rgba(color)
    light = lighten(color, 0.45)
    draw  = ImageDraw.Draw(img)

    # Градієнтна рамка (горизонтальні лінії що змінюють колір)
    for i in range(bw):
        t = i / max(1, bw-1)
        rc = tuple(int(rgba[c] + (light[c]-rgba[c])*t) for c in range(3)) + (255,)
        draw.rectangle([i, i, W-1-i, H-1-i], outline=rc)

    # Нижня градієнтна смуга
    if price:
        font    = get_font(int(cfg.get("badge_font_size", 52)))
        strip_h = int(cfg.get("badge_font_size", 52)) + 52

        # Малюємо градієнт горизонтальними лініями
        for yi in range(strip_h):
            t  = yi / max(1, strip_h-1)
            rc = tuple(int(rgba[c] + (light[c]-rgba[c])*t) for c in range(3))
            alpha = 220 - int(40*t)
            draw.line([(0, H-strip_h+yi), (W, H-strip_h+yi)], fill=(*rc, alpha))

        bbox = draw.textbbox((0,0), price, font=font)
        tx = (W - (bbox[2]-bbox[0])) // 2
        ty = H - strip_h + (strip_h - (bbox[3]-bbox[1])) // 2
        draw.text((tx-bbox[0]+2, ty-bbox[1]+2), price, font=font, fill=(0,0,0,100))
        draw.text((tx-bbox[0], ty-bbox[1]), price, font=font, fill=(255,255,255,255))

    if domain:
        df  = get_font_regular(26)
        db  = draw.textbbox((0,0), domain, font=df)
        dw2 = db[2]-db[0]
        dx  = W - dw2 - bw - 16
        dy  = bw + 12
        # Pill-форма фону
        ph  = db[3]-db[1]+12
        draw.rounded_rectangle([dx-12, dy-6, dx+dw2+12, dy+ph-6],
                                radius=ph//2, fill=(*rgba[:3], 200))
        draw.text((dx-db[0], dy-db[1]), domain, font=df, fill=(255,255,255,255))

    return img


def style_dark(img: Image.Image, price: str, domain: str, cfg: dict) -> Image.Image:
    """Dark mode: темне накладення + яскравий акцент кольором."""
    W, H  = img.size
    color = safe_str(cfg.get("border_color", "#FF0000"))
    bw    = int(cfg.get("border_width", 18))
    rgba  = hex_to_rgba(color)

    # Темні кути
    corner_size = 180
    corners = [(0,0), (W-corner_size,0), (0,H-corner_size), (W-corner_size,H-corner_size)]
    for cx, cy in corners:
        overlay = Image.new("RGBA", (corner_size, corner_size), (0,0,0,0))
        od = ImageDraw.Draw(overlay)
        od.rectangle([0,0,corner_size,corner_size], fill=(0,0,0,120))
        img.alpha_composite(overlay, (cx, cy))

    draw = ImageDraw.Draw(img)

    # Товста рамка
    for i in range(bw):
        draw.rectangle([i, i, W-1-i, H-1-i], outline=rgba)

    # Діагональні акцентні смужки в кутах
    accent_len = 60
    for cx, cy, dx2, dy2 in [(bw,bw,bw+accent_len,bw),(W-bw,bw,W-bw-accent_len,bw),
                               (bw,H-bw,bw+accent_len,H-bw),(W-bw,H-bw,W-bw-accent_len,H-bw)]:
        draw.line([(cx,cy),(dx2,dy2)], fill=(255,255,255,255), width=3)

    if price:
        font    = get_font(int(cfg.get("badge_font_size", 52)))
        strip_h = int(cfg.get("badge_font_size", 52)) + 44

        dark_bar = Image.new("RGBA", (W, strip_h), (10,10,10,230))
        img.alpha_composite(dark_bar, (0, H-strip_h))
        draw = ImageDraw.Draw(img)

        # Кольорова лінія зверху смуги
        for i in range(4):
            draw.line([(0, H-strip_h+i), (W, H-strip_h+i)], fill=rgba, width=1)

        bbox = draw.textbbox((0,0), price, font=font)
        tx = (W - (bbox[2]-bbox[0])) // 2
        ty = H - strip_h + (strip_h - (bbox[3]-bbox[1])) // 2
        draw.text((tx-bbox[0], ty-bbox[1]), price, font=font, fill=(*rgba[:3], 255))

    if domain:
        df  = get_font_regular(24)
        db  = draw.textbbox((0,0), domain, font=df)
        dw2 = db[2]-db[0]
        dx  = bw + 16
        dy  = bw + 14
        draw.rectangle([dx-8, dy-4, dx+dw2+8, dy+(db[3]-db[1])+4], fill=(10,10,10,200))
        draw.text((dx-db[0], dy-db[1]), domain, font=df, fill=(*rgba[:3], 255))

    return img


# ─── Диспетчер стилів ─────────────────────────────────────────────────────────

STYLE_MAP = {
    "classic":  style_classic,
    "neon":     style_neon,
    "luxury":   style_luxury,
    "minimal":  style_minimal,
    "gradient": style_gradient,
    "dark":     style_dark,
}


def enhance_image(original_img: Image.Image, price: str, cfg: dict) -> Image.Image:
    img    = original_img.copy().resize(cfg.get("output_size", (1200,1200)), Image.LANCZOS)
    style  = safe_str(cfg.get("banner_style", "classic"))
    domain = safe_str(cfg.get("domain", ""))

    # Витягуємо домен з source_url якщо не вказано явно
    if not domain:
        source = safe_str(cfg.get("source_url", ""))
        if source:
            try:
                parsed = urlparse(source)
                domain = parsed.netloc.replace("www.", "")
            except Exception:
                pass

    fn = STYLE_MAP.get(style, style_classic)
    return fn(img, price, domain, cfg)


def save_image(img: Image.Image, filename: str, cfg: dict) -> str:
    out_dir = Path(safe_str(cfg.get("output_dir","enhanced_images")))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    img.convert("RGB").save(str(out_path), "JPEG", quality=int(cfg.get("output_quality",90)))
    return str(out_path)


# ─── XML ──────────────────────────────────────────────────────────────────────

def parse_feed(feed_path: str):
    tree  = ET.parse(feed_path)
    root  = tree.getroot()
    items = root.findall(".//item")
    if not items:
        items = root.findall(f".//{{{NS_ATOM}}}entry")
    log.info(f"Знайдено {len(items)} товарів у фіді.")
    return tree, items

def set_image_link(item, new_url: str):
    for tag in ["image_link", f"{{{NS_G}}}image_link"]:
        el = item.find(tag)
        if el is not None:
            el.text = new_url
            return
    ET.SubElement(item, f"{{{NS_G}}}image_link").text = new_url


# ─── Головна функція ──────────────────────────────────────────────────────────

def process_feed(feed_path: str, cfg: dict, progress_callback=None) -> dict:
    import gc
    tree, items = parse_feed(feed_path)
    total = len(items)
    success = skipped = 0

    for i, item in enumerate(items, 1):
        img = None
        enhanced = None
        try:
            data    = extract_item_data(item)
            img_url = safe_str(data.get("image_link",""))
            price   = safe_str(data.get("price",""))
            raw_id  = safe_str(data.get("id",""))

            item_id = raw_id or (hashlib.md5(img_url.encode()).hexdigest()[:8] if img_url else f"item_{i}")
            log.info(f"[{i}/{total}] ID={item_id}  Ціна={price}  URL={img_url[:80]}")

            if progress_callback and (i == 1 or i % 5 == 0 or i == total):
                progress_callback(i, total, success, skipped)

            if not img_url:
                log.warning("  → Немає URL, пропускаємо.")
                skipped += 1
                continue

            img = download_image(img_url, cfg)
            if img is None:
                log.warning("  → Не завантажено, пропускаємо.")
                skipped += 1
                continue

            enhanced = enhance_image(img, price, cfg)

            # Явно вивантажуємо оригінал з пам'яті
            img.close()
            img = None

            filename = f"{item_id}.jpg"
            save_image(enhanced, filename, cfg)

            # Явно вивантажуємо оброблене зображення
            enhanced.close()
            enhanced = None

            base = safe_str(cfg.get("base_url",""))
            new_url = (base.rstrip("/") + "/" + filename) if base else str(Path(cfg["output_dir"]) / filename)
            set_image_link(item, new_url)
            success += 1

        except Exception as e:
            log.error(f"  → Помилка: {e}")
            skipped += 1
        finally:
            # Гарантовано чистимо пам'ять навіть при помилці
            if img is not None:
                try: img.close()
                except: pass
            if enhanced is not None:
                try: enhanced.close()
                except: pass
            # Запускаємо збирач сміття кожні 50 зображень
            if i % 50 == 0:
                gc.collect()

    # Фінальна очистка
    gc.collect()

    tree.write(safe_str(cfg.get("output_feed","enhanced_feed.xml")), encoding="utf-8", xml_declaration=True)
    log.info(f"\n✅ Готово! Оброблено: {success}/{total}, пропущено: {skipped}")
    return {"total": total, "success": success, "skipped": skipped}
