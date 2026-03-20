"""
Feed Image Enhancer
===================
Читає XML фід (Google Merchant / Meta Catalog),
для кожного товару:
  - завантажує зображення
  - малює червону рамку
  - додає бейдж з ціною
  - зберігає нове зображення
  - генерує новий XML фід з оновленими посиланнями на зображення
"""

import os
import io
import time
import hashlib
import logging
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse

import requests
from PIL import Image, ImageDraw, ImageFont

# ─── Логування ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Конфігурація за замовчуванням ───────────────────────────────────────────
DEFAULT_CONFIG = {
    # Рамка
    "border_color": "#FF0000",        # червона рамка
    "border_width": 18,               # товщина рамки (px)

    # Бейдж ціни
    "badge_bg_color": "#FF0000",      # фон бейджа
    "badge_text_color": "#FFFFFF",    # колір тексту
    "badge_font_size": 52,            # розмір шрифту
    "badge_padding_x": 28,            # відступ по горизонталі
    "badge_padding_y": 14,            # відступ по вертикалі
    "badge_radius": 14,               # заокруглення кутів
    "badge_position": "bottom-right", # позиція: bottom-right / bottom-left / top-right / top-left

    # Зображення
    "output_size": (1200, 1200),      # розмір вихідного зображення
    "output_quality": 90,             # якість JPEG
    "output_dir": "enhanced_images",  # папка для збережених зображень

    # Мережа
    "request_timeout": 15,
    "retry_count": 3,
    "retry_delay": 2,

    # Фід
    "output_feed": "enhanced_feed.xml",
    "base_url": "",  # базовий URL, куди будуть завантажені зображення
                     # напр. "https://cdn.myshop.com/enhanced/"
}

# ─── Простори імен Google Merchant ───────────────────────────────────────────
NS = {
    "g": "http://base.google.com/ns/1.0",
    "": "http://www.w3.org/2005/Atom",
}
ET.register_namespace("", "http://www.w3.org/2005/Atom")
ET.register_namespace("g", "http://base.google.com/ns/1.0")


# ═════════════════════════════════════════════════════════════════════════════
# Завантаження зображення
# ═════════════════════════════════════════════════════════════════════════════

def download_image(url: str, cfg: dict) -> Image.Image | None:
    """Завантажує зображення з URL, повертає PIL Image або None."""
    for attempt in range(1, cfg["retry_count"] + 1):
        try:
            resp = requests.get(url, timeout=cfg["request_timeout"])
            resp.raise_for_status()
            img = Image.open(io.BytesIO(resp.content)).convert("RGBA")
            return img
        except Exception as e:
            log.warning(f"Спроба {attempt}/{cfg['retry_count']} — помилка завантаження {url}: {e}")
            if attempt < cfg["retry_count"]:
                time.sleep(cfg["retry_delay"])
    return None


# ═════════════════════════════════════════════════════════════════════════════
# Обробка зображення
# ═════════════════════════════════════════════════════════════════════════════

def hex_to_rgb(hex_color: str) -> tuple:
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


def get_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Намагається завантажити системний шрифт, інакше fallback."""
    font_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "C:/Windows/Fonts/arialbd.ttf",
        "arial.ttf",
        "DejaVuSans-Bold.ttf",
    ]
    for path in font_candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    # fallback — маленький вбудований шрифт
    log.warning("TrueType шрифт не знайдено, використовується вбудований.")
    return ImageFont.load_default()


def draw_border(img: Image.Image, color: str, width: int) -> Image.Image:
    """Малює кольорову рамку по периметру зображення."""
    draw = ImageDraw.Draw(img)
    w, h = img.size
    rgb = hex_to_rgb(color)
    for i in range(width):
        draw.rectangle([i, i, w - 1 - i, h - 1 - i], outline=rgb + (255,))
    return img


def draw_price_badge(img: Image.Image, price_text: str, cfg: dict) -> Image.Image:
    """Малює бейдж з ціною у вказаному куті зображення."""
    draw = ImageDraw.Draw(img)
    font = get_font(cfg["badge_font_size"])

    # Розмір тексту
    bbox = draw.textbbox((0, 0), price_text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]

    pad_x = cfg["badge_padding_x"]
    pad_y = cfg["badge_padding_y"]
    badge_w = text_w + pad_x * 2
    badge_h = text_h + pad_y * 2
    r = cfg["badge_radius"]

    img_w, img_h = img.size
    margin = cfg["border_width"] + 10

    pos = cfg["badge_position"]
    if pos == "bottom-right":
        bx = img_w - badge_w - margin
        by = img_h - badge_h - margin
    elif pos == "bottom-left":
        bx = margin
        by = img_h - badge_h - margin
    elif pos == "top-right":
        bx = img_w - badge_w - margin
        by = margin
    else:  # top-left
        bx = margin
        by = margin

    bg_rgb = hex_to_rgb(cfg["badge_bg_color"]) + (230,)  # трохи прозорий
    text_rgb = hex_to_rgb(cfg["badge_text_color"]) + (255,)

    # Фон бейджа з заокругленими кутами
    draw.rounded_rectangle([bx, by, bx + badge_w, by + badge_h],
                            radius=r, fill=bg_rgb)

    # Текст
    tx = bx + pad_x - bbox[0]
    ty = by + pad_y - bbox[1]
    draw.text((tx, ty), price_text, font=font, fill=text_rgb)

    return img


def enhance_image(original_img: Image.Image, price_text: str, cfg: dict) -> Image.Image:
    """Масштабує, додає рамку та бейдж ціни."""
    # 1. Масштабуємо
    img = original_img.copy()
    img = img.resize(cfg["output_size"], Image.LANCZOS)

    # 2. Червона рамка
    img = draw_border(img, cfg["border_color"], cfg["border_width"])

    # 3. Бейдж ціни
    if price_text:
        img = draw_price_badge(img, price_text, cfg)

    return img


def save_image(img: Image.Image, filename: str, cfg: dict) -> str:
    """Зберігає зображення у output_dir, повертає шлях."""
    out_dir = Path(cfg["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / filename
    rgb_img = img.convert("RGB")
    rgb_img.save(str(out_path), "JPEG", quality=cfg["output_quality"])
    return str(out_path)


# ═════════════════════════════════════════════════════════════════════════════
# Парсинг XML фіду
# ═════════════════════════════════════════════════════════════════════════════

def parse_feed(feed_path: str) -> tuple[ET.ElementTree, list[ET.Element]]:
    """Повертає (tree, список item-елементів)."""
    tree = ET.parse(feed_path)
    root = tree.getroot()

    # Google Merchant: <channel><item>...</item></channel>
    items = root.findall(".//item")
    if not items:
        # Atom-формат: <entry>
        items = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    log.info(f"Знайдено {len(items)} товарів у фіді.")
    return tree, items


def get_text(element: ET.Element, *tags) -> str:
    """Шукає перший знайдений тег серед переданих (з NS або без)."""
    for tag in tags:
        el = element.find(tag)
        if el is not None and el.text:
            return el.text.strip()
        # спробуємо з простором імен g:
        el = element.find(f"{{http://base.google.com/ns/1.0}}{tag}")
        if el is not None and el.text:
            return el.text.strip()
    return ""


def extract_item_data(item: ET.Element) -> dict:
    """Витягує id, title, price, image_link з елемента фіду."""
    return {
        "id": get_text(item, "id", "g:id"),
        "title": get_text(item, "title", "g:title"),
        "price": get_text(item, "price", "g:price"),
        "image_link": get_text(item, "image_link", "g:image_link",
                               "link", "g:link"),
    }


def set_image_link(item: ET.Element, new_url: str):
    """Оновлює image_link у XML елементі."""
    for tag in ["image_link",
                "{http://base.google.com/ns/1.0}image_link"]:
        el = item.find(tag)
        if el is not None:
            el.text = new_url
            return
    # якщо тег не існує — створюємо
    new_el = ET.SubElement(item, "{http://base.google.com/ns/1.0}image_link")
    new_el.text = new_url


# ═════════════════════════════════════════════════════════════════════════════
# Головна функція
# ═════════════════════════════════════════════════════════════════════════════

def process_feed(feed_path: str, cfg: dict) -> dict:
    """Обробляє фід і повертає статистику: total, success, skipped."""
    tree, items = parse_feed(feed_path)
    total = len(items)
    success = 0
    skipped = 0

    for i, item in enumerate(items, 1):
        data = extract_item_data(item)
        img_url = data["image_link"] or ""
        price   = data["price"]      or ""
        raw_id  = data["id"]         or ""

        # Генеруємо fallback ID — лише якщо є img_url, інакше використовуємо порядковий номер
        if raw_id:
            item_id = raw_id
        elif img_url:
            item_id = hashlib.md5(img_url.encode()).hexdigest()[:8]
        else:
            item_id = f"item_{i}"

        log.info(f"[{i}/{total}] ID={item_id}  Ціна={price}  URL={img_url}")

        if not img_url:
            log.warning("  → Немає URL зображення, пропускаємо.")
            skipped += 1
            continue

        # Завантажуємо
        img = download_image(img_url, cfg)
        if img is None:
            log.warning("  → Не вдалося завантажити, пропускаємо.")
            skipped += 1
            continue

        # Обробляємо
        enhanced = enhance_image(img, price, cfg)

        # Зберігаємо
        filename = f"{item_id}.jpg"
        saved_path = save_image(enhanced, filename, cfg)
        log.info(f"  → Збережено: {saved_path}")

        # Оновлюємо URL у фіді
        if cfg["base_url"]:
            new_url = cfg["base_url"].rstrip("/") + "/" + filename
        else:
            new_url = saved_path  # локальний шлях як fallback

        set_image_link(item, new_url)
        success += 1

    # Зберігаємо новий фід
    out_feed = cfg["output_feed"]
    tree.write(out_feed, encoding="utf-8", xml_declaration=True)
    log.info(f"\n✅ Готово! Оброблено: {success}, пропущено: {skipped}")
    log.info(f"📄 Новий фід: {out_feed}")
    log.info(f"🖼  Зображення: {cfg['output_dir']}/")

    return {"total": total, "success": success, "skipped": skipped}


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Enhance product feed images with price badge & red border"
    )
    parser.add_argument("feed", help="Шлях до вхідного XML фіду")
    parser.add_argument("--base-url", default="",
                        help="Базовий URL для зображень у новому фіді")
    parser.add_argument("--output-feed", default="enhanced_feed.xml",
                        help="Назва вихідного фіду")
    parser.add_argument("--output-dir", default="enhanced_images",
                        help="Папка для збереження зображень")
    parser.add_argument("--border-width", type=int, default=18)
    parser.add_argument("--border-color", default="#FF0000")
    parser.add_argument("--badge-position",
                        choices=["bottom-right", "bottom-left",
                                 "top-right", "top-left"],
                        default="bottom-right")
    parser.add_argument("--badge-font-size", type=int, default=52)
    args = parser.parse_args()

    cfg = {**DEFAULT_CONFIG}
    cfg["base_url"] = args.base_url
    cfg["output_feed"] = args.output_feed
    cfg["output_dir"] = args.output_dir
    cfg["border_width"] = args.border_width
    cfg["border_color"] = args.border_color
    cfg["badge_position"] = args.badge_position
    cfg["badge_font_size"] = args.badge_font_size

    process_feed(args.feed, cfg)


if __name__ == "__main__":
    main()
