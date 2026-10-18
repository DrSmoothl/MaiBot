from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
from typing import Tuple

from PIL import Image

import base64
import threading
import pytest

from src.chat.replyer.maisaka_generator_base import BaseMaisakaReplyGenerator
from src.common.data_models.message_component_data_model import EmojiComponent, ImageComponent, MessageSequence
from src.llm_models.payload_content.context_item import ContextImagePart, get_item_text
from src.maisaka.chat_loop_service import MaisakaChatLoopService
from src.maisaka.context.messages import SessionBackedMessage, build_context_items_from_history_entry
from src.maisaka.visual import emoji_image_cache as cache_module
from src.maisaka.visual.emoji_image_cache import EmojiImageCache


def image_bytes(size: Tuple[int, int], *, color: str = "red", mode: str = "RGB") -> bytes:
    output = BytesIO()
    Image.new(mode, size, color).save(output, format="PNG")
    return output.getvalue()


def dimensions(part: ContextImagePart) -> Tuple[int, int]:
    with Image.open(BytesIO(base64.b64decode(part.image_base64))) as image:
        return image.size


def test_limit_only_changes_emoji_request_copy() -> None:
    raw = image_bytes((1024, 512))
    emoji = EmojiComponent(binary_hash="", binary_data=raw)
    photo = ImageComponent(binary_hash="", binary_data=raw)
    message = SessionBackedMessage(
        raw_message=MessageSequence([emoji, photo]), visible_text="", timestamp=datetime(2026, 10, 18)
    )
    items = build_context_items_from_history_entry(message, limit_emoji_size=True)
    parts = [part for item in items for part in item.parts if isinstance(part, ContextImagePart)]
    assert [dimensions(part) for part in parts] == [(256, 128), (1024, 512)]
    assert emoji.binary_data == raw
    assert photo.binary_data == raw

    # 复古模式使用的默认入口继续收到原尺寸图片，原有的 640/128 策略可以独立执行。
    original_items = build_context_items_from_history_entry(message)
    assert all(
        dimensions(part) == (1024, 512)
        for item in original_items
        for part in item.parts
        if isinstance(part, ContextImagePart)
    )


def test_small_image_is_not_upscaled_or_reencoded() -> None:
    raw = image_bytes((92, 92))
    part = EmojiImageCache().get("small", raw)
    assert dimensions(part) == (92, 92)
    assert base64.b64decode(part.image_base64) == raw


def test_planner_and_normal_replyer_limit_emoji_but_retro_keeps_existing_policy() -> None:
    raw = image_bytes((1200, 600))
    message = SessionBackedMessage(
        raw_message=MessageSequence([EmojiComponent(binary_hash="", binary_data=raw)]),
        visible_text="",
        timestamp=datetime(2026, 10, 18),
    )
    service = MaisakaChatLoopService(chat_system_prompt="system")
    planner_items, _ = service._build_request_messages([message], enable_visual_message=True)
    generator = object.__new__(BaseMaisakaReplyGenerator)
    replyer_items = generator._build_history_messages([message], enable_visual_message=True)
    for items in (planner_items, replyer_items):
        images = [part for item in items for part in item.parts if isinstance(part, ContextImagePart)]
        assert [dimensions(part) for part in images] == [(256, 128)]
        assert all("[消息类型]表情包" not in get_item_text(item) for item in items)

    _, retro_images = generator._prepare_retro_visual_history([message])
    assert dimensions(retro_images[0][0]) == (640, 320)


def test_resized_image_preserves_transparency_and_narrow_dimensions() -> None:
    raw = image_bytes((1, 1024), color="#ff000000", mode="RGBA")
    part = EmojiImageCache().get("transparent", raw)
    with Image.open(BytesIO(base64.b64decode(part.image_base64))) as image:
        assert image.size == (1, 256)
        assert image.convert("RGBA").getpixel((0, 0))[3] == 0


def test_large_gif_uses_first_frame() -> None:
    output = BytesIO()
    Image.new("RGB", (600, 300), "red").save(
        output, format="GIF", save_all=True, append_images=[Image.new("RGB", (600, 300), "blue")]
    )
    part = EmojiImageCache().get("gif", output.getvalue())
    with Image.open(BytesIO(base64.b64decode(part.image_base64))) as image:
        assert image.size == (256, 128)
        red, green, blue = image.convert("RGB").getpixel((0, 0))
        assert red > 200 and green < 20 and blue < 20


def test_resize_applies_exif_orientation() -> None:
    output = BytesIO()
    exif = Image.Exif()
    exif[0x0112] = 6
    Image.new("RGB", (800, 400), "red").save(output, format="JPEG", exif=exif)
    assert dimensions(EmojiImageCache().get("rotated", output.getvalue())) == (128, 256)


def test_cache_enforces_entry_limit_and_refreshes_lru() -> None:
    cache = EmojiImageCache(max_entries=2)
    raw = image_bytes((20, 20))
    first = cache.get("first", raw)
    cache.get("second", raw)
    assert cache.get("first", raw) is first
    cache.get("third", raw)
    assert [key[0] for key in cache._entries] == ["first", "third"]


def test_cache_enforces_byte_limit_and_does_not_keep_oversized_entries() -> None:
    raw = image_bytes((20, 20))
    probe = EmojiImageCache()
    probe.get("key-1", raw)
    one_entry_size = probe._size_bytes
    cache = EmojiImageCache(max_bytes=one_entry_size * 2 - 1)
    for index in range(10):
        cache.get(f"key-{index}", raw)
        assert cache._size_bytes <= cache._max_bytes
        assert len(cache._entries) <= 1
    tiny = EmojiImageCache(max_bytes=1)
    assert dimensions(tiny.get("oversized", raw)) == (20, 20)
    assert not tiny._entries
    assert tiny._size_bytes == 0
    assert not tiny._pending


def test_concurrent_same_image_is_only_converted_once(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = EmojiImageCache()
    started = threading.Event()
    release = threading.Event()
    calls = []
    prepare = cache_module._prepare_emoji_image

    def blocked_prepare(raw: bytes) -> ContextImagePart:
        calls.append(raw)
        started.set()
        assert release.wait(5)
        return prepare(raw)

    monkeypatch.setattr(cache_module, "_prepare_emoji_image", blocked_prepare)
    raw = image_bytes((1024, 512))
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(cache.get, "shared", raw) for _ in range(8)]
        try:
            assert started.wait(5)
        finally:
            release.set()
        parts = [future.result(timeout=5) for future in futures]
    assert len(calls) == 1
    assert all(part is parts[0] for part in parts)
    assert not cache._pending


def test_at_most_two_conversions_run_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = EmojiImageCache()
    release = threading.Event()
    two_started = threading.Event()
    lock = threading.Lock()
    active = 0
    peak = 0
    prepare = cache_module._prepare_emoji_image

    def blocked_prepare(raw: bytes) -> ContextImagePart:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == 2:
                two_started.set()
        try:
            assert release.wait(5)
            return prepare(raw)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(cache_module, "_prepare_emoji_image", blocked_prepare)
    raw = image_bytes((600, 300))
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(cache.get, str(index), raw) for index in range(8)]
        try:
            assert two_started.wait(5)
        finally:
            release.set()
        for future in futures:
            future.result(timeout=5)
    assert peak == 2
    assert not cache._pending


def test_failed_conversion_is_exposed_and_can_be_retried() -> None:
    cache = EmojiImageCache()
    with pytest.raises(OSError):
        cache.get("retry", b"broken-image")
    assert not cache._entries
    assert not cache._pending
    assert dimensions(cache.get("retry", image_bytes((600, 300)))) == (256, 128)
