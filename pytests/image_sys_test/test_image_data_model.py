from pathlib import Path
from types import SimpleNamespace
from PIL import Image as PILImage

import hashlib
import io
import pytest

from src.common.data_models.image_data_model import MaiEmoji, MaiImage
from src.common.utils import image_path as image_path_utils
from src.common.utils.image_path import StoredImagePathError


def _build_test_image_bytes(image_format: str) -> bytes:
    image = PILImage.new("RGB", (8, 8), color="white")
    buffer = io.BytesIO()
    image.save(buffer, format=image_format)
    return buffer.getvalue()


@pytest.mark.asyncio
async def test_calculate_hash_format_updates_runtime_path_metadata(tmp_path: Path) -> None:
    image_bytes = _build_test_image_bytes("JPEG")
    tmp_file_path = tmp_path / "emoji.tmp"
    tmp_file_path.write_bytes(image_bytes)

    emoji = MaiEmoji(full_path=tmp_file_path, image_bytes=image_bytes)

    assert await emoji.calculate_hash_format() is True
    assert emoji.image_format == "jpeg"
    assert emoji.full_path.suffix == ".jpeg"
    assert emoji.file_name == emoji.full_path.name
    assert emoji.dir_path == tmp_path.resolve()


@pytest.mark.asyncio
async def test_calculate_hash_format_reuses_existing_target_file(tmp_path: Path) -> None:
    image_bytes = _build_test_image_bytes("JPEG")
    tmp_file_path = tmp_path / "emoji.tmp"
    target_file_path = tmp_path / "emoji.jpeg"
    tmp_file_path.write_bytes(image_bytes)
    target_file_path.write_bytes(image_bytes)

    emoji = MaiEmoji(full_path=tmp_file_path, image_bytes=image_bytes)

    assert await emoji.calculate_hash_format() is True
    assert emoji.full_path == target_file_path.resolve()
    assert emoji.file_name == target_file_path.name
    assert not tmp_file_path.exists()
    assert target_file_path.exists()


@pytest.mark.asyncio
async def test_calculate_hash_format_reuses_existing_hashed_file_when_tmp_was_consumed(tmp_path: Path) -> None:
    image_bytes = _build_test_image_bytes("PNG")
    image_hash = hashlib.sha256(image_bytes).hexdigest()
    existing_file_path = tmp_path / f"{image_hash}.png"
    tmp_file_path = tmp_path / f"{image_hash}.tmp"
    existing_file_path.write_bytes(image_bytes)
    tmp_file_path.write_bytes(image_bytes)

    emoji = MaiEmoji(full_path=tmp_file_path, image_bytes=image_bytes)
    tmp_file_path.unlink()

    assert await emoji.calculate_hash_format() is True
    assert emoji.full_path == existing_file_path.resolve()
    assert emoji.file_name == existing_file_path.name
    assert not tmp_file_path.exists()


@pytest.mark.parametrize(
    ("model_cls", "extra_fields"),
    [
        (
            MaiEmoji,
            {
                "description": "",
                "last_used_time": None,
                "query_count": 0,
                "register_time": None,
            },
        ),
        (
            MaiImage,
            {
                "description": "",
                "vlm_processed": False,
            },
        ),
    ],
)
def test_from_db_instance_restores_image_format_from_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    model_cls: type[MaiEmoji] | type[MaiImage],
    extra_fields: dict[str, object],
) -> None:
    monkeypatch.setattr(image_path_utils, "PROJECT_ROOT", tmp_path.resolve())
    image_path = tmp_path / "cached.png"
    image_path.write_bytes(_build_test_image_bytes("PNG"))

    record = SimpleNamespace(
        no_file_flag=False,
        image_hash="hash",
        full_path=str(image_path),
        **extra_fields,
    )

    image = model_cls.from_db_instance(record)

    assert image.full_path == image_path.resolve()
    assert image.file_name == image_path.name
    assert image.image_format == "png"


def test_to_db_instance_rejects_project_external_path(tmp_path: Path) -> None:
    image_path = tmp_path / "external.png"
    image_bytes = _build_test_image_bytes("PNG")
    image_path.write_bytes(image_bytes)

    emoji = MaiEmoji(full_path=image_path, image_bytes=image_bytes)
    emoji.file_hash = "hash"

    with pytest.raises(StoredImagePathError):
        emoji.to_db_instance()
