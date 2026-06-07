from pathlib import Path

from sqlalchemy import Engine, text

from src.common.logger import get_logger
from src.common.utils.image_path import PROJECT_ROOT, serialize_stored_image_path


logger = get_logger("database")


def normalize_image_storage_paths(engine: Engine) -> None:
    """启动时规整图片路径：项目内绝对路径转相对路径，项目外路径直接丢弃。"""
    with engine.begin() as connection:
        table_exists = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='images'")
        ).first()
        if table_exists is None:
            return

        rows = connection.execute(text("SELECT id, full_path FROM images WHERE full_path IS NOT NULL")).all()
        converted_count = 0
        discarded_count = 0

        for row in rows:
            record_id = row.id
            raw_path = str(row.full_path or "").strip()
            if not raw_path:
                continue

            path = Path(raw_path)
            resolved_path = path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()
            try:
                resolved_path.relative_to(PROJECT_ROOT)
            except ValueError:
                connection.execute(text("DELETE FROM images WHERE id = :id"), {"id": record_id})
                discarded_count += 1
                continue

            normalized_path = serialize_stored_image_path(resolved_path)
            if normalized_path != raw_path:
                connection.execute(
                    text("UPDATE images SET full_path = :full_path WHERE id = :id"),
                    {"id": record_id, "full_path": normalized_path},
                )
                converted_count += 1

    if converted_count or discarded_count:
        logger.info(f"图片路径规整完成：转换 {converted_count} 条，丢弃目录外记录 {discarded_count} 条")
