"""对旧人物事实执行可中断、幂等的原始消息重验。"""

from typing import Any, Dict, List, Tuple

import asyncio
import json
import sqlite3

from src.A_memorix.host_service import a_memorix_host_service
from src.common.logger import get_logger
from src.common.message_repository import find_messages
from src.services.memory_service import memory_service

from .person_fact_verifier import verify_direct_person_fact

logger = get_logger("person_fact_reverification")


def _historical_fact_batch(cursor: str, limit: int) -> Tuple[List[Dict[str, Any]], bool]:
    db_path = a_memorix_host_service.get_runtime_data_dir() / "metadata" / "metadata.db"
    if not db_path.exists():
        return [], False
    connection = sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT c.claim_id, c.scope_id AS person_id, c.value_text,
                   p.metadata AS paragraph_metadata
            FROM fact_claims c
            JOIN fact_evidence e ON e.claim_id = c.claim_id
              AND e.evidence_type = 'paragraph' AND e.stance = 'support'
            JOIN paragraphs p ON p.hash = e.evidence_id
            WHERE c.scope_type = 'person' AND c.status = 'active'
              AND c.authority = 'summary_derived' AND c.stability = 'uncertain'
              AND c.profile_section = 'uncertain_notes' AND c.claim_id > ?
              AND (p.is_deleted IS NULL OR p.is_deleted = 0)
              AND e.evidence_id = (
                  SELECT MIN(e2.evidence_id) FROM fact_evidence e2
                  WHERE e2.claim_id = c.claim_id AND e2.evidence_type = 'paragraph'
                    AND e2.stance = 'support'
              )
            ORDER BY c.claim_id ASC LIMIT ?
            """,
            (cursor, max(1, limit) + 1),
        ).fetchall()
        return [dict(row) for row in rows[:limit]], len(rows) > limit
    finally:
        connection.close()


def _has_verified_message(row: Dict[str, Any], metadata: Dict[str, Any]) -> bool:
    person_id = str(row["person_id"] or "").strip()
    session_id = str(metadata.get("chat_id", "") or "").strip()
    person_name = str(metadata.get("person_name", "") or "").strip()
    message_ids = metadata.get("evidence_message_ids", [])
    if not isinstance(message_ids, list):
        return False
    for message_id in message_ids:
        token = str(message_id or "").strip()
        messages = find_messages(session_id=session_id, message_id=token, limit=2)
        if len(messages) != 1:
            continue
        quote = str(messages[0].processed_plain_text or "").strip()
        if verify_direct_person_fact(
            fact=str(row["value_text"]),
            evidence_message_id=token,
            evidence_quote=quote,
            person_id=person_id,
            person_name=person_name,
            session_id=session_id,
        ):
            return True
    return False


async def reverify_historical_person_facts(cursor: str = "", limit: int = 50) -> Dict[str, Any]:
    """处理一批旧事实；调用方持久化 next_cursor 后可安全续跑。"""

    rows, has_more = await asyncio.to_thread(_historical_fact_batch, cursor, limit)
    promoted = 0
    for row in rows:
        try:
            metadata = json.loads(str(row["paragraph_metadata"] or "{}"))
        except (TypeError, ValueError):
            logger.warning(f"历史人物事实证据元数据无效: claim_id={row['claim_id']}")
            continue
        if not isinstance(metadata, dict):
            continue
        if not await asyncio.to_thread(_has_verified_message, row, metadata):
            continue
        current = await memory_service.fact_admin(action="get", claim_id=str(row["claim_id"]))
        claim = current.get("claim") if isinstance(current, dict) else None
        if not isinstance(claim, dict) or claim.get("authority") != "summary_derived":
            continue
        result = await memory_service.fact_admin(
            action="update",
            claim_id=str(row["claim_id"]),
            authority="direct_user",
            stability="stable",
            profile_section="stable_facts",
            confidence=1.0,
            reason="historical_message_reverified",
        )
        if not result.get("success"):
            raise RuntimeError(f"历史人物事实晋升失败: {row['claim_id']}: {result.get('error')}")
        promoted += 1
    return {
        "processed": len(rows),
        "promoted": promoted,
        "next_cursor": str(rows[-1]["claim_id"]) if rows else cursor,
        "has_more": has_more,
    }
