from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Literal, Sequence

from sqlalchemy import and_, not_, or_
from sqlmodel import col, delete, select

import jieba
import re

from src.chat.utils.utils import is_bot_self
from src.common.database.database import get_db_session
from src.common.database.database_model import HighFrequencyTerm, Messages

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_CQ_CODE_RE = re.compile(r"\[CQ:[^\]]+\]")
_REPLY_QUOTE_RE = re.compile(r"\[回复了[^\]]{0,2000}?的消息[:：][^\]]*\]", re.DOTALL)
_INACCESSIBLE_REPLY_RE = re.compile(r"\[回复了[^\]]{0,200}?消息[，,][^\]]*?原消息[^\]]*?无法访问\]", re.DOTALL)
_MENTION_RE = re.compile(r"@\S+")
_MEDIA_HINT_RE = re.compile(r"\[(?:图片|表情|语音|视频|文件|转发|回复|image|emoji|voice|video|file)\]", re.IGNORECASE)
_TECH_TERM_RE = re.compile(r"(?<![A-Za-z0-9_])([A-Za-z][A-Za-z0-9_+./#-]{1,})(?![A-Za-z0-9_])")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_LATIN_RE = re.compile(r"[a-zA-Z]")
_NUMERIC_RE = re.compile(r"[\d._+-]+")
_PUNCT_ONLY_RE = re.compile(r"[\W_]+", re.UNICODE)
_TOKEN_TRIM_CHARS = " \t\r\n.,;:!?()[]{}<>\"'`~@#$%^&*=|\\/，。！？；：、（）【】《》“”‘’…"

_CN_STOP_WORDS = {
    "一些",
    "一样",
    "一定",
    "一条",
    "一种",
    "一起",
    "一下",
    "一个",
    "一会",
    "一是",
    "不是",
    "不过",
    "不如",
    "不能",
    "不要",
    "不用",
    "但原",
    "为了",
    "为什么",
    "也是",
    "也许",
    "于是",
    "什么",
    "他们",
    "以后",
    "以前",
    "以及",
    "按照",
    "但是",
    "你们",
    "你的",
    "其实",
    "其它",
    "其他",
    "只是",
    "只要",
    "可以",
    "可是",
    "没有",
    "各位",
    "因为",
    "因此",
    "如果",
    "它们",
    "对于",
    "对方",
    "就是",
    "已经",
    "并且",
    "怎么",
    "怎样",
    "总之",
    "我们",
    "我的",
    "所以",
    "所有",
    "是否",
    "是不是",
    "未知",
    "未知用户",
    "有人",
    "无法访问",
    "然后",
    "现在",
    "关于",
    "根据",
    "由于",
    "的话",
    "直接",
    "真的",
    "自己",
    "虽然",
    "这是",
    "通过",
    "这个",
    "这些",
    "这么",
    "这里",
    "这样",
    "那个",
    "那些",
    "那么",
    "那里",
    "那样",
    "还是",
    "还有",
    "或者",
    "而且",
    "来说",
    "一下子",
    "哈哈",
    "哈哈哈",
}
_EN_STOP_WORDS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "but",
    "by",
    "can",
    "could",
    "for",
    "from",
    "had",
    "has",
    "have",
    "how",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "just",
    "may",
    "more",
    "not",
    "of",
    "on",
    "or",
    "our",
    "so",
    "that",
    "the",
    "their",
    "then",
    "there",
    "this",
    "to",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "will",
    "with",
    "would",
    "you",
    "your",
}
_STOP_WORDS = _CN_STOP_WORDS | _EN_STOP_WORDS


@dataclass(frozen=True)
class WordFrequencyItem:
    """消息词频统计项。"""

    term: str
    count: int
    message_count: int
    frequency: float
    message_frequency: float
    term_type: Literal["word", "phrase"] = "word"


@dataclass(frozen=True)
class WordFrequencyStatistics:
    """最近消息词频统计结果。"""

    generated_at: datetime
    start_time: datetime
    end_time: datetime
    lookback_days: int
    message_count: int
    term_count: int
    terms: list[WordFrequencyItem]


async def get_recent_message_word_frequency(
    *,
    days: int = 30,
    limit: int = 50,
    min_count: int = 2,
    text_only: bool = False,
) -> WordFrequencyStatistics:
    """统计最近一段时间内非 bot 消息的高频词和词组。"""

    lookback_days = max(1, int(days))
    end_time = datetime.now()
    start_time = end_time - timedelta(days=lookback_days)
    texts = _fetch_recent_non_bot_message_texts(start_time, end_time, text_only=text_only)

    return build_message_word_frequency_statistics(
        texts,
        start_time=start_time,
        end_time=end_time,
        lookback_days=lookback_days,
        limit=limit,
        min_count=min_count,
    )


async def refresh_high_frequency_terms(
    *,
    days: int = 30,
    limit: int = 1000,
    min_count: int = 2,
) -> int:
    """重新统计最近非 bot 纯文本消息的高频词，并刷新高频词词库。"""

    lookback_days = max(1, int(days))
    end_time = datetime.now()
    start_time = end_time - timedelta(days=lookback_days)
    texts = _fetch_recent_non_bot_message_texts(start_time, end_time, text_only=True)
    stats = build_message_word_frequency_statistics(
        texts,
        start_time=start_time,
        end_time=end_time,
        lookback_days=lookback_days,
        limit=limit,
        min_count=min_count,
    )
    return store_high_frequency_terms(stats)


def update_high_frequency_terms_from_context_messages(
    context_messages: Sequence[object],
    *,
    limit: int = 1000,
    min_count: int = 2,
    max_terms: int = 1000,
) -> int:
    """从 Maisaka 裁切上下文消息批次中提取真实用户消息，并增量更新高频词词库。"""

    source_messages = _extract_user_messages_from_context(context_messages)
    return update_high_frequency_terms_from_messages(
        source_messages,
        limit=limit,
        min_count=min_count,
        max_terms=max_terms,
    )


def update_high_frequency_terms_from_messages(
    messages: Iterable[object],
    *,
    limit: int = 1000,
    min_count: int = 2,
    max_terms: int = 1000,
) -> int:
    """从一批真实消息中增量更新高频词词库。"""

    texts: list[str] = []
    timestamps: list[datetime] = []
    for message in messages:
        if not _is_countable_user_text_message(message):
            continue

        text = str(getattr(message, "processed_plain_text", "") or "").strip()
        if not text:
            continue

        texts.append(text)
        timestamp = getattr(message, "timestamp", None)
        if isinstance(timestamp, datetime):
            timestamps.append(timestamp)

    if not texts:
        return 0

    now = datetime.now()
    start_time = min(timestamps) if timestamps else now
    end_time = max(timestamps) if timestamps else now
    stats = build_message_word_frequency_statistics(
        texts,
        start_time=start_time,
        end_time=end_time,
        lookback_days=0,
        limit=limit,
        min_count=min_count,
    )
    return merge_high_frequency_terms(stats, max_terms=max_terms)


def build_message_word_frequency_statistics(
    texts: Iterable[str],
    *,
    start_time: datetime,
    end_time: datetime,
    lookback_days: int,
    limit: int = 50,
    min_count: int = 2,
) -> WordFrequencyStatistics:
    """从纯文本消息构建词频统计，便于接口和测试复用。"""

    normalized_limit = max(1, int(limit))
    normalized_min_count = max(1, int(min_count))
    word_counter: Counter[str] = Counter()
    phrase_counter: Counter[str] = Counter()
    word_message_counter: Counter[str] = Counter()
    phrase_message_counter: Counter[str] = Counter()
    message_count = 0

    for text in texts:
        if not isinstance(text, str) or not text.strip():
            continue

        message_count += 1
        tokens = _tokenize_meaningful_terms(text)
        if not tokens:
            continue

        phrases = list(_iter_phrases(tokens))
        word_counter.update(tokens)
        phrase_counter.update(phrases)
        word_message_counter.update(set(tokens))
        phrase_message_counter.update(set(phrases))

    total_term_count = sum(word_counter.values()) + sum(phrase_counter.values())
    terms = _build_frequency_items(
        word_counter=word_counter,
        phrase_counter=phrase_counter,
        word_message_counter=word_message_counter,
        phrase_message_counter=phrase_message_counter,
        message_count=message_count,
        total_term_count=total_term_count,
        min_count=normalized_min_count,
        limit=normalized_limit,
    )

    return WordFrequencyStatistics(
        generated_at=datetime.now(),
        start_time=start_time,
        end_time=end_time,
        lookback_days=lookback_days,
        message_count=message_count,
        term_count=total_term_count,
        terms=terms,
    )


def store_high_frequency_terms(stats: WordFrequencyStatistics) -> int:
    """将高频词统计结果写入 ``high_frequency_terms`` 词库表。"""

    records: list[HighFrequencyTerm] = []
    seen_terms: set[str] = set()
    for item in stats.terms:
        normalized_term = _normalize_term_for_match(item.term)
        if not normalized_term or normalized_term in seen_terms:
            continue
        seen_terms.add(normalized_term)
        records.append(
            HighFrequencyTerm(
                rank=len(records) + 1,
                term=item.term,
                normalized_term=normalized_term,
                term_type=item.term_type,
                occurrence_count=item.count,
                message_count=item.message_count,
                frequency=item.frequency,
                message_frequency=item.message_frequency,
                updated_at=stats.generated_at,
            )
        )

    with get_db_session(auto_commit=False) as session:
        session.exec(delete(HighFrequencyTerm))
        session.add_all(records)
        session.commit()

    return len(records)


def merge_high_frequency_terms(stats: WordFrequencyStatistics, *, max_terms: int = 1000) -> int:
    """将一批统计结果合并进当前高频词词库，保持每个归一化词仅一行。"""

    max_term_count = max(1, int(max_terms))
    with get_db_session(auto_commit=False) as session:
        records = list(session.exec(select(HighFrequencyTerm)).all())
        records_by_term = {record.normalized_term: record for record in records if record.normalized_term}
        merged_count = 0

        for item in stats.terms:
            normalized_term = _normalize_term_for_match(item.term)
            if not normalized_term:
                continue

            existing_record = records_by_term.get(normalized_term)
            if existing_record is None:
                existing_record = HighFrequencyTerm(
                    term=item.term,
                    normalized_term=normalized_term,
                    term_type=item.term_type,
                    occurrence_count=0,
                    message_count=0,
                    created_at=stats.generated_at,
                    updated_at=stats.generated_at,
                )
                records.append(existing_record)
                records_by_term[normalized_term] = existing_record

            existing_record.term = item.term
            existing_record.term_type = item.term_type
            existing_record.occurrence_count += item.count
            existing_record.message_count += item.message_count
            existing_record.updated_at = stats.generated_at
            merged_count += 1

        kept_records, removed_records = _rerank_high_frequency_records(records, max_terms=max_term_count)
        session.add_all(kept_records)
        for record in removed_records:
            if record.id is not None:
                session.delete(record)
        session.commit()

    return merged_count


def _fetch_recent_non_bot_message_texts(
    start_time: datetime,
    end_time: datetime,
    *,
    text_only: bool = False,
) -> list[str]:
    conditions = [
        Messages.message_id != "notice",
        col(Messages.processed_plain_text).is_not(None),
        Messages.processed_plain_text != "",
        Messages.timestamp >= start_time,
        Messages.timestamp <= end_time,
        Messages.is_notify == False,  # noqa: E712
        Messages.is_command == False,  # noqa: E712
    ]

    bot_exclusion = _build_bot_exclusion_condition()
    if bot_exclusion is not None:
        conditions.append(bot_exclusion)
    if text_only:
        conditions.append(Messages.is_picture == False)  # noqa: E712
        conditions.append(Messages.is_emoji == False)  # noqa: E712

    with get_db_session(auto_commit=False) as session:
        rows = session.exec(select(Messages.processed_plain_text).where(*conditions)).all()

    return [text for text in rows if isinstance(text, str) and text.strip()]


def _extract_user_messages_from_context(context_messages: Sequence[object]) -> list[object]:
    from src.maisaka.context.messages import SessionBackedMessage

    source_messages: list[object] = []
    seen_message_ids: set[str] = set()
    seen_object_ids: set[int] = set()

    for context_message in context_messages:
        if not isinstance(context_message, SessionBackedMessage):
            continue
        if context_message.source_kind != "user":
            continue

        original_message = context_message.original_message
        if original_message is None:
            continue

        message_id = str(getattr(original_message, "message_id", "") or "").strip()
        if message_id:
            if message_id in seen_message_ids:
                continue
            seen_message_ids.add(message_id)
        else:
            object_id = id(original_message)
            if object_id in seen_object_ids:
                continue
            seen_object_ids.add(object_id)

        source_messages.append(original_message)

    return source_messages


def _is_countable_user_text_message(message: object) -> bool:
    if str(getattr(message, "message_id", "") or "").strip() == "notice":
        return False
    if bool(getattr(message, "is_notify", False)) or bool(getattr(message, "is_command", False)):
        return False
    if bool(getattr(message, "is_picture", False)) or bool(getattr(message, "is_emoji", False)):
        return False

    user_info = getattr(getattr(message, "message_info", None), "user_info", None)
    user_id = str(getattr(user_info, "user_id", "") or "").strip()
    platform = str(getattr(message, "platform", "") or "").strip()
    if user_id and is_bot_self(platform, user_id):
        return False

    return bool(str(getattr(message, "processed_plain_text", "") or "").strip())


def _rerank_high_frequency_records(
    records: list[HighFrequencyTerm],
    *,
    max_terms: int,
) -> tuple[list[HighFrequencyTerm], list[HighFrequencyTerm]]:
    sorted_records = sorted(records, key=_high_frequency_record_sort_key)
    kept_records = sorted_records[:max_terms]
    removed_records = sorted_records[max_terms:]
    total_occurrence_count = sum(record.occurrence_count for record in kept_records)
    total_message_count = sum(record.message_count for record in kept_records)

    for rank, record in enumerate(kept_records, start=1):
        record.rank = rank
        record.frequency = (
            record.occurrence_count / total_occurrence_count if total_occurrence_count > 0 else 0.0
        )
        record.message_frequency = record.message_count / total_message_count if total_message_count > 0 else 0.0

    return kept_records, removed_records


def _high_frequency_record_sort_key(record: HighFrequencyTerm) -> tuple[int, int, int, int, str]:
    phrase_priority = 0 if record.term_type == "phrase" else 1
    return (
        -record.occurrence_count,
        -record.message_count,
        phrase_priority,
        -len(record.term or ""),
        record.normalized_term or "",
    )


def _normalize_term_for_match(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _build_bot_exclusion_condition():
    from src.chat.utils.utils import get_all_bot_accounts, get_bot_account

    bot_accounts = get_all_bot_accounts()
    exclusion_conditions = []
    if bot_accounts:
        exclusion_conditions.append(
            or_(
                *[
                    and_(Messages.platform == platform_name, Messages.user_id == account)
                    for platform_name, account in bot_accounts.items()
                ]
            )
        )

    # 兼容旧数据：历史机器人消息可能在所有平台上都使用 QQ 账号作为 user_id 存储。
    if qq_fallback := get_bot_account("qq"):
        exclusion_conditions.append(Messages.user_id == qq_fallback)

    if not exclusion_conditions:
        return None
    return not_(or_(*exclusion_conditions))


def _tokenize_meaningful_terms(text: str) -> list[str]:
    cleaned_text = _normalize_text(text)
    terms: list[str] = []

    for raw_token in jieba.cut(cleaned_text):
        token = _normalize_token(raw_token)
        if _is_meaningful_token(token):
            terms.append(token)

    existing_terms = set(terms)
    for match in _TECH_TERM_RE.finditer(cleaned_text):
        token = _normalize_token(match.group(1))
        if token not in existing_terms and _is_meaningful_token(token):
            terms.append(token)
            existing_terms.add(token)

    return terms


def _normalize_text(text: str) -> str:
    without_urls = _URL_RE.sub(" ", text)
    without_codes = _CQ_CODE_RE.sub(" ", without_urls)
    without_failed_replies = _INACCESSIBLE_REPLY_RE.sub(" ", without_codes)
    without_reply_quotes = _REPLY_QUOTE_RE.sub(" ", without_failed_replies)
    without_mentions = _MENTION_RE.sub(" ", without_reply_quotes)
    return _MEDIA_HINT_RE.sub(" ", without_mentions)


def _normalize_token(raw_token: str) -> str:
    token = raw_token.strip().lower().strip(_TOKEN_TRIM_CHARS)
    return re.sub(r"\s+", " ", token)


def _is_meaningful_token(token: str) -> bool:
    if not token or token in _STOP_WORDS:
        return False
    if _NUMERIC_RE.fullmatch(token) or _PUNCT_ONLY_RE.fullmatch(token):
        return False

    if _contains_cjk(token):
        return _count_cjk_chars(token) >= 2

    if _LATIN_RE.search(token):
        return len(token) >= 2

    return len(token) >= 2


def _iter_phrases(tokens: list[str], *, max_size: int = 3) -> Iterable[str]:
    for phrase_size in range(2, max(2, max_size) + 1):
        if len(tokens) < phrase_size:
            break
        for index in range(0, len(tokens) - phrase_size + 1):
            phrase_tokens = tokens[index : index + phrase_size]
            phrase = _join_phrase(phrase_tokens)
            if _is_meaningful_phrase(phrase, phrase_tokens):
                yield phrase


def _join_phrase(tokens: list[str]) -> str:
    if all(_contains_cjk(token) and not _LATIN_RE.search(token) for token in tokens):
        return "".join(tokens)
    return " ".join(tokens)


def _is_meaningful_phrase(phrase: str, tokens: list[str]) -> bool:
    if phrase in _STOP_WORDS or len(set(tokens)) == 1:
        return False
    if _contains_cjk(phrase):
        return _count_cjk_chars(phrase) >= 4
    return any(_LATIN_RE.search(token) for token in tokens)


def _build_frequency_items(
    *,
    word_counter: Counter[str],
    phrase_counter: Counter[str],
    word_message_counter: Counter[str],
    phrase_message_counter: Counter[str],
    message_count: int,
    total_term_count: int,
    min_count: int,
    limit: int,
) -> list[WordFrequencyItem]:
    items = []
    items.extend(
        _counter_to_items(
            counter=word_counter,
            message_counter=word_message_counter,
            term_type="word",
            message_count=message_count,
            total_term_count=total_term_count,
            min_count=min_count,
        )
    )
    items.extend(
        _counter_to_items(
            counter=phrase_counter,
            message_counter=phrase_message_counter,
            term_type="phrase",
            message_count=message_count,
            total_term_count=total_term_count,
            min_count=min_count,
        )
    )

    return sorted(items, key=_frequency_item_sort_key)[:limit]


def _counter_to_items(
    *,
    counter: Counter[str],
    message_counter: Counter[str],
    term_type: Literal["word", "phrase"],
    message_count: int,
    total_term_count: int,
    min_count: int,
) -> list[WordFrequencyItem]:
    items: list[WordFrequencyItem] = []
    for term, count in counter.items():
        if count < min_count:
            continue
        term_message_count = int(message_counter.get(term, 0))
        items.append(
            WordFrequencyItem(
                term=term,
                count=int(count),
                message_count=term_message_count,
                frequency=count / total_term_count if total_term_count > 0 else 0.0,
                message_frequency=term_message_count / message_count if message_count > 0 else 0.0,
                term_type=term_type,
            )
        )
    return items


def _frequency_item_sort_key(item: WordFrequencyItem) -> tuple[int, int, int, int, str]:
    phrase_priority = 0 if item.term_type == "phrase" else 1
    return (-item.count, -item.message_count, phrase_priority, -len(item.term), item.term)


def _contains_cjk(text: str) -> bool:
    return bool(_CJK_RE.search(text))


def _count_cjk_chars(text: str) -> int:
    return len(_CJK_RE.findall(text))
