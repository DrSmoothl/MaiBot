"""验证真实消息来源过滤，不调用模型或写入学习数据库。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncio
import pytest


@pytest.fixture(scope="module")
def learners(tmp_path_factory):
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("MAIBOT_INSTANCE_DIR", str(tmp_path_factory.mktemp("learning-instance")))
        from src.learners import expression_learner, jargon_learner

        yield expression_learner, jargon_learner


def message(platform, user_id):
    return SimpleNamespace(
        platform=platform,
        message_info=SimpleNamespace(user_info=SimpleNamespace(user_id=user_id)),
        message_id=f"{platform}-{user_id}",
        session_id="test-session",
        processed_plain_text="今天真开心",
        is_notify=False,
    )


def test_expression_filters_source_and_matches_platform(learners, monkeypatch):
    expression, _ = learners
    monkeypatch.setattr(expression, "is_bot_self", lambda platform, user: user == "bot")
    monkeypatch.setattr(expression.global_config.expression, "learning_blacklist", [
        {"platform": "qq", "user_id": "123"},
    ])
    sources = [message("qq", "123"), message("other", "123"), message("qq", "456"), message("qq", "bot")]
    candidates = [("开心", "好耶", str(index + 1)) for index in range(len(sources))]
    assert expression.ExpressionLearner._filter_expressions(None, candidates, sources) == [
        ("开心", "好耶"), ("开心", "好耶"),
    ]
    monkeypatch.setattr(expression.global_config.expression, "learning_blacklist", [])
    assert len(expression.ExpressionLearner._filter_expressions(None, candidates, sources)) == 3


def test_jargon_filters_resolved_source_before_miner(learners, monkeypatch):
    _, jargon = learners
    monkeypatch.setattr(jargon, "is_bot_self", lambda platform, user: user == "bot")
    monkeypatch.setattr(jargon.global_config.jargon, "learning_blacklist", [
        {"platform": "qq", "user_id": "123"},
    ])
    sources = [
        jargon.JargonLearningSourceItem(content="今天真开心", original_message=message(platform, user))
        for platform, user in [("qq", "123"), ("other", "123"), ("qq", "456"), ("qq", "bot")]
    ]
    learner = SimpleNamespace(_log_jargon_update_process=Mock())
    miner = SimpleNamespace(process_extracted_entries=AsyncMock(return_value=(2, 0)))
    candidates = [("词甲", "1"), ("词乙", "2"), ("词丙", "3"), ("词丁", "4")]
    assert asyncio.run(jargon.JargonLearner._process_jargon_entries(learner, candidates, sources, miner))
    entries = miner.process_extracted_entries.call_args.args[0]
    assert [entry["content"] for entry in entries] == ["词乙", "词丙"]
    # 黑名单消息仍可作为其他用户来源的上下文，不会改变来源编号。
    assert sources[0].original_message.message_id in [
        item["message_id"] for item in entries[0]["evidence_messages"][0]
    ]
    monkeypatch.setattr(jargon.global_config.jargon, "learning_blacklist", [])
    asyncio.run(jargon.JargonLearner._process_jargon_entries(learner, candidates, sources, miner))
    assert len(miner.process_extracted_entries.call_args.args[0]) == 3
