from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Generator

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine
import pytest

from src.chat.replyer import maisaka_expression_selector as selector_module
from src.chat.replyer.maisaka_expression_selector import MaisakaExpressionSelector
from src.common.database.database_model import Expression, ModifiedBy
from src.common.utils.utils_session import SessionUtils


class _FakeHookManager:
    def __init__(self, responses: dict[str, SimpleNamespace]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def invoke_hook(self, hook_name: str, **kwargs: Any) -> SimpleNamespace:
        self.calls.append((hook_name, dict(kwargs)))
        return self.responses.get(hook_name, SimpleNamespace(kwargs=dict(kwargs), aborted=False))


def _build_target(platform: str, item_id: str, rule_type: str = "group") -> SimpleNamespace:
    return SimpleNamespace(platform=platform, item_id=item_id, rule_type=rule_type)


def test_resolve_expression_group_scope_returns_related_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    current_session_id = SessionUtils.calculate_session_id("qq", group_id="10001")
    related_session_id = SessionUtils.calculate_session_id("qq", group_id="10002")

    monkeypatch.setattr(
        selector_module,
        "global_config",
        SimpleNamespace(
            expression=SimpleNamespace(
                expression_groups=[
                    SimpleNamespace(
                        targets=[
                            _build_target("qq", "10001"),
                            _build_target("qq", "10002"),
                        ]
                    )
                ]
            )
        ),
    )
    target_session_ids = {
        "10001": current_session_id,
        "10002": related_session_id,
    }
    monkeypatch.setattr(
        selector_module.ChatConfigUtils,
        "get_target_session_ids",
        lambda target_item: {target_session_ids[target_item.item_id]},
    )
    monkeypatch.setattr(
        selector_module.ChatConfigUtils,
        "target_matches_session",
        lambda target_item, session_id: target_item.item_id == "10001" and session_id == current_session_id,
    )

    selector = MaisakaExpressionSelector()
    related_session_ids, has_global_share = selector._resolve_expression_group_scope(current_session_id)

    assert related_session_ids == {current_session_id, related_session_id}
    assert has_global_share is False


def test_resolve_expression_group_scope_matches_routed_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    current_session_id = SessionUtils.calculate_session_id("qq", group_id="10001", account_id="bot-a")
    related_session_id = SessionUtils.calculate_session_id("qq", group_id="10002", account_id="bot-a")

    monkeypatch.setattr(
        selector_module,
        "global_config",
        SimpleNamespace(
            expression=SimpleNamespace(
                expression_groups=[
                    SimpleNamespace(
                        targets=[
                            _build_target("qq", "10001"),
                            _build_target("qq", "10002"),
                        ]
                    )
                ]
            )
        ),
    )
    monkeypatch.setattr(
        selector_module.ChatConfigUtils,
        "_get_chat_stream",
        lambda session_id: SimpleNamespace(platform="qq", group_id="10001", user_id=None)
        if session_id == current_session_id
        else None,
    )
    target_session_ids = {
        "10001": current_session_id,
        "10002": related_session_id,
    }
    monkeypatch.setattr(
        selector_module.ChatConfigUtils,
        "get_target_session_ids",
        lambda target_item: {target_session_ids[target_item.item_id]},
    )

    selector = MaisakaExpressionSelector()
    related_session_ids, has_global_share = selector._resolve_expression_group_scope(current_session_id)

    assert related_session_ids == {current_session_id, related_session_id}
    assert has_global_share is False


def test_resolve_expression_group_scope_uses_star_as_global_share(monkeypatch: pytest.MonkeyPatch) -> None:
    current_session_id = SessionUtils.calculate_session_id("qq", group_id="10001")

    monkeypatch.setattr(
        selector_module,
        "global_config",
        SimpleNamespace(
            expression=SimpleNamespace(
                expression_groups=[
                    SimpleNamespace(
                        targets=[
                            _build_target("*", "*"),
                        ]
                    )
                ]
            )
        ),
    )

    selector = MaisakaExpressionSelector()
    related_session_ids, has_global_share = selector._resolve_expression_group_scope(current_session_id)

    assert related_session_ids == {current_session_id}
    assert has_global_share is True


def test_resolve_expression_group_scope_does_not_treat_empty_target_as_global(monkeypatch: pytest.MonkeyPatch) -> None:
    current_session_id = SessionUtils.calculate_session_id("qq", group_id="10001")

    monkeypatch.setattr(
        selector_module,
        "global_config",
        SimpleNamespace(
            expression=SimpleNamespace(
                expression_groups=[
                    SimpleNamespace(
                        targets=[
                            _build_target("", ""),
                        ]
                    )
                ]
            )
        ),
    )

    selector = MaisakaExpressionSelector()
    related_session_ids, has_global_share = selector._resolve_expression_group_scope(current_session_id)

    assert related_session_ids == {current_session_id}
    assert has_global_share is False


def test_load_expression_candidates_checked_only_requires_user_review(monkeypatch: pytest.MonkeyPatch) -> None:
    """仅用已检查表达时，只允许人工 USER 检查过的表达进入候选池。"""

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    session_id = "session-a"
    now = datetime.now()
    user_checked_ids: set[int] = set()

    with Session(engine) as session:
        for index in range(10):
            expression = Expression(
                situation=f"人工情景{index}",
                style=f"人工风格{index}",
                content_list="[]",
                count=1,
                session_id=session_id,
                checked=True,
                modified_by=ModifiedBy.USER,
                create_time=now,
                last_active_time=now,
            )
            session.add(expression)
            session.flush()
            assert expression.id is not None
            user_checked_ids.add(expression.id)

        for index in range(10):
            session.add(
                Expression(
                    situation=f"AI情景{index}",
                    style=f"AI风格{index}",
                    content_list="[]",
                    count=1,
                    session_id=session_id,
                    checked=True,
                    modified_by=ModifiedBy.AI,
                    create_time=now,
                    last_active_time=now,
                )
            )
        session.commit()

    @contextmanager
    def fake_get_db_session(auto_commit: bool = True) -> Generator[Session, None, None]:
        session = Session(engine)
        try:
            yield session
            if auto_commit:
                session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(selector_module, "get_db_session", fake_get_db_session)
    monkeypatch.setattr(selector_module, "weighted_sample", lambda items, count: list(items[:count]))
    monkeypatch.setattr(
        selector_module,
        "global_config",
        SimpleNamespace(
            expression=SimpleNamespace(
                expression_checked_only=True,
                expression_groups=[],
            )
        ),
    )

    candidates = MaisakaExpressionSelector()._load_expression_candidates(session_id)

    assert candidates
    assert {candidate["id"] for candidate in candidates}.issubset(user_checked_ids)


@pytest.mark.asyncio
async def test_select_for_reply_can_be_aborted_by_before_select_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_manager = _FakeHookManager(
        {
            "expression.select.before_select": SimpleNamespace(kwargs={}, aborted=True),
        }
    )
    selector = MaisakaExpressionSelector()

    monkeypatch.setattr(selector, "_get_runtime_manager", lambda: fake_manager)
    monkeypatch.setattr(selector, "_can_use_expressions", lambda session_id: True)
    monkeypatch.setattr(
        selector,
        "_load_expression_candidates",
        lambda session_id: [
            {"id": 1, "situation": "有人开玩笑", "style": "轻松吐槽", "count": 1},
            {"id": 2, "situation": "气氛沉默", "style": "主动接话", "count": 1},
        ],
    )

    result = await selector.select_for_reply(
        session_id="session-1",
        chat_history=[],
        reply_message=None,
        reply_reason="",
        sub_agent_runner=None,
    )

    assert result.expression_habits == ""
    assert result.selected_expression_ids == []
    assert fake_manager.calls[0][0] == "expression.select.before_select"


@pytest.mark.asyncio
async def test_select_for_reply_passes_reply_tool_args_to_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_manager = _FakeHookManager({})
    selector = MaisakaExpressionSelector()

    monkeypatch.setattr(selector, "_get_runtime_manager", lambda: fake_manager)
    monkeypatch.setattr(selector, "_can_use_expressions", lambda session_id: True)
    monkeypatch.setattr(
        selector,
        "_load_expression_candidates",
        lambda session_id: [
            {"id": 1, "situation": "有人开玩笑", "style": "轻松吐槽", "count": 1},
            {"id": 2, "situation": "气氛沉默", "style": "主动接话", "count": 1},
        ],
    )
    monkeypatch.setattr(selector, "_update_last_active_time", lambda selected_ids: None)

    await selector.select_for_reply(
        session_id="session-1",
        chat_history=[],
        reply_message=None,
        reply_reason="",
        reply_tool_args={"expression_strategy": "casual"},
        sub_agent_runner=None,
    )

    assert fake_manager.calls[0][1]["reply_tool_args"] == {"expression_strategy": "casual"}
    assert fake_manager.calls[1][1]["reply_tool_args"] == {"expression_strategy": "casual"}


@pytest.mark.asyncio
async def test_select_for_reply_uses_candidates_modified_by_before_select_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    original_candidates = [
        {"id": 1, "situation": "有人开玩笑", "style": "轻松吐槽", "count": 1},
        {"id": 2, "situation": "气氛沉默", "style": "主动接话", "count": 1},
    ]
    fake_manager = _FakeHookManager(
        {
            "expression.select.before_select": SimpleNamespace(
                kwargs={
                    "candidates": [original_candidates[1]],
                    "max_num": 1,
                },
                aborted=False,
            ),
        }
    )
    selector = MaisakaExpressionSelector()

    monkeypatch.setattr(selector, "_get_runtime_manager", lambda: fake_manager)
    monkeypatch.setattr(selector, "_can_use_expressions", lambda session_id: True)
    monkeypatch.setattr(selector, "_load_expression_candidates", lambda session_id: list(original_candidates))
    monkeypatch.setattr(selector, "_update_last_active_time", lambda selected_ids: None)

    result = await selector.select_for_reply(
        session_id="session-1",
        chat_history=[],
        reply_message=None,
        reply_reason="",
        sub_agent_runner=None,
    )

    assert result.selected_expression_ids == [2]
    assert result.selected_expressions == [original_candidates[1]]
    assert "主动接话" in result.expression_habits
    assert fake_manager.calls[0][0] == "expression.select.before_select"
    assert fake_manager.calls[1][0] == "expression.select.after_selection"


@pytest.mark.asyncio
async def test_select_for_reply_uses_ids_modified_by_after_selection_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    candidates = [
        {"id": 1, "situation": "有人开玩笑", "style": "轻松吐槽", "count": 1},
        {"id": 2, "situation": "气氛沉默", "style": "主动接话", "count": 1},
    ]
    fake_manager = _FakeHookManager(
        {
            "expression.select.after_selection": SimpleNamespace(
                kwargs={
                    "selected_expression_ids": [2],
                    "selected_expressions": [candidates[0], candidates[1]],
                },
                aborted=False,
            ),
        }
    )
    selector = MaisakaExpressionSelector()

    monkeypatch.setattr(selector, "_get_runtime_manager", lambda: fake_manager)
    monkeypatch.setattr(selector, "_can_use_expressions", lambda session_id: True)
    monkeypatch.setattr(selector, "_load_expression_candidates", lambda session_id: list(candidates))
    monkeypatch.setattr(selector, "_update_last_active_time", lambda selected_ids: None)

    result = await selector.select_for_reply(
        session_id="session-1",
        chat_history=[],
        reply_message=None,
        reply_reason="",
        sub_agent_runner=None,
    )

    assert result.selected_expression_ids == [2]
    assert "主动接话" in result.expression_habits
    assert "轻松吐槽" not in result.expression_habits


@pytest.mark.asyncio
async def test_select_for_reply_uses_direct_selection_when_precise_selection_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = [
        {"id": 1, "situation": "有人开玩笑", "style": "轻松吐槽", "count": 1},
        {"id": 2, "situation": "气氛沉默", "style": "主动接话", "count": 1},
    ]
    selector = MaisakaExpressionSelector()
    sub_agent_calls: list[str] = []

    async def fake_sub_agent(prompt: str) -> str:
        sub_agent_calls.append(prompt)
        return '{"selected_ids":[2]}'

    monkeypatch.setattr(selector, "_get_runtime_manager", lambda: _FakeHookManager({}))
    monkeypatch.setattr(selector, "_can_use_expressions", lambda session_id: True)
    monkeypatch.setattr(selector, "_load_expression_candidates", lambda session_id: list(candidates))
    monkeypatch.setattr(selector, "_update_last_active_time", lambda selected_ids: None)
    monkeypatch.setattr(
        selector_module,
        "global_config",
        SimpleNamespace(
            expression=SimpleNamespace(
                enable_precise_expression_selection=False,
            )
        ),
    )

    result = await selector.select_for_reply(
        session_id="session-1",
        chat_history=[],
        reply_message=None,
        reply_reason="",
        sub_agent_runner=fake_sub_agent,
    )

    assert sub_agent_calls == []
    assert result.selected_expression_ids == [1, 2]
    assert result.selected_expressions == candidates


@pytest.mark.asyncio
async def test_select_for_reply_uses_sub_agent_when_precise_selection_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = [
        {"id": 1, "situation": "有人开玩笑", "style": "轻松吐槽", "count": 1},
        {"id": 2, "situation": "气氛沉默", "style": "主动接话", "count": 1},
    ]
    selector = MaisakaExpressionSelector()
    sub_agent_calls: list[str] = []

    async def fake_sub_agent(prompt: str) -> str:
        sub_agent_calls.append(prompt)
        return '{"selected_ids":[2]}'

    monkeypatch.setattr(selector, "_get_runtime_manager", lambda: _FakeHookManager({}))
    monkeypatch.setattr(selector, "_can_use_expressions", lambda session_id: True)
    monkeypatch.setattr(selector, "_load_expression_candidates", lambda session_id: list(candidates))
    monkeypatch.setattr(selector, "_update_last_active_time", lambda selected_ids: None)
    monkeypatch.setattr(
        selector_module,
        "global_config",
        SimpleNamespace(
            expression=SimpleNamespace(
                enable_precise_expression_selection=True,
            )
        ),
    )

    result = await selector.select_for_reply(
        session_id="session-1",
        chat_history=[],
        reply_message=None,
        reply_reason="",
        sub_agent_runner=fake_sub_agent,
    )

    assert len(sub_agent_calls) == 1
    assert result.selected_expression_ids == [2]
    assert result.selected_expressions == [candidates[1]]
    assert "主动接话" in result.expression_habits
    assert "轻松吐槽" not in result.expression_habits

