"""测试表达方式学习器的数据库读取行为。"""

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Generator

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select
import pytest

from src.common.database.database_model import Expression, ModifiedBy
from src.learners.expression_learner import ExpressionLearner


@pytest.fixture(name="expression_learner_engine")
def expression_learner_engine_fixture() -> Generator:
    """创建用于表达方式学习器测试的内存数据库引擎。

    Yields:
        Generator: 供测试使用的 SQLite 内存引擎。
    """
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    yield engine


def test_find_similar_expression_uses_read_only_session_and_history_content(
    monkeypatch: pytest.MonkeyPatch,
    expression_learner_engine,
) -> None:
    """查找相似表达方式时，应能在离开会话后安全使用结果，并比较历史情景内容。"""
    import src.learners.expression_learner as expression_learner_module

    with Session(expression_learner_engine) as session:
        session.add(
            Expression(
                situation="发送汗滴表情",
                style="发送💦表情符号",
                content_list='["表达情绪高涨或生理反应"]',
                count=1,
                session_id="session-a",
                checked=False,
            )
        )
        session.commit()

    @contextmanager
    def fake_get_db_session(auto_commit: bool = True) -> Generator[Session, None, None]:
        """构造带自动提交语义的测试会话工厂。

        Args:
            auto_commit: 退出上下文时是否自动提交。

        Yields:
            Generator[Session, None, None]: SQLModel 会话对象。
        """
        session = Session(expression_learner_engine)
        try:
            yield session
            if auto_commit:
                session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(expression_learner_module, "get_db_session", fake_get_db_session)

    learner = ExpressionLearner(session_id="session-a")
    result = learner._find_similar_expression("表达情绪高涨或生理反应", session_id="session-a")

    assert result is not None
    expression, similarity = result
    assert expression.item_id is not None
    assert expression.style == "发送💦表情符号"
    assert similarity == pytest.approx(1.0)


def test_get_session_display_name_uses_chat_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    """表达学习日志应优先展示聊天流名称。"""

    from src.chat.message_receive.chat_manager import chat_manager

    learner = ExpressionLearner(session_id="session-a")

    def fake_get_session_name(session_id: str) -> str | None:
        if session_id == "session-a":
            return "测试群"
        return None

    monkeypatch.setattr(chat_manager, "get_session_name", fake_get_session_name)
    monkeypatch.setattr(chat_manager, "get_existing_session_by_session_id", lambda session_id: None)

    assert learner._get_session_display_name("session-a") == "测试群"
    assert learner._get_session_display_name("unknown-session") == "unknown-session"


@pytest.mark.asyncio
async def test_ai_self_reflect_expression_stays_unchecked(
    monkeypatch: pytest.MonkeyPatch,
    expression_learner_engine,
) -> None:
    """AI 优化通过的表达方式仍应保持待人工审核状态。"""

    import src.learners.expression_learner as expression_learner_module

    @contextmanager
    def fake_get_db_session(auto_commit: bool = True) -> Generator[Session, None, None]:
        """构造带自动提交语义的测试会话工厂。"""

        session = Session(expression_learner_engine)
        try:
            yield session
            if auto_commit:
                session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    class FakePromptTemplate:
        def add_context(self, key: str, value: object) -> None:
            del key, value

    class FakePromptManager:
        def get_prompt(self, name: str) -> FakePromptTemplate:
            del name
            return FakePromptTemplate()

        async def render_prompt(self, prompt_template: FakePromptTemplate) -> str:
            del prompt_template
            return "prompt"

    class FakeLearnModel:
        async def generate_response_with_messages(self, builder, options):
            del builder, options
            return SimpleNamespace(response="response")

    class FakeRuntimeManager:
        async def invoke_hook(self, *args, **kwargs):
            del args, kwargs
            return SimpleNamespace(aborted=False, kwargs={})

    async def fake_build_multi_learning_messages(self, pending_messages, prompt):
        del self, pending_messages, prompt
        return []

    async def fake_check_expression_before_upsert(
        self,
        situation: str,
        style: str,
        *,
        session_id: str | None = None,
    ) -> bool:
        del self, situation, style, session_id
        return True

    monkeypatch.setattr(expression_learner_module, "get_db_session", fake_get_db_session)
    monkeypatch.setattr(expression_learner_module, "prompt_manager", FakePromptManager())
    monkeypatch.setattr(expression_learner_module, "express_learn_model", FakeLearnModel())
    monkeypatch.setattr(
        expression_learner_module,
        "global_config",
        SimpleNamespace(
            bot=SimpleNamespace(nickname="麦麦", alias_names=[]),
            expression=SimpleNamespace(expression_self_reflect=True),
        ),
    )
    monkeypatch.setattr(expression_learner_module, "parse_expression_response", lambda response: ([("情景", "风格", "1")], []))
    monkeypatch.setattr(ExpressionLearner, "_get_runtime_manager", staticmethod(lambda: FakeRuntimeManager()))
    monkeypatch.setattr(ExpressionLearner, "_build_multi_learning_messages", fake_build_multi_learning_messages)
    monkeypatch.setattr(ExpressionLearner, "_check_expression_before_upsert", fake_check_expression_before_upsert)
    monkeypatch.setattr(ExpressionLearner, "_filter_expressions", lambda self, expressions, messages: [("情景", "风格")])
    monkeypatch.setattr(ExpressionLearner, "_get_session_display_name", staticmethod(lambda session_id: session_id))
    monkeypatch.setattr(ExpressionLearner, "_log_learning_context_preview", lambda *args, **kwargs: None)

    learner = ExpressionLearner(session_id="session-a")
    wrote_expression = await learner._run_learning_batch([], learning_session_id="session-a")

    assert wrote_expression is True
    with Session(expression_learner_engine) as session:
        expression = session.exec(select(Expression)).one()

    assert expression.checked is False
    assert expression.modified_by == ModifiedBy.AI

