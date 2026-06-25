from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from typing import Iterator

import json
import pytest

from sqlmodel import SQLModel, Session, create_engine

from src.common.data_models.jargon_data_model import MaiJargon
from src.common.database.database_model import Jargon, Messages
from src.common.database.migrations.models import MigrationExecutionContext
from src.common.database.migrations.schema import SQLiteSchemaInspector
from src.common.database.migrations.v33_to_v34 import migrate_v33_to_v34
from src.learners.jargon_miner import JargonMiner


def test_v33_to_v34_adds_jargon_evidence_messages_column_and_drops_raw_content() -> None:
    engine = create_engine("sqlite://")

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE jargons (
                id INTEGER NOT NULL,
                content VARCHAR(255) NOT NULL,
                raw_content TEXT,
                meaning TEXT NOT NULL,
                session_id_dict TEXT NOT NULL,
                count INTEGER NOT NULL,
                is_jargon BOOLEAN,
                is_complete BOOLEAN NOT NULL,
                is_global BOOLEAN NOT NULL,
                last_inference_count INTEGER NOT NULL,
                created_by VARCHAR(6) NOT NULL,
                created_timestamp DATETIME,
                updated_timestamp DATETIME,
                PRIMARY KEY (id)
            )
            """
        )

        migrate_v33_to_v34(
            MigrationExecutionContext(
                connection=connection,
                current_version=33,
                target_version=34,
                step_index=1,
                step_name="v33_to_v34",
                total_steps=1,
            )
        )

        table_schema = SQLiteSchemaInspector().get_table_schema(connection, "jargons")

    assert table_schema.has_column("evidence_messages")
    assert not table_schema.has_column("raw_content")


def test_jargon_evidence_message_groups_merge_in_order() -> None:
    current_groups = [
        [
            {"platform": "qq", "message_id": "1"},
            {"platform": "qq", "message_id": "2"},
        ]
    ]
    new_groups = [
        [
            {"platform": "qq", "message_id": "1"},
            {"platform": "qq", "message_id": "2"},
        ],
        [
            {"platform": "qq", "message_id": "2"},
            {"platform": "qq", "message_id": "3"},
        ],
    ]

    merged_groups = JargonMiner._merge_evidence_message_groups(current_groups, new_groups)

    assert merged_groups == [
        [
            {"platform": "qq", "message_id": "1"},
            {"platform": "qq", "message_id": "2"},
        ],
        [
            {"platform": "qq", "message_id": "2"},
            {"platform": "qq", "message_id": "3"},
        ],
    ]


def test_jargon_evidence_context_loader_removes_missing_message_groups(monkeypatch) -> None:
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine, tables=[Messages.__table__])

    with Session(engine) as session:
        session.add(
            Messages(
                message_id="exists",
                timestamp=datetime.now(),
                platform="qq",
                user_id="user-a",
                user_nickname="用户A",
                session_id="session-a",
                raw_content=b"",
                processed_plain_text="保留的上下文",
            )
        )
        session.commit()

    @contextmanager
    def session_factory(*args, **kwargs) -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    monkeypatch.setattr("src.learners.jargon_miner.get_db_session", session_factory)

    evidence_messages = json.dumps(
        [
            [{"platform": "qq", "message_id": "exists"}],
            [{"platform": "qq", "message_id": "missing"}],
        ],
        ensure_ascii=False,
    )

    contexts, cleaned_evidence_messages, changed = JargonMiner("session-a", "测试会话")._load_evidence_contexts(
        MaiJargon(content="测试黑话", meaning="", evidence_messages=evidence_messages)
    )

    assert contexts == ["[1] 保留的上下文"]
    assert json.loads(cleaned_evidence_messages or "[]") == [[{"platform": "qq", "message_id": "exists"}]]
    assert changed is True


def test_jargon_evidence_context_loader_filters_emoji_only_messages(monkeypatch) -> None:
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine, tables=[Messages.__table__])

    with Session(engine) as session:
        session.add_all(
            [
                Messages(
                    message_id="emoji",
                    timestamp=datetime.now(),
                    platform="qq",
                    user_id="user-a",
                    user_nickname="用户A",
                    session_id="session-a",
                    raw_content=b"",
                    is_emoji=True,
                    processed_plain_text="[表情包: 疑惑]",
                ),
                Messages(
                    message_id="text",
                    timestamp=datetime.now(),
                    platform="qq",
                    user_id="user-b",
                    user_nickname="用户B",
                    session_id="session-a",
                    raw_content=b"",
                    processed_plain_text="这条是真正的上下文",
                ),
                Messages(
                    message_id="legacy-emoji",
                    timestamp=datetime.now(),
                    platform="qq",
                    user_id="user-c",
                    user_nickname="用户C",
                    session_id="session-a",
                    raw_content=b"",
                    processed_plain_text="[表情包1: 开心]",
                ),
            ]
        )
        session.commit()

    @contextmanager
    def session_factory(*args, **kwargs) -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    monkeypatch.setattr("src.learners.jargon_miner.get_db_session", session_factory)

    evidence_messages = json.dumps(
        [
            [
                {"platform": "qq", "message_id": "emoji"},
                {"platform": "qq", "message_id": "text"},
            ],
            [{"platform": "qq", "message_id": "legacy-emoji"}],
        ],
        ensure_ascii=False,
    )

    contexts, cleaned_evidence_messages, changed = JargonMiner("session-a", "测试会话")._load_evidence_contexts(
        MaiJargon(content="测试黑话", meaning="", evidence_messages=evidence_messages)
    )

    assert contexts == ["[1] 这条是真正的上下文"]
    assert json.loads(cleaned_evidence_messages or "[]") == [[{"platform": "qq", "message_id": "text"}]]
    assert changed is True
    assert JargonMiner._format_evidence_context_segments(["[1] 第一段", "[1] 第二段"]) == (
        "【对话片段 1】\n[1] 第一段\n\n【对话片段 2】\n[1] 第二段"
    )


@pytest.mark.asyncio
async def test_jargon_inference_clears_evidence_messages_after_using_them(monkeypatch) -> None:
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine, tables=[Messages.__table__, Jargon.__table__])

    evidence_messages = json.dumps(
        [[{"platform": "qq", "message_id": "exists"}]],
        ensure_ascii=False,
    )
    with Session(engine) as session:
        session.add(
            Messages(
                message_id="exists",
                timestamp=datetime.now(),
                platform="qq",
                user_id="user-a",
                user_nickname="用户A",
                session_id="session-a",
                raw_content=b"",
                processed_plain_text="本次用于推断的上下文",
            )
        )
        session.add(
            Jargon(
                id=100,
                content="测试黑话",
                evidence_messages=evidence_messages,
                meaning="旧含义",
                session_id_dict=json.dumps({"session-a": 4}),
                count=4,
                is_jargon=True,
                is_complete=False,
                is_global=False,
                last_inference_count=0,
            )
        )
        session.commit()

    @contextmanager
    def session_factory(*args, **kwargs) -> Iterator[Session]:
        with Session(engine) as session:
            yield session
            session.commit()

    async def no_response(*args, **kwargs):
        return SimpleNamespace(response=None)

    class FakePrompt:
        def add_context(self, *args, **kwargs) -> None:
            return None

    class FakePromptManager:
        def get_prompt(self, prompt_name: str) -> FakePrompt:
            return FakePrompt()

        async def render_prompt(self, prompt: FakePrompt) -> str:
            return "prompt"

    monkeypatch.setattr("src.learners.jargon_miner.get_db_session", session_factory)
    monkeypatch.setattr("src.learners.jargon_miner.llm_inference.generate_response", no_response)
    monkeypatch.setattr("src.learners.jargon_miner.prompt_manager", FakePromptManager())

    await JargonMiner("session-a", "测试会话").infer_meaning(
        MaiJargon(
            item_id=100,
            content="测试黑话",
            meaning="旧含义",
            evidence_messages=evidence_messages,
            session_id_list={"session-a": 4},
            count=4,
        )
    )

    with Session(engine) as session:
        db_jargon = session.get(Jargon, 100)

    assert db_jargon is not None
    assert db_jargon.evidence_messages is None


@pytest.mark.asyncio
async def test_jargon_inference_logs_three_replayable_update_prompts(monkeypatch) -> None:
    """黑话含义推断的三次 LLM 调用应记录到 jargon_learning_update。"""

    class FakePrompt:
        def __init__(self, name: str) -> None:
            self.name = name
            self.contexts = {}

        def add_context(self, key, value) -> None:
            self.contexts[key] = value

    class FakePromptManager:
        def get_prompt(self, prompt_name: str) -> FakePrompt:
            return FakePrompt(prompt_name)

        async def render_prompt(self, prompt: FakePrompt) -> str:
            return f"{prompt.name}:{json.dumps(prompt.contexts, ensure_ascii=False, sort_keys=True)}"

    llm_responses = [
        SimpleNamespace(response=json.dumps({"meaning": "上下文含义", "no_info": False}), model_name="model-a"),
        SimpleNamespace(response=json.dumps({"meaning": "字面含义"}), model_name="model-a"),
        SimpleNamespace(response=json.dumps({"is_similar": False, "reason": "不同"}), model_name="model-a"),
    ]
    captured_previews = []
    evidence_messages = json.dumps(
        [[{"platform": "qq", "message_id": "exists"}]],
        ensure_ascii=False,
    )
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine, tables=[Messages.__table__])
    with Session(engine) as session:
        session.add(
            Messages(
                message_id="exists",
                timestamp=datetime.now(),
                platform="qq",
                user_id="user-a",
                user_nickname="用户A",
                session_id="session-a",
                raw_content=b"",
                processed_plain_text="本次用于推断的上下文",
            )
        )
        session.commit()

    async def fake_generate_response(*args, **kwargs):
        del args, kwargs
        return llm_responses.pop(0)

    async def fake_invoke_hook(*args, **kwargs):
        return SimpleNamespace(aborted=False, kwargs=kwargs)

    def fake_build_prompt_preview_access(messages, **kwargs):
        captured_previews.append((messages, kwargs))
        return SimpleNamespace(
            preview_web_uri="preview",
            reasoning_web_uri="reasoning",
            record_path="record.json",
        )

    miner = JargonMiner("session-a", "测试会话")

    @contextmanager
    def session_factory(*args, **kwargs) -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    monkeypatch.setattr("src.learners.jargon_miner.get_db_session", session_factory)
    monkeypatch.setattr("src.learners.jargon_miner.prompt_manager", FakePromptManager())
    monkeypatch.setattr("src.learners.jargon_miner.llm_inference.generate_response", fake_generate_response)
    monkeypatch.setattr(
        "src.learners.jargon_miner.PromptCLIVisualizer.build_prompt_preview_access",
        fake_build_prompt_preview_access,
    )
    monkeypatch.setattr(miner, "_modify_jargon_entry", lambda jargon_obj: None)
    monkeypatch.setattr(
        miner,
        "_get_runtime_manager",
        lambda: SimpleNamespace(invoke_hook=fake_invoke_hook),
    )

    await miner.infer_meaning(
        MaiJargon(
            content="测试黑话",
            meaning="旧含义",
            evidence_messages=evidence_messages,
            session_id_list={"session-a": 4},
            count=4,
        )
    )

    assert len(captured_previews) == 3
    assert [preview[1]["category"] for preview in captured_previews] == ["jargon_learning_update"] * 3
    assert [preview[1]["request_kind"] for preview in captured_previews] == ["jargon_learning_update"] * 3
    assert [preview[1]["output_title"] for preview in captured_previews] == [
        "黑话含义推断输出 - with_context",
        "黑话含义推断输出 - content_only",
        "黑话含义推断输出 - compare",
    ]


@pytest.mark.asyncio
async def test_jargon_inference_keeps_meaning_when_classified_as_not_jargon(monkeypatch) -> None:
    """重新推断为无黑话时，不应清空已有含义。"""

    class FakePrompt:
        def add_context(self, *args, **kwargs) -> None:
            return None

    class FakePromptManager:
        def get_prompt(self, prompt_name: str) -> FakePrompt:
            return FakePrompt()

        async def render_prompt(self, prompt: FakePrompt) -> str:
            return "prompt"

    llm_responses = [
        SimpleNamespace(response=json.dumps({"meaning": "本次上下文含义", "no_info": False}), model_name="model-a"),
        SimpleNamespace(response=json.dumps({"meaning": "本次字面含义"}), model_name="model-a"),
        SimpleNamespace(response=json.dumps({"is_similar": True, "reason": "相似"}), model_name="model-a"),
    ]
    captured_jargon = None

    async def fake_generate_response(*args, **kwargs):
        del args, kwargs
        return llm_responses.pop(0)

    async def fake_invoke_hook(*args, **kwargs):
        return SimpleNamespace(aborted=False, kwargs=kwargs)

    def fake_modify_jargon_entry(jargon_obj):
        nonlocal captured_jargon
        captured_jargon = jargon_obj

    miner = JargonMiner("session-a", "测试会话")
    monkeypatch.setattr(miner, "_load_evidence_contexts", lambda jargon_obj: (["[1] 上下文"], None, False))
    monkeypatch.setattr("src.learners.jargon_miner.prompt_manager", FakePromptManager())
    monkeypatch.setattr("src.learners.jargon_miner.llm_inference.generate_response", fake_generate_response)
    monkeypatch.setattr(
        "src.learners.jargon_miner.PromptCLIVisualizer.build_prompt_preview_access",
        lambda *args, **kwargs: SimpleNamespace(
            preview_web_uri="preview",
            reasoning_web_uri="reasoning",
            record_path="record.json",
        ),
    )
    monkeypatch.setattr(miner, "_modify_jargon_entry", fake_modify_jargon_entry)
    monkeypatch.setattr(miner, "_get_runtime_manager", lambda: SimpleNamespace(invoke_hook=fake_invoke_hook))

    await miner.infer_meaning(
        MaiJargon(
            content="测试黑话",
            meaning="旧含义",
            evidence_messages="[]",
            session_id_list={"session-a": 8},
            count=8,
        )
    )

    assert captured_jargon is not None
    assert captured_jargon.is_jargon is False
    assert captured_jargon.meaning == "旧含义"
