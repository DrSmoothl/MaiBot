"""测试黑话学习器的独立抽取行为。"""

from types import SimpleNamespace

import pytest

from src.learners.jargon_learner import JargonLearner


@pytest.mark.asyncio
async def test_jargon_learner_processes_all_extracted_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    """黑话学习器应独立处理 LLM 提取出的全部候选。"""

    import src.learners.jargon_learner as jargon_learner_module

    class FakePromptTemplate:
        def add_context(self, key: str, value: object) -> None:
            del key, value

    class FakePromptManager:
        def get_prompt(self, name: str) -> FakePromptTemplate:
            assert name == "learn_jargon"
            return FakePromptTemplate()

        async def render_prompt(self, prompt_template: FakePromptTemplate) -> str:
            del prompt_template
            return "prompt"

    class FakeLearnModel:
        async def generate_response_with_messages(self, builder, options):
            del builder, options
            return SimpleNamespace(response="response")

    class FakeJargonMiner:
        session_id = "session-a"
        session_name = "session-a"

        def get_cached_jargons(self):
            return []

    captured_jargon_entries = []

    async def fake_build_multi_learning_messages(self, pending_messages, prompt):
        del self, pending_messages, prompt
        return []

    async def fake_process_jargon_entries(self, jargon_entries, messages, jargon_miner):
        del self, messages, jargon_miner
        captured_jargon_entries.extend(jargon_entries)
        return True

    jargon_entries = [(f"黑话{i}", "1") for i in range(31)]
    monkeypatch.setattr(jargon_learner_module, "prompt_manager", FakePromptManager())
    monkeypatch.setattr(jargon_learner_module, "jargon_learn_model", FakeLearnModel())
    monkeypatch.setattr(
        jargon_learner_module,
        "global_config",
        SimpleNamespace(bot=SimpleNamespace(nickname="麦麦")),
    )
    monkeypatch.setattr(jargon_learner_module, "parse_jargon_response", lambda response: jargon_entries)
    monkeypatch.setattr(JargonLearner, "_build_multi_learning_messages", fake_build_multi_learning_messages)
    monkeypatch.setattr(JargonLearner, "_process_jargon_entries", fake_process_jargon_entries)
    monkeypatch.setattr(JargonLearner, "_log_learning_context_preview", lambda *args, **kwargs: None)

    learner = JargonLearner(session_id="session-a")
    wrote_result = await learner._run_learning_batch(
        [],
        learning_session_id="session-a",
        jargon_miner=FakeJargonMiner(),
    )

    assert wrote_result is True
    assert captured_jargon_entries == jargon_entries
