from typing import Any

from src.A_memorix.core.utils import profile_policy


def _config_getter(values: dict[str, Any]):
    return lambda key, default=None: values.get(key, default)


def test_should_auto_enqueue_episode_respects_episode_switches() -> None:
    assert (
        profile_policy.should_auto_enqueue_episode(
            _config_getter({"episode.enabled": False}),
            source_type="chat_summary",
        )
        is False
    )
    assert (
        profile_policy.should_auto_enqueue_episode(
            _config_getter({"episode.generation_enabled": False}),
            source_type="chat_summary",
        )
        is False
    )


def test_should_auto_enqueue_episode_respects_disabled_source_types() -> None:
    config_getter = _config_getter({"episode.disabled_source_types": ["person_fact", "manual"]})

    assert profile_policy.should_auto_enqueue_episode(config_getter, source_type="manual") is False
    assert profile_policy.should_auto_enqueue_episode(config_getter, source_type="knowledge_pack") is False
    assert profile_policy.should_auto_enqueue_episode(config_getter, source_type="chat_summary") is True
