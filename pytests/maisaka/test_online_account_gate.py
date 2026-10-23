from types import SimpleNamespace

import pytest

from src.maisaka.turn_trigger.gates import DynamicReplyTurnGate
from src.platform_io import DriverDescriptor, DriverKind, PlatformIOManager
from src.services.bot_account_service import get_online_bot_accounts


def test_private_message_gate_tracks_gateway_disconnect_and_reconnect(monkeypatch) -> None:
    manager = PlatformIOManager()
    monkeypatch.setattr("src.platform_io.get_platform_io_manager", lambda: manager)
    gate = DynamicReplyTurnGate(SimpleNamespace(log_prefix="[测试私聊]", chat_stream=SimpleNamespace(is_group_session=False)))
    message = SimpleNamespace(platform="qq", message_info=SimpleNamespace(user_info=SimpleNamespace(user_id="old-bot")))

    def register(driver_id: str, account_id: str, platform: str = "qq") -> None:
        manager.driver_registry.register(SimpleNamespace(
            driver_id=driver_id,
            descriptor=DriverDescriptor(
                driver_id=driver_id, kind=DriverKind.PLUGIN, platform=platform, account_id=account_id,
            ),
        ))

    register("current", "current-bot")
    register("other-platform", "old-bot", "telegram")
    assert gate.evaluate(pending_messages=[message], frequency=1.0).should_trigger

    register("old", "old-bot")
    register("duplicate", "old-bot")
    assert not gate.evaluate(pending_messages=[message], frequency=1.0).should_trigger
    manager.driver_registry.unregister("old")
    assert not gate.evaluate(pending_messages=[message], frequency=1.0).should_trigger
    manager.driver_registry.unregister("duplicate")
    assert gate.evaluate(pending_messages=[message], frequency=1.0).should_trigger

    register("reconnected", "old-bot")
    assert not gate.evaluate(pending_messages=[message], frequency=1.0).should_trigger


@pytest.mark.parametrize("platform, expected", [("webui", {"self"}), ("qq", set())])
def test_no_gateway_does_not_restore_historical_accounts(monkeypatch, platform, expected) -> None:
    monkeypatch.setattr("src.platform_io.get_platform_io_manager", PlatformIOManager)
    monkeypatch.setattr(
        "src.services.bot_account_service.BotAccountService.get_bot_accounts", lambda *_: {"historical-bot"},
    )
    assert get_online_bot_accounts(platform) == expected
