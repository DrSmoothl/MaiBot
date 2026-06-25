from src.config.config_upgrade_hooks import apply_config_upgrade_hooks


def test_split_chat_config_sections_upgrade_hook():
    config_data = {
        "chat": {
            "talk_value": 0.4,
            "private_talk_value": 0.8,
            "reply_trigger_mode": "frequency",
            "enable_talk_value_rules": True,
            "talk_value_rules": [
                {
                    "platform": "",
                    "item_id": "",
                    "rule_type": "group",
                    "time": "*",
                    "value": 0.5,
                }
            ],
            "enable_reply_quote": False,
            "group_chat_prompt": "group prompt",
            "private_chat_prompts": "private prompt",
            "chat_prompts": [],
            "max_context_size": 40,
        }
    }

    result = apply_config_upgrade_hooks(
        config_data,
        config_name="bot_config.toml",
        old_ver="8.14.18",
        new_ver="8.14.19",
    )

    chat_config = result.data["chat"]
    assert result.migrated is True
    assert chat_config["max_context_size"] == 40
    assert chat_config["reply_timing"]["talk_value"] == 0.4
    assert chat_config["reply_timing"]["private_talk_value"] == 0.8
    assert chat_config["reply_timing"]["reply_trigger_mode"] == "frequency"
    assert chat_config["reply_timing"]["enable_talk_value_rules"] is True
    assert chat_config["reply_timing"]["talk_value_rules"][0]["value"] == 0.5
    assert chat_config["reply_style"]["enable_reply_quote"] is False
    assert chat_config["reply_style"]["group_chat_prompt"] == "group prompt"
    assert chat_config["reply_style"]["private_chat_prompts"] == "private prompt"
    assert chat_config["reply_style"]["chat_prompts"] == []
    assert "talk_value" not in chat_config
    assert "group_chat_prompt" not in chat_config
