import json

from src.maisaka.display import prompt_cli_renderer as prompt_cli_renderer_module
from src.maisaka.display.prompt_cli_renderer import PromptCLIVisualizer
from src.maisaka.display.prompt_preview_logger import PromptPreviewLogger


PNG_1X1_BASE64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="


def test_prompt_html_keeps_internal_mixed_content_part_order() -> None:
    content = [
        '<message msg_id="m1" time="17:25:54" user="user1">\n先看这张：',
        ("png", PNG_1X1_BASE64),
        "\n再看这句",
    ]

    rendered_html = PromptCLIVisualizer._render_message_content_html(content)

    first_text_index = rendered_html.index("先看这张")
    image_index = rendered_html.index("image-preview")
    second_text_index = rendered_html.index("再看这句")
    assert first_text_index < image_index < second_text_index
    assert "data:image" not in rendered_html


def test_prompt_html_keeps_openai_mixed_content_part_order() -> None:
    content = [
        {"type": "text", "text": "第一段"},
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{PNG_1X1_BASE64}"},
        },
        {"type": "text", "text": "第二段"},
    ]

    rendered_html = PromptCLIVisualizer._render_message_content_html(content)
    dump_text = PromptCLIVisualizer._serialize_message_content_for_dump(content)

    first_text_index = rendered_html.index("第一段")
    image_index = rendered_html.index("image-preview")
    second_text_index = rendered_html.index("第二段")
    assert first_text_index < image_index < second_text_index
    assert "image_url" not in rendered_html
    assert "第一段\n[图片 image/png" in dump_text
    assert "第二段" in dump_text


def test_prompt_preview_metadata_is_written_to_text_and_html(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(PromptPreviewLogger, "_BASE_DIR", tmp_path)
    monkeypatch.setattr(
        PromptPreviewLogger,
        "_get_max_preview_groups_per_chat",
        classmethod(lambda cls: 256),
    )

    preview_access = PromptCLIVisualizer.build_prompt_preview_access(
        [{"role": "user", "content": "你好"}],
        category="planner",
        chat_id="qq_group_10000",
        request_kind="planner",
        selection_reason="测试选择原因",
        metadata={
            "model_name": "test-model",
            "duration_ms": 123.456,
        },
    )

    dump_text = preview_access.dump_path.read_text(encoding="utf-8")
    viewer_html = preview_access.viewer_path.read_text(encoding="utf-8")

    assert "[请求信息]" in dump_text
    assert "请求模型：test-model" in dump_text
    assert "推理耗时：123.46 ms" in dump_text
    assert "prompt-preview-metadata" in viewer_html
    assert "test-model" in viewer_html
    assert "123.46 ms" in viewer_html


def test_prompt_preview_json_uses_image_reference_by_default(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(PromptPreviewLogger, "_BASE_DIR", tmp_path / "prompt")
    monkeypatch.setattr(prompt_cli_renderer_module, "DATA_HTML_IMAGE_DIR", tmp_path / "html_imgs")
    monkeypatch.setattr(
        PromptPreviewLogger,
        "_get_max_preview_groups_per_chat",
        classmethod(lambda cls: 256),
    )
    monkeypatch.setattr(
        PromptCLIVisualizer,
        "_should_keep_prompt_preview_json_base64",
        staticmethod(lambda: False),
    )

    preview_access = PromptCLIVisualizer.build_prompt_preview_access(
        [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "第一段"},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{PNG_1X1_BASE64}"},
                    },
                    {"type": "text", "text": "第二段"},
                ],
            }
        ],
        category="planner",
        chat_id="qq_group_10000",
        request_kind="planner",
        selection_reason="测试选择原因",
    )

    payload = json.loads(preview_access.structured_path.read_text(encoding="utf-8"))
    payload_text = json.dumps(payload, ensure_ascii=False)
    image_part = payload["messages"][0]["content"][1]

    assert payload["schema_version"] == 2
    assert PNG_1X1_BASE64 not in payload_text
    assert "data:image" not in payload_text
    assert image_part["image_reference"]["base64_omitted"] is True
    assert image_part["image_reference"]["image_available"] is True
    assert "image_path" in image_part["image_reference"]
    assert "image_uri" in image_part["image_reference"]
    assert image_part["image_url"]["url"] == image_part["image_reference"]["image_uri"]


def test_prompt_preview_json_can_keep_image_base64_when_enabled(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(PromptPreviewLogger, "_BASE_DIR", tmp_path / "prompt")
    monkeypatch.setattr(prompt_cli_renderer_module, "DATA_HTML_IMAGE_DIR", tmp_path / "html_imgs")
    monkeypatch.setattr(
        PromptPreviewLogger,
        "_get_max_preview_groups_per_chat",
        classmethod(lambda cls: 256),
    )
    monkeypatch.setattr(
        PromptCLIVisualizer,
        "_should_keep_prompt_preview_json_base64",
        staticmethod(lambda: True),
    )

    preview_access = PromptCLIVisualizer.build_prompt_preview_access(
        [{"role": "user", "content": [("png", PNG_1X1_BASE64)]}],
        category="planner",
        chat_id="qq_group_10000",
        request_kind="planner",
        selection_reason="测试选择原因",
    )

    payload_text = preview_access.structured_path.read_text(encoding="utf-8")

    assert PNG_1X1_BASE64 in payload_text


def test_prompt_preview_logger_writes_navigation_into_html_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(PromptPreviewLogger, "_BASE_DIR", tmp_path)
    monkeypatch.setattr(
        PromptPreviewLogger,
        "_get_max_preview_groups_per_chat",
        classmethod(lambda cls: 256),
    )
    stems = iter(["1700000000000", "1700000000001", "1700000000002"])
    monkeypatch.setattr(
        PromptPreviewLogger,
        "_build_file_stem",
        classmethod(lambda cls, chat_dir: next(stems)),
    )

    saved_paths = []
    for stem in ("first", "second", "third"):
        saved = PromptPreviewLogger.save_preview_files(
            "qq_group_10000",
            "planner",
            {
                ".html": f"<!DOCTYPE html><html><body><main>{stem}</main></body></html>",
            },
        )
        saved_paths.append(saved[".html"])

    first_html = saved_paths[0].read_text(encoding="utf-8")
    middle_html = saved_paths[1].read_text(encoding="utf-8")
    last_html = saved_paths[2].read_text(encoding="utf-8")

    assert "maibot-reasoning-html-navigation:start" in middle_html
    assert "上一份" in middle_html
    assert "下一份" in middle_html
    assert "href='1700000000000.html'" in middle_html
    assert "href='1700000000002.html'" in middle_html
    assert "maibot-html-nav-button-disabled" in first_html
    assert "href='1700000000001.html'" in first_html
    assert "href='1700000000001.html'" in last_html
