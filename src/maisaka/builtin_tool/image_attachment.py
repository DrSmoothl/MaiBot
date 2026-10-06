"""回复图片附件的上下文解析与加载。"""

from typing import TYPE_CHECKING

from src.common.data_models.message_component_data_model import ImageComponent
from src.maisaka.context.message_id_alias import to_display_message_id
from src.maisaka.context.messages import SessionBackedMessage

if TYPE_CHECKING:
    from .context import BuiltinToolRuntimeContext


async def resolve_image_attachment(
    tool_ctx: "BuiltinToolRuntimeContext", source_id: str, image_index: int
) -> ImageComponent:
    """从 Maisaka 历史消息或工具返回媒体消息里读取指定图片组件。"""

    if not source_id:
        raise ValueError("图片附件需要提供 msg_id 或 media_index。")

    # 工具返回媒体保存在历史中，普通聊天图片也可以从当前源消息读取。
    context_message = next(
        (
            message
            for message in reversed(tool_ctx.runtime._chat_history)
            if isinstance(message, SessionBackedMessage) and message.message_id == source_id
        ),
        None,
    )
    source_message = context_message or tool_ctx.runtime.find_source_message_by_id(source_id)
    if source_message is None:
        raise ValueError(f"没有找到消息：msg_id={to_display_message_id(source_id)}")

    # 保留原始图片序号；不能过滤加载失败的图片，否则会发送另一张图。
    images = [component for component in source_message.raw_message.components if isinstance(component, ImageComponent)]
    if image_index < 0 or image_index >= len(images):
        raise ValueError(f"图片序号超出范围：index={image_index}，该消息共有 {len(images)} 张图片。")

    image = images[image_index]
    if not image.binary_data:
        await image.load_image_binary()
    if not image.binary_data:
        raise ValueError(f"目标图片数据不可读取：msg_id={to_display_message_id(source_id)}，index={image_index}")
    return ImageComponent(binary_hash=image.binary_hash, content=image.content, binary_data=image.binary_data)
