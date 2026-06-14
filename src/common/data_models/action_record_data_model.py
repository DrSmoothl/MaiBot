from datetime import datetime
from typing import Dict, Optional

import json

from src.common.database.database_model import ToolRecord

from . import BaseDatabaseDataModel


class MaiActionRecord(BaseDatabaseDataModel[ToolRecord]):
    """``action_records`` 的兼容数据模型。

    历史动作记录已统一并入 ``tool_records``，该类仅保留旧命名接口，
    底层读写对象统一映射为 ``ToolRecord``。
    """

    def __init__(
        self,
        action_id: str,
        timestamp: datetime,
        session_id: str,
        action_name: str,
        action_reasoning: Optional[str] = None,
        action_data: Optional[Dict] = None,
    ):
        self.action_id = action_id
        self.timestamp = timestamp
        self.session_id = session_id
        self.action_name = action_name
        self.action_reasoning = action_reasoning
        self.action_data = action_data or {}

    @classmethod
    def from_db_instance(cls, db_record: ToolRecord):
        """从数据库实例创建兼容数据模型对象。"""

        return cls(
            action_id=db_record.tool_id,
            timestamp=db_record.timestamp,
            session_id=db_record.session_id,
            action_name=db_record.tool_name,
            action_reasoning=db_record.tool_reasoning,
            action_data=json.loads(db_record.tool_data) if db_record.tool_data else None,
        )

    def to_db_instance(self):
        """将兼容数据模型对象转换为 ``ToolRecord``。"""

        return ToolRecord(
            tool_id=self.action_id,
            timestamp=self.timestamp,
            session_id=self.session_id,
            tool_name=self.action_name,
            tool_reasoning=self.action_reasoning,
            tool_data=json.dumps(self.action_data) if self.action_data else None,
        )
