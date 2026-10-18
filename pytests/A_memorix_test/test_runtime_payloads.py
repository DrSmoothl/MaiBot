from datetime import datetime

from src.A_memorix.core.utils.runtime_payloads import (
    coerce_datetime,
    optional_float,
    optional_int,
    safe_json_loads,
)






def test_runtime_payload_json_datetime_and_optional_number_helpers() -> None:
    raw_dict = {"name": "Alice"}
    assert safe_json_loads(raw_dict) is raw_dict
    assert safe_json_loads("{name: 'Alice', score: 1}") == {"name": "Alice", "score": 1}
    assert safe_json_loads("[1, 2, 3]") == {}

    value = datetime(2026, 1, 2, 3, 4, 5)
    assert coerce_datetime(value) is value
    assert coerce_datetime("2026-01-02T03:04:05") == value
    assert coerce_datetime("") is None

    assert optional_float("1.5") == 1.5
    assert optional_float("bad") is None
    assert optional_int("7") == 7
    assert optional_int("bad") is None
