import json
from dataclasses import dataclass
from typing import Literal, assert_never

type VarName = str

# Encoding of the trustworthiness checker internal
# types
#
# Excludes Deferred and NoVal since these are not explicitly sent
type TCType = Literal["Int", "Float", "Str", "Bool", "List", "Map", "Unit"]

type TCData = int | float | str | bool | list[TCData] | dict[str, TCData]


@dataclass(frozen=True)
class TypedTCData:
    typ: TCType
    data: TCData

    def __post_init__(self) -> None:
        validate_tc_data(self.typ, self.data)

    def to_json(self) -> str:
        return tc_data_to_json(self.data)


def validate_tc_data(typ: TCType, data: TCData) -> None:
    match typ:
        case "Int":
            assert isinstance(data, int)
        case "Float":
            assert isinstance(data, float)
        case "Str":
            assert isinstance(data, str)
        case "Bool":
            assert isinstance(data, bool)
        case "List":
            assert isinstance(data, list)
        case "Map":
            assert isinstance(data, dict)
        case "Unit":
            assert data == ()

        case _:
            assert_never(typ)


def tc_data_to_json(data: TCData) -> str:
    return json.dumps(data)
