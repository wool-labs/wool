from enum import Enum
from typing import Final
from typing import final


@final
class UndefinedType(Enum):
    Undefined = "Undefined"


Undefined: Final = UndefinedType.Undefined
