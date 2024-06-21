from typing import Union


def lte(x: Union[float, str, int], y: Union[float, str, int]) -> bool:
    if isinstance(x, str) and isinstance(y, str):
        return x <= y
    else:
        assert isinstance(x, (float, int)) and isinstance(
            y, (float, int)
        ), f"Error! Unexpected type: {x=} {y=}"
        return x <= y


def lt(x: Union[float, str, int], y: Union[float, str, int]) -> bool:
    if isinstance(x, str) and isinstance(y, str):
        return x < y
    else:
        assert isinstance(x, (float, int)) and isinstance(
            y, (float, int)
        ), f"Error! Unexpected type: {x=} {y=}"
        return x < y


def gte(x: Union[float, str, int], y: Union[float, str, int]) -> bool:
    if isinstance(x, str) and isinstance(y, str):
        return x >= y
    else:
        assert isinstance(x, (float, int)) and isinstance(
            y, (float, int)
        ), f"Error! Unexpected type: {x=} {y=}"
        return x >= y


def gt(x: Union[float, str, int], y: Union[float, str, int]) -> bool:
    if isinstance(x, str) and isinstance(y, str):
        return x > y
    else:
        assert isinstance(x, (float, int)) and isinstance(
            y, (float, int)
        ), f"Error! Unexpected type: {x=} {y=}"
        return x > y


def eq(x: Union[float, str, int], y: Union[float, str, int]) -> bool:
    if isinstance(x, str) and isinstance(y, str):
        return x == y
    else:
        assert isinstance(x, (float, int)) and isinstance(
            y, (float, int)
        ), f"Error! Unexpected type: {x=} {y=}"
        return x == y
