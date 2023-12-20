import pytest

from stmharry.rust.result import Err, Ok, Result, returns_result


def _division(x: float, y: float) -> float:
    return x / y


@returns_result(err=ValueError)
def division_raises_value_error(x: float, y: float) -> float:
    if y == 0:
        raise ValueError("Cannot divide by zero")
    return _division(x, y)


@returns_result
def division_raises_exception(x: float, y: float) -> float:
    return _division(x, y)


def division_returns_result(x: float, y: float) -> Result[float, ValueError]:
    if y == 0:
        return Err(ValueError("Cannot divide by zero"))
    return Ok(_division(x, y))


def test_ok():
    assert division_raises_value_error(4, 2) == Ok(2)
    assert division_raises_exception(4, 2) == Ok(2)
    assert division_returns_result(4, 2) == Ok(2)


def test_err():
    assert division_raises_value_error(4, 0) == Err(ValueError("Cannot divide by zero"))
    assert division_raises_exception(4, 0) == Err(ZeroDivisionError("division by zero"))
    assert division_returns_result(4, 0) == Err(ValueError("Cannot divide by zero"))


@pytest.mark.parametrize(
    "numerator, denominator, expected",
    [
        (4, 2, 2),
        (4, 0, ValueError("Cannot divide by zero")),
    ],
)
def test_full(numerator, denominator, expected):
    result: Result = division_returns_result(numerator, denominator)

    match result:
        case Ok(value):
            assert value == expected
            assert result.unwrap() == expected

        case Err(err):
            assert err.args == expected.args
            assert result.unwrap_or(0) == 0
            assert result.unwrap_or_else(lambda err: 0) == 0
