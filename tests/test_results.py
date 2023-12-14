from stmharry.results import Err, Ok, Result, returns_result


@returns_result(err=ValueError)
def division_raises_value_error(x: float, y: float) -> float:
    if y == 0:
        raise ValueError("Cannot divide by zero")
    return x / y


@returns_result
def division_raises_exception(x: float, y: float) -> float:
    return x / y


def division_returns_result(x: float, y: float) -> Result[float, ValueError]:
    if y == 0:
        return Err(ValueError("Cannot divide by zero"))
    return Ok(x / y)


def test_ok():
    assert division_raises_value_error(4, 2) == Ok(2)
    assert division_raises_value_error(4, 0) == Err(ValueError("Cannot divide by zero"))

    assert division_raises_exception(4, 2) == Ok(2)
    assert division_raises_exception(4, 0) == Err(ZeroDivisionError("division by zero"))

    assert division_returns_result(4, 2) == Ok(2)
    assert division_returns_result(4, 0) == Err(ValueError("Cannot divide by zero"))
