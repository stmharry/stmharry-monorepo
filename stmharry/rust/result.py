import dataclasses
import functools
from collections.abc import Callable
from typing import Any, Generic, NoReturn, TypeVar, overload

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


@dataclasses.dataclass(frozen=True)
class Ok(Generic[T, E]):
    value: T

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Ok):
            return self.value == other.value
        return False

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"

    def unwrap(self) -> T:
        return self.value

    def unwrap_or(self, default: T) -> T:
        return self.unwrap()

    def unwrap_or_else(self, op: Callable[[E], T]) -> T:
        return self.unwrap()


@dataclasses.dataclass(frozen=True)
class Err(Generic[T, E]):
    err: E

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Err):
            return self.err.args == other.err.args
        return False

    def __repr__(self) -> str:
        return f"Err({self.err!r})"

    def unwrap(self) -> NoReturn:
        raise self.err

    def unwrap_or(self, default: T) -> T:
        return default

    def unwrap_or_else(self, op: Callable[[E], T]) -> T:
        return op(self.err)


Result = Ok[T, E] | Err[T, E]


@overload
def returns_result(
    *,
    err: type[E],
) -> Callable[[Callable[..., T]], Callable[..., Result[T, E]]]:
    ...


@overload
def returns_result(
    *,
    err: tuple[type[E], ...],
) -> Callable[[Callable[..., T]], Callable[..., Result[T, Exception]]]:
    ...


@overload
def returns_result(
    fn: Callable[..., T],
) -> Callable[..., Result[T, Exception]]:
    ...


def returns_result(
    fn: Callable[..., T] | None = None,
    *,
    err: type[Exception] | type[E] | tuple[type[E], ...] | None = None,
) -> (
    Callable[[Callable[..., T]], Callable[..., Result[T, E]]]
    | Callable[[Callable[..., T]], Callable[..., Result[T, E | Exception]]]
    | Callable[..., Result[T, Exception]]
):
    if (err is None) or isinstance(err, tuple):
        err = Exception

    def _returns_result(
        fn: Callable[..., T]
    ) -> Callable[..., Result[T, E | Exception]]:
        @functools.wraps(fn)
        def _fn(*args: Any, **kwargs: Any) -> Result[T, E | Exception]:
            try:
                return Ok(fn(*args, **kwargs))

            except err as e:
                return Err(e)

        return _fn

    if fn is None:
        return _returns_result
    else:
        return _returns_result(fn)
