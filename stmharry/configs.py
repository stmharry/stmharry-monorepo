import importlib
import types
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Protocol,
    Type,
    TypeGuard,
    TypeVar,
    get_args,
)

import yaml
from absl import logging
from pydantic import BaseModel, Extra, Field, parse_obj_as

T_GENERIC = TypeVar("T_GENERIC")
T_CONFIG = TypeVar("T_CONFIG", bound="BaseConfig")


class GenericAlias(Protocol):
    __origin__: Type[object]


class IndirectGenericSubclass(Protocol):
    __orig_bases__: tuple[GenericAlias]


def is_indirect_generic_subclass(
    obj: object,
) -> TypeGuard[IndirectGenericSubclass]:
    bases = getattr(obj, "__orig_bases__")
    return bases is not None and isinstance(bases, tuple)


def _import_module_native(module_name: str) -> ModuleType | None:
    try:
        return importlib.import_module(module_name)

    except ModuleNotFoundError:
        return None


def import_module(module_name: str) -> ModuleType:
    module: ModuleType | None = None

    # native import
    module = _import_module_native(module_name)
    if module is not None:
        return module

    # indirect import
    name_parts: list[str] = module_name.split(".")
    if len(name_parts) == 0:
        raise ValueError(f"Invalid module name {module_name}!")

    # this is to facilitate backwards-compatible importing patterns
    module = _import_module_native(name_parts[0])
    for name_part in name_parts[1:]:
        module = getattr(module, name_part, None)

    if module is not None:
        return module

    raise ModuleNotFoundError(f"Module {module_name} not found!")


class ClassConfig(object):
    @classmethod
    def __get_validators__(cls) -> Generator[Callable, None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: Any) -> Type:
        if not isinstance(v, str):
            raise ValueError(f"Expected string, got {v}!")

        module_name: str
        obj_name: str
        (module_name, _, obj_name) = v.rpartition(".")

        if module_name == "":
            module_name = "__main__"

        module: ModuleType = import_module(module_name)
        obj_cls = getattr(module, obj_name, None)

        if obj_cls is None:
            raise ValueError(f"Referenced class {module_name} not found!")

        return obj_cls


class ObjectConfig(Generic[T_GENERIC], BaseModel):
    obj_cls: ClassConfig = Field(alias="__class__", repr=False)

    def instantiate(self, **kwargs: Any) -> T_GENERIC:
        obj_dict: dict = self.dict()

        assert isinstance(self.obj_cls, type)

        logging.info(
            f"Creating object '{self.obj_cls.__name__}' from config {obj_dict}."
        )

        for field_name in obj_dict.keys():
            field_value: Any = getattr(self, field_name)

            if isinstance(field_value, dict) and ("__class__" in field_value):
                logging.info(
                    f"Detected object '{field_name}' of type '{field_value['__class__']}'."
                )
                field_value = parse_obj_as(
                    types.new_class(field_name, (ObjectConfig[object],)), field_value
                )

            # use `ObjectConfig.create` if field is of type `ObjectConfig`
            if isinstance(field_value, ObjectConfig):
                obj_dict[field_name] = field_value.instantiate()

        if kwargs is not None:
            obj_dict.update(kwargs)

        assert is_indirect_generic_subclass(self.__class__)

        if hasattr(self.obj_cls, "create"):
            # TODO: this is a hack to avoid `mypy` error
            obj = self.obj_cls.create(**obj_dict)  # type: ignore
        else:
            obj = self.obj_cls(**obj_dict)

        type_T: Type[T_GENERIC] = get_args(self.__class__.__orig_bases__[0])[0]
        if not isinstance(obj, type_T):
            logging.fatal(
                f"Object {obj} is not a sub-class of config-specificed class '{type_T}'!"
            )

        return obj

    # this has to be arranged to the last position to avoid overriding `dict`
    def dict(  # type: ignore
        self,
        exclude: set | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict:
        if exclude is None:
            exclude = set()

        exclude = exclude | {"obj_cls"}

        return super().dict(*args, exclude=exclude, **kwargs)

    class Config:
        extra = Extra.allow
        arbitrary_types_allowed = True


class BaseConfig(BaseModel):
    @classmethod
    def parse_yaml(cls: Type[T_CONFIG], path: str | Path) -> T_CONFIG:
        logging.info(f"Loading config from path {path!s}")

        with open(path, "r") as f:
            obj: dict = yaml.unsafe_load(f)

        return cls.parse_obj(obj=obj)

    def to_yaml(self) -> str:
        return yaml.dump(self.dict(by_alias=True))
