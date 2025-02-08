import importlib
import warnings
from pathlib import Path
from types import ModuleType
from typing import Annotated, Any, Generic, Protocol, Type, TypeGuard, TypeVar, get_args

import yaml
from absl import logging
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.warnings import GenericBeforeBaseModelWarning

warnings.filterwarnings("ignore", category=GenericBeforeBaseModelWarning)

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


def instantiate_obj(obj: Any) -> Any:
    match obj:
        case ObjectConfig():
            obj_dict: dict[str, Any] = {
                key: instantiate_obj(getattr(obj, key))
                # model_fields + model_extra.keys()
                for key in obj.model_dump().keys()
            }
            logging.debug(
                f"Creating object of class '{obj.__class__.__name__}' using dict {obj_dict}."
            )

            # `obj_cls` has been checked in `validate_obj_cls()` as a sub-class
            obj_cls: Type = obj_dict.pop("obj_cls")

            if hasattr(obj_cls, "create"):
                obj_inst = obj_cls.create(**obj_dict)
            else:
                obj_inst = obj_cls(**obj_dict)

            return obj_inst

        case dict():
            obj = {key: instantiate_obj(value) for key, value in obj.items()}

            try:
                obj = ObjectConfig.model_validate(obj)

            except ValidationError:
                ...

            else:
                # we are a deeply hidden `ObjectConfig`!
                obj = instantiate_obj(obj)

        case list() | tuple() | set():
            obj = type(obj)(instantiate_obj(value) for value in obj)

    return obj


class ObjectConfig(Generic[T_GENERIC], BaseModel):
    model_config = ConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
    )

    obj_cls: Annotated[
        Type,
        # a default value of None would trigger the validation to use the generic type
        Field(alias="__class__"),
    ]

    @classmethod
    def get_generic_type(cls) -> Type:
        assert is_indirect_generic_subclass(cls)

        return get_args(cls.__orig_bases__[0])[0]

    @model_validator(mode="before")
    @classmethod
    def set_obj_cls_default(cls, values: Any) -> dict:
        match values:
            case dict():
                if "__class__" not in values:
                    return {"__class__": cls.get_generic_type(), **values}

        return values

    @field_validator("obj_cls", mode="before")
    @classmethod
    def validate_obj_cls(cls, v: Any) -> Type:
        obj_cls: Type | None

        match v:
            case type():
                obj_cls = v

            case str():
                module_name: str
                obj_name: str
                (module_name, _, obj_name) = v.rpartition(".")

                if module_name == "":
                    module_name = "__main__"

                module: ModuleType = import_module(module_name)
                obj_cls = getattr(module, obj_name, None)

                if obj_cls is None:
                    raise ValueError(
                        f"Referenced module name '{module_name}' not found!"
                    )

            case _:
                raise ValueError(f"Invalid object class reference '{v}'!")

        generic_type: Type = cls.get_generic_type()
        if (generic_type is not T_GENERIC) and not issubclass(  # type: ignore
            obj_cls, generic_type
        ):
            raise ValueError(
                f"Object class '{obj_cls}' is not a sub-class of '{generic_type}'!"
            )

        return obj_cls

    @field_serializer("obj_cls")
    def serialize_obj_cls(self, obj_cls: Type, _info) -> str:
        return f"{obj_cls.__module__}.{obj_cls.__name__}"

    def instantiate(self, **kwargs: Any) -> T_GENERIC:
        if kwargs is not None:
            for key, value in kwargs.items():
                setattr(self, key, value)

        return instantiate_obj(self)


class BaseConfig(BaseModel):
    @classmethod
    def parse_yaml(cls: Type[T_CONFIG], path: str | Path) -> T_CONFIG:
        logging.info(f"Loading config from path {path!s}")

        with open(path, "r") as f:
            yaml_obj: dict[str, Any] = yaml.unsafe_load(f)

        return TypeAdapter(cls).validate_python(yaml_obj)

    def to_yaml(self) -> str:
        return yaml.dump(self.model_dump(by_alias=True))
