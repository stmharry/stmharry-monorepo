import importlib
import itertools
import warnings
from collections.abc import Callable
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
    def validate_obj_cls_default(cls, values: Any) -> dict:
        match values:
            case dict():
                if "__class__" not in values:
                    values["__class__"] = cls.get_generic_type()

        return values

    @field_validator("obj_cls", mode="before")
    @classmethod
    def validate_obj_cls(cls, v: Any) -> Type:
        match v:
            case type():
                return v

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

                return obj_cls

            case _:
                raise ValueError(f"Invalid object class reference '{v}'!")

    @field_serializer("obj_cls")
    def serialize_obj_cls(self, obj_cls: Type, _info) -> str:
        return f"{obj_cls.__module__}.{obj_cls.__name__}"

    @classmethod
    def recursive_model_validate(
        cls,
        obj: Any,
        after_validator: Callable[[Any], Any] | None = None,
    ) -> Any:
        match obj:
            case BaseModel():
                obj = obj.__class__.model_construct(
                    **{
                        key: cls.recursive_model_validate(
                            getattr(obj, key), after_validator=after_validator
                        )
                        for key in obj.model_dump().keys()  # every field including extras
                    }
                )

            case dict():
                obj = {
                    key: cls.recursive_model_validate(
                        value, after_validator=after_validator
                    )
                    for key, value in obj.items()
                }

                # this has to be done after the validation of each field
                try:
                    obj = cls.model_validate(obj)

                except ValidationError:
                    ...

            case list() | tuple() | set():
                obj = type(obj)(
                    cls.recursive_model_validate(value, after_validator=after_validator)
                    for value in obj
                )

        if after_validator is not None:
            obj = after_validator(obj)

        return obj

    @classmethod
    def instantiate_obj(cls, config: Any) -> T_GENERIC:
        if not isinstance(config, cls):
            return config

        config_dict: dict = {
            key: getattr(config, key)
            for key in itertools.chain(
                config.model_fields,
                config.model_extra.keys() if config.model_extra else [],
            )
            if key not in {"obj_cls"}
        }
        logging.debug(
            f"Creating object of class '{config.obj_cls.__name__}' using config dict {config_dict}."
        )

        assert is_indirect_generic_subclass(config.__class__)

        obj_instance: T_GENERIC
        if hasattr(config.obj_cls, "create"):
            # TODO: this is a hack to avoid `mypy` error
            obj_instance = config.obj_cls.create(**config_dict)  # type: ignore
        else:
            obj_instance = config.obj_cls(**config_dict)

        if config.__class__ is not ObjectConfig:
            type_T: Type[T_GENERIC] = config.__class__.get_generic_type()  # type: ignore

            if not isinstance(obj_instance, type_T):
                logging.fatal(
                    f"Object {obj_instance} is not a sub-class of config-specificed class '{type_T}'!"
                )

        return obj_instance  # type: ignore

    def instantiate(self, **kwargs: Any) -> T_GENERIC:
        if kwargs is not None:
            for key, value in kwargs.items():
                setattr(self, key, value)

        return ObjectConfig.recursive_model_validate(
            self, after_validator=ObjectConfig.instantiate_obj
        )


class BaseConfig(BaseModel):
    @classmethod
    def parse_yaml(cls: Type[T_CONFIG], path: str | Path) -> T_CONFIG:
        logging.info(f"Loading config from path {path!s}")

        with open(path, "r") as f:
            obj: dict[str, Any] = yaml.unsafe_load(f)

        return ObjectConfig.recursive_model_validate(
            TypeAdapter(cls).validate_python(obj)
        )

    def to_yaml(self) -> str:
        return yaml.dump(self.model_dump(by_alias=True))
