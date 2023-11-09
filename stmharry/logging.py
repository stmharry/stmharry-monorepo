import logging

from absl import app
from absl import logging as absl_logging
from rich.logging import RichHandler


def _patch_logging() -> None:
    logging.root.removeHandler(absl_logging._absl_handler)
    absl_logging._warn_preinit_stderr = False


def patch_logging() -> None:
    logging.basicConfig(
        level=logging.NOTSET,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler()],
    )
    app.call_after_init(_patch_logging)
