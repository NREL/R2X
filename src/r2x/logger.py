"""R2X logger configuration."""

# System packages
import argparse
import os
import sys

# Third-party packages
from loguru import logger

# Logger printing formats
DEFAULT_FORMAT = "<level>{level}</level>: {message}"


class Formatter:  # noqa: D101
    def __init__(self):
        self.padding = 0
        self.fmt = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level>| <cyan>{name}:{line}{extra[padding]}</cyan> | {message}\n{exception}"  # noqa: E501

    def format(self, record):  # noqa: D102
        length = len("{name}:{line}".format(**record))
        self.padding = max(self.padding, length)
        record["extra"]["padding"] = " " * (self.padding - length)
        return self.fmt


def setup_logging(
    filename=None,
    level="INFO",
    debug: bool = False,
):
    """Configure logging of file.

    Parameters
    ----------
    filename : str | None
        log filename
    level : str, optional
        change defualt level of logging.
    verbose :  bool
        returns additional logging information.
    """
    if debug:
        level = "DEBUG"
    from loguru import logger

    logger.remove()
    logger.enable("r2x")
    # logger.enable("infrasys")
    # logger.enable("resource_monitor")
    level = os.environ["LOGURU_LEVEL"] if os.environ.get("LOGURU_LEVEL") else level
    logger.add(
        sys.stderr,
        level=level,
        enqueue=False,
        format=Formatter().format if level == "DEBUG" or level == "TRACE" else DEFAULT_FORMAT,
    )
    if filename:
        logger.add(filename, level=level, enqueue=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    setup_logging(level="TRACE")

    # Testing different types of loggers.
    logger.trace("A trace message.")
    logger.debug("A debug message.")
    logger.info("An info message.")
    logger.success("A success message.")
    logger.warning("A warning message.")
    logger.error("An error message.")
    logger.critical("A critical message.")
