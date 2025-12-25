from config.version import __version__
import logging
import os

APP_VERSION = os.getenv("APP_VERSION", __version__)

class VersionFilter(logging.Filter):
    def filter(self, record):
        record.version = APP_VERSION
        return True


def setup_logging():
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | v%(version)s | %(name)s | %(message)s"
    )

    handler.setFormatter(formatter)
    handler.addFilter(VersionFilter())

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)