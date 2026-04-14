"""
Application logging setup.
Writes to console plus local server log files used by the UI.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

import config


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    out_path = os.path.join(config.BASE_DIR, "server.out.log")
    err_path = os.path.join(config.BASE_DIR, "server.err.log")
    existing_files = {
        os.path.abspath(getattr(handler, "baseFilename", ""))
        for handler in root.handlers
        if getattr(handler, "baseFilename", None)
    }

    if not any(isinstance(handler, logging.StreamHandler) and not getattr(handler, "baseFilename", None)
               for handler in root.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

    if os.path.abspath(out_path) not in existing_files:
        out_handler = RotatingFileHandler(
            out_path,
            maxBytes=1_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        out_handler.setLevel(logging.INFO)
        out_handler.setFormatter(formatter)
        root.addHandler(out_handler)

    if os.path.abspath(err_path) not in existing_files:
        err_handler = RotatingFileHandler(
            err_path,
            maxBytes=1_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        err_handler.setLevel(logging.ERROR)
        err_handler.setFormatter(formatter)
        root.addHandler(err_handler)

    logging.captureWarnings(True)
