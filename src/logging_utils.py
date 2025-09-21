import json
import logging
import sys
import uuid


def configure_logger(name: str = "app", level: int = logging.INFO) -> logging.Logger:
    """Create a JSON logger writing to stdout with a correlation id."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "cid": getattr(record, "cid", None) or str(uuid.uuid4()),
            }
            return json.dumps(payload)

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger