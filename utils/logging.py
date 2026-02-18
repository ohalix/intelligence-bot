import logging
import os
from typing import Any, Dict

def setup_logging(config: Dict[str, Any]) -> None:
    log_cfg = config.get("logging", {})
    level = getattr(logging, str(log_cfg.get("level","INFO")).upper(), logging.INFO)
    fmt = log_cfg.get("format","%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_path = log_cfg.get("file_path","./logs/web3_intelligence.log")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    handlers = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(file_path))
    except Exception:
        pass

    logging.basicConfig(level=level, format=fmt, handlers=handlers)
