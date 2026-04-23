"""
Google Maps Business Scraper — scraper package.

Public API
----------
>>> from scraper import load_config, ProxyManager
>>> cfg = load_config("config.yaml")
>>> proxy_mgr = ProxyManager(cfg["stealth"]["proxies"])
"""

from .browser  import ProxyManager, launch_browser, is_captcha_page, handle_captcha
from .config   import load_config
from .controls import ControlState, ControlHandler
from .storage  import (
    build_output_path, load_existing_output, save_output,
    build_row, save_checkpoint, load_checkpoint, clear_checkpoint,
    log_done_query, load_done_queries, clear_done_queries,
    OUTPUT_FIELDS,
)
from .filters  import is_in_region, classify_company, make_uid, rebuild_seen_ids
from .utils    import beep, elapsed, clean_phone

__version__ = "1.0.0"
__all__ = [
    # browser
    "ProxyManager", "launch_browser", "is_captcha_page", "handle_captcha",
    # config
    "load_config",
    # controls
    "ControlState", "ControlHandler",
    # storage
    "build_output_path", "load_existing_output", "save_output", "build_row",
    "save_checkpoint", "load_checkpoint", "clear_checkpoint",
    "log_done_query", "load_done_queries", "clear_done_queries", "OUTPUT_FIELDS",
    # filters
    "is_in_region", "classify_company", "make_uid", "rebuild_seen_ids",
    # utils
    "beep", "elapsed", "clean_phone",
]
