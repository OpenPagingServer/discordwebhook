import importlib.util
import os
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR.parent.parent / ".env"
load_dotenv(ENV_PATH)

core = None
running = False
thread = None
INTERVAL = 60


def load_message_send():
    module_name = "discordwebhook_message_send_runtime"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    module_path = BASE_DIR / "message_send.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


message_send = load_message_send()


def init(core_obj):
    global core, running, thread
    core = core_obj
    running = True
    message_send.ensure_database_schema()
    thread = threading.Thread(target=loop, daemon=True)
    thread.start()


def log(message):
    if core and hasattr(core, "log"):
        core.log(message)
    else:
        print(message)


def loop():
    while running:
        try:
            for endpoint in message_send.fetch_configured_endpoints():
                status = str(endpoint.get("status") or "").strip()
                if status == "Unchecked":
                    continue
                result = message_send.check_webhook(endpoint.get("webhook_url"))
                if result != status:
                    message_send.update_endpoint_status(endpoint.get("id"), result)
                    log(f"DiscordWebhook {endpoint.get('id')} -> {result}")
        except Exception as exc:
            log(f"DiscordWebhook module error: {exc}")
        time.sleep(INTERVAL)


def shutdown():
    global running
    running = False


def get_endpoint_status():
    return message_send.get_endpoint_status_payload()


def api_endpoint(command_string):
    message_send.handle_api(command_string)


def handle_dispatch(action, stream_id, msg_id, targets, metadata=None):
    message_send.handle_dispatch(action, stream_id, msg_id, targets, metadata)


def receive_audio(chunk, stream_id):
    message_send.receive_audio(chunk, stream_id)


def end_stream(stream_id):
    message_send.end_stream(stream_id)
