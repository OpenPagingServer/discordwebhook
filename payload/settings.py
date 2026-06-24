import importlib.util
from pathlib import Path


def module_web():
    module_path = Path(__file__).resolve().parent.parent / "web" / "web.py"
    spec = importlib.util.spec_from_file_location("discord_web", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def handle_request(request, conn_factory, page, user):
    return module_web().render_settings(request, conn_factory, page, user)
