from dotenv import load_dotenv
from split_settings.tools import include
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv()

include(
    "components/base.py",
    "components/database.py",
    "components/apps.py",
    "components/middleware.py",
    "components/templates.py",
    "components/auth.py",
    "components/i18n.py",
    "components/static.py",
    "components/debug.py",
)

ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
