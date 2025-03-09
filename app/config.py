import logging
import os
from typing import Literal
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - [QUALITY_CHEKS] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
    level=logging.INFO
)

def get_env_var(key: str, default: str) -> str:
    value = os.getenv(key, default)
    if value == default:
        logging.warning(f"Using default value for {key}: {default}")
    else:
        logging.info(f"Loaded {key} from environment variables.")
    return value

DB_USER = get_env_var("DB_USER", "root")
DB_PASSWORD = get_env_var("DB_PASSWORD", "root")
DB_HOST = get_env_var("DB_HOST", "localhost")
DB_PORT = get_env_var("DB_PORT", "3377")
DB_NAME = get_env_var("DB_NAME", "inventory")
CONNECTION_STRING = (f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
CONN_IDLE_TIMEOUT = 3600
DEFAULT_CHUNK_SIZE = 20000
DATAHUB_SERVER_URL = get_env_var("DATAHUB_SERVER_URL", "http://localhost:8080")
DATAHUB_PLATFORM_URN = get_env_var("PLATFORM_URN", "urn:li:dataPlatform:mysql")
DATAHUB_ENTITY_URN = get_env_var("ENTITY_URN", "urn:li:dataset:(urn:li:dataPlatform:mysql,inventory.products,PROD)")
DB_TYPE_MYSQL = "mysql"
CONTROLLER_CONFIG_PATH = "./app/controller_configs.yaml"
