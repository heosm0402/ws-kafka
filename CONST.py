import os


PROJECT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
BOOTSTRAP_SERVER = os.environ.get('WS_BOOTSTRAP_SERVER')
TOPIC_NAME = os.environ.get('WS_TOPIC_USER')
SCHEMA_REGISTRY = os.environ.get('WS_SCHEMA_REGISTRY')