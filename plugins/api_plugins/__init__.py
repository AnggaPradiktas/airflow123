from airflow.plugins_manager import AirflowPlugin
from api_plugins.operators.api_to_postgres_operator import ApiToPostgresOperator


class ApiToPostgresOperator(AirflowPlugin):
    name = "ApiToPostgresOperator"
    operators = [ApiToPostgresOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
