from airflow.plugins_manager import AirflowPlugin
from powerbi_plugin.hooks.powerbi import PowerBIHook
from powerbi_plugin.operators.dataset_in_group import PowerBIDatasetInGroupRefreshOperator


class PowerBIPlugin(AirflowPlugin):
    name = 'powerbi_plugin'
    hooks = [PowerBIHook]
    operators = [PowerBIDatasetInGroupRefreshOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
