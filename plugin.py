from airflow.plugins_manager import AirflowPlugin
import listener

class MetadataCollectionPlugin(AirflowPlugin):
    name = "MetadataCollectionPlugin"
    listeners = [listener]