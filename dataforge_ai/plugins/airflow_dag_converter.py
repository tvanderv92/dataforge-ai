import json
import os
import re
from typing import Dict, Any, Union

from dataforge_ai.core.plugin_interface import PluginInterface


class AirflowDAGConverterPlugin(PluginInterface):
    def __init__(self, output_dir: str = 'dags'):
        super().__init__()
        self.output_dir = output_dir

    def execute(self, input_data: Union[str, Dict[str, Any]]) -> Dict[str, str]:
        """
        Convert a dlt pipeline to an Airflow DAG and write it to a file.

        :param input_data: A dictionary or JSON string containing the pipeline code and configuration.
        :return: Dictionary with the generated Airflow DAG code and the file path.
        """
        self.log_execution("Starting Airflow DAG conversion")

        if isinstance(input_data, str):
            input_data = json.loads(input_data)

        if not self.validate_input(input_data):
            raise ValueError("Invalid input data")

        pipeline_code = input_data['pipeline_code']
        pipeline_name = input_data.get('pipeline_name', self._extract_pipeline_name(pipeline_code))
        schedule = input_data.get('schedule', None)
        dataset_name = input_data.get('dataset_name', 'default_dataset')

        dag_code = self._generate_airflow_dag(pipeline_code, pipeline_name, schedule, dataset_name)
        file_path = self._write_dag_to_file(dag_code, pipeline_name)

        self.log_execution("Airflow DAG conversion completed and written to file")
        return {"dag_code": dag_code, "file_path": file_path}

    def _write_dag_to_file(self, dag_code: str, pipeline_name: str) -> str:
        os.makedirs(self.output_dir, exist_ok=True)
        file_name = f"dag_{pipeline_name}.py"
        file_path = os.path.join(self.output_dir, file_name)

        with open(file_path, 'w') as f:
            f.write(dag_code)

        self.log_execution(f"DAG written to file: {file_path}")
        return file_path

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        if not isinstance(input_data, dict):
            self.log_execution("Input validation failed: input_data is not a dictionary", level="error")
            return False

        if 'pipeline_code' not in input_data or not isinstance(input_data['pipeline_code'], str):
            self.log_execution("Input validation failed: missing or invalid pipeline_code", level="error")
            return False
        return True

    def get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "pipeline_code": {"type": "string"},
                "pipeline_name": {"type": "string"},
                "schedule": {"type": "string"}
            },
            "required": ["pipeline_code"]
        }

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "dag_code": {"type": "string"},
                "file_path": {"type": "string"}
            },
            "required": ["dag_code", "file_path"]
        }

    def _extract_pipeline_name(self, pipeline_code: str) -> str:
        match = re.search(r'pipeline_name\s*=\s*["]([\w_]+)["]', pipeline_code)
        return match.group(1) if match else "dlt_pipeline"

    def _generate_airflow_dag(self, pipeline_code: str, pipeline_name: str, schedule: str, dataset_name: str) -> str:
        dag_code = f"""
        from airflow.decorators import dag
        from dlt.common import pendulum
        from dlt.helpers.airflow_helper import PipelineTasksGroup
        from tenacity import Retrying, stop_after_attempt
        
        default_args = {{
            'owner': 'airflow',
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
        }}
        
        @dag(
            dag_id='{pipeline_name}_dag',
            default_args=default_args,
            description='DAG for {pipeline_name} pipeline',
            schedule_interval='{schedule}',
            start_date=pendulum.datetime(2023, 1, 1),
            catchup=False,
            max_active_runs=1,
        )
        def load_{pipeline_name}_data():
            tasks = PipelineTasksGroup(
                pipeline_name="{pipeline_name}",
                use_data_folder=False,
                wipe_local_data=True,
                use_task_logger=True,
                retry_policy=Retrying(stop=stop_after_attempt(3), reraise=True),
            )
        
            {pipeline_code}
        
            # Modify the pipeline parameters if needed
            pipeline = dlt.pipeline(pipeline_name='{pipeline_name}', dataset_name='{dataset_name}', full_refresh=False)
        
            # Create the source, the "serialize" decompose option
            # will convert dlt resources into Airflow tasks.
            tasks.add_run(
                pipeline=pipeline,
                data=source,
                decompose="serialize",
                trigger_rule="all_done",
                retries=0,
                provide_context=True
            )
        
        load_{pipeline_name}_data()
        """
        return dag_code
