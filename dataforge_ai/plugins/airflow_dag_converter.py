from dataforge_ai.core.plugin_interface import PluginInterface
from typing import Dict, Any
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableSerializable


class AirflowDAGConverterPlugin(PluginInterface):
    def __init__(self, llm, prompt_generator):
        super().__init__()
        self.llm = llm
        self.prompt_generator = prompt_generator

    def execute(self, input_data: Dict[str, Any]) -> str:
        """
        Convert a dlt pipeline to an Airflow DAG.

        :param input_data: A dictionary containing the dlt pipeline code.
        :return: Generated Airflow DAG code as a string.
        """
        self.log_execution("Starting Airflow DAG conversion")
        if not self.validate_input(input_data):
            raise ValueError("Invalid input data")

        prompt = self.prompt_generator.execute({
            "prompt_type": "airflow_dag",
            "parameters": input_data
        })

        dag_template = PromptTemplate(template=prompt, input_variables=[])
        dag_chain = dag_template | self.llm | StrOutputParser()
        dag_code = dag_chain.invoke({})

        self.log_execution("Airflow DAG conversion completed")
        return dag_code

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        """
        Validate the input data for the plugin.

        :param input_data: A dictionary containing the input data for the plugin.
        :return: True if the input is valid, False otherwise.
        """
        if 'pipeline_code' not in input_data or not isinstance(input_data['pipeline_code'], str):
            self.log_execution("Input validation failed: missing or invalid pipeline_code", level="error")
            return False
        return True

    def get_input_schema(self) -> Dict[str, Any]:
        """
        Get the schema for the input data expected by the plugin.

        :return: A dictionary representing the input schema.
        """
        return {
            "type": "object",
            "properties": {
                "pipeline_code": {"type": "string"},
                "pipeline_name": {"type": "string"},
                "schedule": {"type": "string"}
            },
            "required": ["pipeline_code", "pipeline_name", "schedule"]
        }

    def get_output_schema(self) -> Dict[str, Any]:
        """
        Get the schema for the output data produced by the plugin.

        :return: A dictionary representing the output schema.
        """
        return {
            "type": "string",
            "description": "Generated Airflow DAG code"
        }
