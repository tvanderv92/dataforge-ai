from dataforge_ai.core.plugin_interface import PluginInterface
from typing import Dict, Any, Union
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableSerializable
import json


class AirflowDAGConverterPlugin(PluginInterface):
    def __init__(self, llm, prompt_generator):
        super().__init__()
        self.llm = llm
        self.prompt_generator = prompt_generator

    def execute(self, input_data: Union[str, Dict[str, Any]]) -> str:
        """
        Convert a dlt pipeline to an Airflow DAG.

        :param input_data: A dictionary or JSON string containing the pipeline code.
        :return: Generated Airflow DAG code as a string.
        """
        self.log_execution("Starting Airflow DAG conversion")

        # Parse input_data if it's a string
        if isinstance(input_data, str):
            try:
                input_data = json.loads(input_data)
            except json.JSONDecodeError:
                # If JSON parsing fails, assume the entire string is the pipeline code
                input_data = {"pipeline_code": input_data}

        if not self.validate_input(input_data):
            raise ValueError("Invalid input data")

        prompt = self.prompt_generator.execute({
            "prompt_type": "airflow_dag",
            "parameters": input_data
        })

        # Escape curly braces in the pipeline code
        escaped_pipeline_code = input_data["pipeline_code"].replace("{", "{{").replace("}", "}}")

        dag_template = PromptTemplate(template=prompt, input_variables=["pipeline_code"])
        dag_chain = dag_template | self.llm | StrOutputParser()
        dag_code = dag_chain.invoke({"pipeline_code": escaped_pipeline_code})

        self.log_execution("Airflow DAG conversion completed")
        return dag_code

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        """
        Validate the input data for the plugin.
    
        :param input_data: A dictionary containing the input data for the plugin.
        :return: True if the input is valid, False otherwise.
        """
        if not isinstance(input_data, dict):
            self.log_execution("Input validation failed: input_data is not a dictionary", level="error")
            return False

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
            },
            "required": ["pipeline_code"]
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
