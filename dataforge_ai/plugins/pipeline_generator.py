import json

from dataforge_ai.core.plugin_interface import PluginInterface
from typing import Dict, Any, Union
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableSerializable


class PipelineGeneratorPlugin(PluginInterface):
    def __init__(self, llm, prompt_generator):
        super().__init__()
        self.llm = llm
        self.prompt_generator = prompt_generator

    def execute(self, input_data: Union[str, Dict[str, Any]]) -> str:
        """
        Generate a data pipeline based on the input configuration.

        :param input_data: A dictionary or JSON string containing the pipeline configuration.
        :return: Generated pipeline code as a string.
        """
        self.log_execution("Starting pipeline generation")

        # Parse input_data if it's a string
        if isinstance(input_data, str):
            try:
                # Remove any leading/trailing whitespace and code block markers
                input_data = input_data.strip().lstrip('`').rstrip('`')
                if input_data.startswith('json'):
                    input_data = input_data[4:].lstrip()
                input_data = json.loads(input_data)
            except json.JSONDecodeError as e:
                self.log_execution(f"Error parsing JSON: {str(e)}", level="error")
                raise ValueError(f"Invalid JSON string provided as input_data: {str(e)}")

        if not self.validate_input(input_data):
            raise ValueError("Invalid input data structure")

        try:
            prompt_data = self.prompt_generator.execute({
                "prompt_type": "data_pipeline",
                "parameters": input_data
            })

            pipeline_template = PromptTemplate(
                template=prompt_data["template"],
                input_variables=prompt_data["input_variables"]
            )
            pipeline_chain = pipeline_template | self.llm | StrOutputParser()
            pipeline_code = pipeline_chain.invoke(prompt_data["formatted_params"])

            self.log_execution("Pipeline generation completed")
            return pipeline_code
        except Exception as e:
            self.log_execution(f"Error during pipeline generation: {str(e)}", level="error")
            raise

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        """
        Validate the input data for the plugin.

        :param input_data: A dictionary containing the input data for the plugin.
        :return: True if the input is valid, False otherwise.
        """
        required_keys = ['source', 'destination', 'pipeline_name', 'schedule']
        if not all(key in input_data for key in required_keys):
            self.log_execution("Input validation failed: missing required keys", level="error")
            return False

        # Validate source configuration
        if not self._validate_source_config(input_data['source']):
            return False

        # Validate destination configuration
        if not self._validate_destination_config(input_data['destination']):
            return False

        return True

    def _validate_source_config(self, source_config: Dict[str, Any]) -> bool:
        """
        Validate the source configuration.

        :param source_config: The source configuration dictionary.
        :return: True if valid, False otherwise.
        """
        if source_config.get('type') != 'rest_api':
            self.log_execution("Invalid source type", level="error")
            return False

        required_source_keys = ['config']
        if not all(key in source_config for key in required_source_keys):
            self.log_execution("Missing required source configuration keys", level="error")
            return False

        return True

    def _validate_destination_config(self, destination_config: Dict[str, Any]) -> bool:
        """
        Validate the destination configuration.

        :param destination_config: The destination configuration dictionary.
        :return: True if valid, False otherwise.
        """
        if destination_config.get('type') != 'azure_blob':
            self.log_execution("Invalid destination type", level="error")
            return False

        required_dest_keys = ['config']
        if not all(key in destination_config for key in required_dest_keys):
            self.log_execution("Missing required destination configuration keys", level="error")
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
                "source": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["rest_api"]},
                        "config": {
                            "type": "object",
                            "properties": {
                                "base_url": {"type": "string"},
                                "endpoints": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "path": {"type": "string"},
                                            "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE"]}
                                        },
                                        "required": ["name", "path", "method"]
                                    }
                                },
                                "auth": {
                                    "type": "object",
                                    "properties": {
                                        "type": {"type": "string", "enum": ["none", "bearer_token", "api_key"]}
                                    },
                                    "required": ["type"]
                                },
                                "pagination": {
                                    "type": "object",
                                    "properties": {
                                        "type": {"type": "string", "enum": ["offset", "page", "cursor"]},
                                        "limit_param": {"type": "string"},
                                        "offset_param": {"type": "string"},
                                        "total_count_path": {"type": "string"}
                                    },
                                    "required": ["type"]
                                }
                            },
                            "required": ["base_url", "endpoints", "auth", "pagination"]
                        }
                    },
                    "required": ["type", "config"]
                },
                "destination": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["azure_blob"]},
                        "config": {
                            "type": "object",
                            "properties": {
                                "account_name": {"type": "string"},
                                "account_key": {"type": "string"},
                                "container_name": {"type": "string"},
                                "folder_path": {"type": "string"}
                            },
                            "required": ["account_name", "account_key", "container_name", "folder_path"]
                        }
                    },
                    "required": ["type", "config"]
                },
                "pipeline_name": {"type": "string"},
                "schedule": {"type": "string"}
            },
            "required": ["source", "destination", "pipeline_name", "schedule"]
        }

    def get_output_schema(self) -> Dict[str, Any]:
        """
        Get the schema for the output data produced by the plugin.

        :return: A dictionary representing the output schema.
        """
        return {
            "type": "string",
            "description": "Generated dlt pipeline code"
        }
