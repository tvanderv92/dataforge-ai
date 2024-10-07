import json
import re
from typing import Dict, Any, Union


from dataforge_ai.core.plugin_interface import PluginInterface
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser


class PipelineGeneratorPlugin(PluginInterface):
    VALID_SOURCE_TYPES = ["rest_api", "sql_database", "filesystem"]
    VALID_DESTINATION_TYPES = ["filesystem", "bigquery", "redshift", "postgres", "mysql", "duckdb"]

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
        input_data = self._parse_input(input_data)

        if not self.validate_input(input_data):
            raise ValueError("Invalid input data structure")

        return self._generate_pipeline(input_data)

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        """
        Validate the input data for the plugin.

        :param input_data: A dictionary containing the input data for the plugin.
        :return: True if the input is valid, False otherwise.
        """
        self.log_execution(f"Validating input data: {json.dumps(input_data, indent=2)}", level="debug")
        required_keys = ['source', 'destination', 'pipeline_name', 'schedule']

        if not all(key in input_data for key in required_keys):
            missing_keys = [key for key in required_keys if key not in input_data]
            self.log_execution(f"Input validation failed: missing required keys {missing_keys}", level="error")
            return False

        return self._validate_source_config(input_data['source']) and self._validate_destination_config(
            input_data['destination'])

    def _parse_input(self, input_data: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Parse and validate input data from a JSON string or dictionary.

        :param input_data: Input data in JSON string or dictionary format.
        :return: Parsed input data as a dictionary.
        """
        self.log_execution(f"Received input data: {input_data}", level="debug")

        if isinstance(input_data, str):
            # Extract content between triple backticks
            pattern = r"```json\s*([\s\S]*?)\s*```"
            match = re.search(pattern, input_data)

            if match:
                cleaned_input_data = match.group(1).strip()
                self.log_execution(f"Extracted JSON content: {repr(cleaned_input_data)}", level="debug")

                try:
                    # Attempt to parse the extracted JSON string
                    return json.loads(cleaned_input_data)
                except json.JSONDecodeError as e:
                    self.log_execution(f"Error parsing JSON: {str(e)}", level="error")
                    self.log_execution(f"Problematic input: {repr(cleaned_input_data)}", level="error")
                    raise ValueError(f"Invalid JSON string provided as input_data: {str(e)}")
            else:
                self.log_execution("No JSON content found between ```json ... ``` markers", level="error")
                raise ValueError("No valid JSON content found in the input string")

        return input_data


    def _generate_pipeline(self, input_data: Dict[str, Any]) -> str:
        """
        Generate pipeline code using LLM based on the input configuration.

        :param input_data: Parsed input configuration dictionary.
        :return: Generated pipeline code as a string.
        """
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

    def _validate_source_config(self, source_config: Dict[str, Any]) -> bool:
        """
        Validate the source configuration.

        :param source_config: The source configuration dictionary.
        :return: True if valid, False otherwise.
        """
        self.log_execution(f"Validating source config: {json.dumps(source_config, indent=2)}", level="debug")

        if source_config.get('type') not in self.VALID_SOURCE_TYPES:
            self.log_execution(
                f"Invalid source type: {source_config.get('type')}. Valid types are: {self.VALID_SOURCE_TYPES}",
                level="error")
            return False

        if 'config' not in source_config:
            self.log_execution(f"Missing required 'config' key in source configuration.", level="error")
            return False

        return True

    def _validate_destination_config(self, destination_config: Dict[str, Any]) -> bool:
        """
        Validate the destination configuration.

        :param destination_config: The destination configuration dictionary.
        :return: True if valid, False otherwise.
        """
        self.log_execution(f"Validating destination config: {json.dumps(destination_config, indent=2)}", level="debug")

        if destination_config.get('type') not in self.VALID_DESTINATION_TYPES:
            self.log_execution(
                f"Invalid destination type: {destination_config.get('type')}. Valid types are: {self.VALID_DESTINATION_TYPES}",
                level="error")
            return False

        if 'config' not in destination_config:
            self.log_execution(f"Missing required 'config' key in destination configuration.", level="error")
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
                        "type": {"type": "string", "enum": self.VALID_SOURCE_TYPES},
                        "config": {"type": "object", "required": ["base_url", "endpoints", "auth", "pagination"]}
                    },
                    "required": ["type", "config"]
                },
                "destination": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": self.VALID_DESTINATION_TYPES},
                        "config": {"type": "object",
                                   "required": ["account_name", "account_key", "container_name", "folder_path"]}
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
