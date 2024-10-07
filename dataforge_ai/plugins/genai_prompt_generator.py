import ast
import json

from dataforge_ai.core.plugin_interface import PluginInterface
from typing import Dict, Any, Union, List
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableSerializable


class GenAIPromptGenerator(PluginInterface):
    def __init__(self, azure_endpoint: str, azure_deployment: str, azure_api_key: str):
        super().__init__()
        self.llm = AzureChatOpenAI(
            azure_endpoint=azure_endpoint,
            azure_deployment=azure_deployment,
            api_key=azure_api_key,
            api_version="2023-03-15-preview",
            temperature=0.7,
            max_tokens=4096
        )

    def execute(self, input_data: Dict[str, Any]) -> str:
        self.log_execution("Starting prompt generation")
        if not self.validate_input(input_data):
            raise ValueError("Invalid input data")

        prompt_type = input_data.get('prompt_type')
        parameters = input_data.get('parameters', {})

        prompt_generators = {
            'data_pipeline': self._generate_dlt_pipeline_prompt,
            'code_explanation': self._generate_code_explanation_prompt,
            'react_agent': self._generate_react_agent_prompt,
            'airflow_dag': self._generate_airflow_dag_prompt,
            # 'dlt_pipeline': self._generate_dlt_pipeline_prompt
        }

        if prompt_type not in prompt_generators:
            raise ValueError(f"Unsupported prompt type: {prompt_type}")

        return prompt_generators[prompt_type](parameters)

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        required_keys = ['prompt_type', 'parameters']
        if not all(key in input_data for key in required_keys):
            self.log_execution("Input validation failed: missing required keys", level="error")
            return False
        return True

    def get_input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "prompt_type": {"type": "string",
                                "enum": ["data_pipeline"
                                    , "code_explanation"
                                    , "react_agent"
                                    , "airflow_dag"
                                    , "dlt_pipeline"]
                                },
                "parameters": {"type": "object"}
            },
            "required": ["prompt_type", "parameters"]
        }

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "type": "string",
            "description": "Generated prompt"
        }

    def _create_chain(self, template: str, input_variables: list) -> RunnableSerializable:
        prompt = PromptTemplate(template=template, input_variables=input_variables)
        return prompt | self.llm | StrOutputParser()

    def _generate_data_pipeline_prompt(self, parameters: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        template = """
        Create a data pipeline that extracts data from {source_type} and loads it into {destination_type}.
        The source details are: {source_details}
        The destination details are: {destination_details}
        Additional requirements: {requirements}
    
        Generate a Python script to implement this pipeline.
        Include error handling, logging, and best practices for data pipeline development.
        """

        print("Generated Template:", template)  # Log the template to inspect variables

        # Convert parameters to dictionary if it's a string
        if isinstance(parameters, str):
            parameters = self._parse_parameters_string(parameters)

        # Format the parameters for the prompt
        formatted_params = {
            "source_type": parameters['source']['type'],
            "destination_type": parameters['destination']['type'],
            "source_details": str(parameters['source']['config']),
            "destination_details": str(parameters['destination']['config']),
            "requirements": f"Pipeline name: {parameters['pipeline_name']}, Schedule: {parameters['schedule']}"
        }

        # Ensure that only the defined keys are in formatted_params
        defined_keys = {"source_type", "destination_type", "source_details", "destination_details", "requirements"}
        for key in formatted_params.keys():
            if key not in defined_keys:
                self.log_execution(f"Unexpected key '{key}' found in formatted_params", level="warning")

        # Log the formatted parameters for debugging
        self.log_execution(f"Formatted parameters: {formatted_params}", level="debug")

        return {
            "template": template,
            "input_variables": list(formatted_params.keys()),
            "formatted_params": formatted_params
        }

    def _parse_parameters_string(self, parameters_str: str) -> Dict[str, Any]:
        """
        Helper method to parse a string of parameters into a dictionary.
        Tries to parse using both `ast.literal_eval` and `json.loads`.
        Raises a `ValueError` if the string cannot be parsed.
        """
        try:
            return ast.literal_eval(parameters_str)
        except (ValueError, SyntaxError):
            try:
                return json.loads(parameters_str)
            except json.JSONDecodeError as e:
                raise ValueError(f"Unable to parse parameters string: {e}")

    def _generate_code_explanation_prompt(self, parameters: Dict[str, Any]) -> str:
        template = """
        Explain the following {language} code:
        {code}
        Focus on: {focus_areas}
        Explanation level: {explanation_level}
        """
        chain = self._create_chain(template, ["language", "code", "focus_areas", "explanation_level"])
        return chain.invoke(parameters)

    def _generate_react_agent_prompt(self, parameters: Dict[str, Any]) -> str:
        template = """
        You are a data engineer tasked with {task_description}
        You have access to the following tools:
    
        {available_tools}
    
        Use the following format:
    
        Question: the input question you must answer
        Thought: you should always think about what to do
        Action: the action to take, should be one of [{tool_names}]
        Action Input: the input to the action
        Observation: the result of the action
        ... (this Thought/Action/Action Input/Observation can repeat N times)
        Thought: I now know the final answer
        Final Answer: the final answer to the original input question
    
        Begin!
        """
        return template.format(
            task_description=parameters['task_description'],
            available_tools="\n".join(parameters['available_tools']),
            tool_names=", ".join(parameters['available_tools'])
        )

    def _generate_airflow_dag_prompt(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        template = """
        Convert the following dlt pipeline code to an Apache Airflow DAG:
    
        {pipeline_code}
    
        Consider the following requirements:
        1. Import necessary Airflow modules
        2. Define the DAG using airflow.DAG
        3. Create a PythonOperator that runs the dlt pipeline
        4. Set up proper scheduling and default arguments
        5. Include any necessary environment variables or connections
        6. Implement error handling and logging for the Airflow task
    
        Generate the Airflow DAG code that implements this pipeline.
        """

        input_variables = ["pipeline_code"]
        formatted_params = {"pipeline_code": parameters.get("pipeline_code", "")}

        return {
            "template": template,
            "input_variables": input_variables,
            "formatted_params": formatted_params
        }

    def _generate_dlt_pipeline_prompt(self, parameters: Dict[str, Any]) -> dict[
        str, str | list[str] | dict[str, str | Any]]:
        template = """
        Note: This template has been adjusted to prevent unintentional extraction of `id`, `limit`, and `type` as template variables. Double curly braces `{{ }}` are used around these placeholders to treat them as literal strings.

        Create a data pipeline using the dlt (data load tool) library to extract data from a REST API and load it into Azure Blob Storage (using the 'filesystem' destination in dlt).

        Source details:
        - Base URL: {base_url}
        - Endpoints:
        {endpoints}
        - Authentication:
        {auth}
        - Pagination:
        {pagination}
        - Headers:
        {headers}

        Destination details:
        - Type: {destination_type}
        - Configuration: {destination_details}

        Pipeline name: {pipeline_name}
        Dataset name: {dataset_name}
        Schedule: {schedule}
        Source name: {source_name}

        Include the following components in your dlt pipeline:

        1. **Import Necessary Modules**:
           Import dlt modules and any additional libraries.

           ```python
           import dlt
           from dlt.sources.rest_api import rest_api_source, BearerTokenAuth, typing
           ```

        2. **Define the REST API Source Configuration**:
           - Create a source using `dlt.sources.rest_api.rest_api_source`.
           - Configure the client with:
             - `base_url`: Set the base URL for the API.
             - `auth`: Use `BearerTokenAuth`, `HTTPBasicAuth`, or `APIKeyAuth` for authentication.
             - `headers`: Optional additional headers.
             - `paginator`: Configure the paginator based on the API's pagination mechanism.
           - Define resources for each API endpoint:
             - Specify `name`, `endpoint` path, and any required `params`.
             - Configure `write_disposition` (e.g., `append`, `merge`) and define `primary_key` for each resource.
             - Set up resource relationships using the `resolve` field to link parent-child parameters.
           - Handle pagination using built-in paginator classes (e.g., `PageNumberPaginator`).
           - Optional: Use `processing_steps` for data transformations or filtering.

           **Example:**
           ```python
           @dlt.source
           def {source_name}(api_token: str = dlt.secrets.value):
                return rest_api_source(
                    name="{pipeline_name}",
                    config={{
                        "client": {{
                            "base_url": "{base_url}",
                            "auth": BearerTokenAuth(api_token),
                            "headers": {headers},
                            "paginator": {pagination},
                        }},
                        "resources": [
                            {{
                                "name": "resource1",
                                "endpoint": {{
                                    "path": "resource1",
                                    "params": {{"limit": 100}},
                                }},
                                "primary_key": "id",
                                "write_disposition": "merge",
                                "incremental": {{
                                    "cursor_path": "updated_at"
                                }}
                            }},
                            {{
                                "name": "related_resource",
                                "endpoint": {{
                                    "path": "resource1/{{{{id}}}}/related",
                                    "params": {{
                                        "id": {{
                                            "type": "resolve",
                                            "resource": "resource1",
                                            "field": "id"
                                        }}
                                    }},
                                }},
                                "include_from_parent": ["name"],
                            }},
                        ],
                    }}
                )
           ```

        3. **Create the Main Pipeline Function**:
           Use `dlt.pipeline()` to create the pipeline, configure the destination, and specify the dataset name.

           ```python
           def load_{pipeline_name}() -> None:
               pipeline = dlt.pipeline(
                   pipeline_name="{pipeline_name}",
                   destination="{destination_type}",
                   dataset_name="{dataset_name}",
               )
               load_info = pipeline.run({source_name}())
               print(load_info)
           ```

        4. **Main Execution Block**:
           ```python
           if __name__ == "__main__":
               load_{pipeline_name}()
           ```

        5. **Helper Functions and Configuration**:
           Include any helper functions or additional configurations as necessary.

        **Additional Considerations**:
        - Store sensitive information (e.g., API tokens) in `.dlt/secrets.toml`.
        - Configure schema contracts and incremental loading as needed.
        - Use `response_actions` for handling specific response codes or modifying response content.

        **Example configuration for `.dlt/secrets.toml`:**
        ```toml
        [destination.filesystem]
        bucket_url = "az://your-container-name"

        [destination.filesystem.credentials]
        azure_storage_account_name = "your_account_name"
        azure_storage_account_key = "your_account_key"
        ```

        For more information on configuring the filesystem destination, refer to the [dlt Filesystem Destination Documentation](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem).
        """

        endpoints_str = "\n".join([f"  - {e['name']}: {e['path']} (Method: {e['method']})" for e in
                                   parameters['source']['config']['endpoints']])

        input_variables = [
            "base_url", "endpoints", "auth", "pagination", "headers",
            "destination_type", "destination_details",
            "pipeline_name", "dataset_name",
            "schedule"
        ]

        paginator_params = parameters['source']['config'].get('pagination', {})
        paginator_config = DLTConfigGenerator.generate_paginator_config(paginator_params)

        client_config = {
            "base_url": parameters['source']['config']['base_url'],
            "headers": str(parameters['source']['config'].get('headers', 'None')),
            "paginator": paginator_config
        }

        auth_params = parameters['source']['config']
        client_config = DLTConfigGenerator.add_authentication_config(client_config, auth_params)

        formatted_params = {
            "base_url": parameters['source']['config']['base_url'],
            "endpoints": endpoints_str,
            "auth": (
                None if parameters['source']['config'].get('auth', {}).get('type', 'none') == 'none'
                else parameters['source']['config'].get('auth')
            ),
            "pagination": paginator_config,
            "headers": str(parameters['source']['config'].get('headers', {})),
            "client": client_config,
            "destination_type": parameters['destination']['type'],
            "destination_details": str(parameters['destination']['config']),
            "pipeline_name": parameters['pipeline_name'],
            "dataset_name": parameters['dataset_name'],
            "schedule": parameters.get('schedule', 'Not specified'),
            "source_name": parameters['source'].get('name', 'api_source')
        }

        print("Formatted Params:", formatted_params)

        print("Generated Template:", template)  # Log the template to inspect variables

        return {
            "template": template,
            "input_variables": input_variables,
            "formatted_params": formatted_params
        }


class DLTConfigGenerator:
    @staticmethod
    def generate_paginator_config(paginator_params):
        if isinstance(paginator_params, str):
            # Return the paginator as a dictionary if it's a simple string type
            return {"type": paginator_params}

        if isinstance(paginator_params, dict):
            paginator_type = paginator_params.get('type', 'offset')
            config_items = {
                "type": paginator_type
            }

            # Add pagination-specific configurations based on the paginator type
            config_map = {
                'offset': [
                    ('limit_param', 'limit'),
                    ('offset_param', 'offset'),
                    ('limit', 'limit')
                ],
                'page_number': [
                    ('page_param', 'page'),
                    ('total_path', 'total_pages')
                ],
                'link': [
                    ('next_url_path', 'next_page_url')
                ],
                'json_link': [
                    ('next_url_path ', 'next_page_url')
                ]

            }

            # Add the pagination parameters to the config_items dictionary
            for param, default in config_map.get(paginator_type, []):
                value = paginator_params.get(param, default)
                config_items[param] = value

            return config_items  # Return the paginator configuration as a dictionary

        # If no valid paginator is provided, return an empty dictionary
        return {}

    @staticmethod
    def add_authentication_config(client_config, auth_params):
        auth_type = auth_params.get('auth', {}).get('type', 'none')
        if auth_type != 'none':
            client_config['auth'] = {
                "type": auth_type,
                "token": "api_token"  # Replace with actual value or method to retrieve the token
            }
        return client_config
