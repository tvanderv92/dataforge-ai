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
        Create a data pipeline using the dlt (data load tool) library that extracts data from a REST API 
        and loads it into Azure Blob Storage (using the 'filesystem' destination in dlt).
        
        Source details:
        - Base URL: {base_url}
        - Endpoints:
        {endpoints}
        - Authentication: {auth}
        - Pagination: {pagination}
        - Headers: {headers}
        
        Destination details:
        - Type: filesystem (for Azure Blob Storage)
        - Details: Configure in .dlt/secrets.toml
        
        Pipeline name: {pipeline_name}
        Dataset name: {dataset_name}
        
        Please include the following components in your dlt pipeline:
        
        1. Import necessary dlt modules and any additional required libraries.
           Example:
           ```python
           import dlt
           from dlt.sources.rest_api import rest_api_source, rest_api_resources
           ```
    
        2. Define the REST API source configuration using RESTAPIConfig:
            - Create the Source: Use `dlt.sources.rest_api.rest_api_source` and configure the client and resources.
            - Client Setup:
                - Include `base_url` and `auth` using built-in classes like `BearerTokenAuth`, `HTTPBasicAuth`, or `APIKeyAuth`.
                    - Optional: Include additional `headers` or other client settings as needed.
            - Resource Configuration: Define resources for each API endpoint:
                - Set `name`, `endpoint` path, and required parameters.
                - Configure `write_disposition` (`append`, `merge`, `replace`) and define `primary_key` if needed.
                    - Optionally, set up `incremental` loading using `cursor_path`.
            - Establish resource relationships using the `resolve` field to link parameters between parent and child resources.
            - Pagination: Use built-in paginator classes like `PageNumberPaginator` or `OffsetPaginator` to handle paginated responses.
                - Optional Data Processing: Use `processing_steps` to apply data transformations or filtering if needed.

           Example:
           ```python
           @dlt.source
           def pokemon_source(api_token: str = dlt.secrets.value):
               return rest_api_source(
                   name="pokemon_api",
                   config=dict(
                       client=dict(
                           base_url="{base_url}",
                           auth=dict(type="bearer", token=api_token),
                           headers={headers},
                       ),
                       pagination={pagination},
                       resources=[
                           # Define your resources here
                       ]
                   )
               )
           ```
    
        3. Create the main pipeline function:
           - Include error handling and logging
           
           Example:
           ```python
           def load_github() -> None:
            pipeline = dlt.pipeline(
                pipeline_name="rest_api_github",
                destination="duckdb",
                dataset_name="rest_api_data",
            )
        
            load_info = pipeline.run(github_source())
            print(load_info)
           ```
    
        4. Set up the dlt pipeline:
           - Use dlt.pipeline() to create the pipeline instance
           - Configure the destination as 'filesystem' for Azure Blob Storage
           - Set the dataset name
           Note: Further configuration of the destination should be done in .dlt/secrets.toml
    
        5. Implement the main execution block:
           - Create the pipeline instance
           - Use pipeline.run() to execute the pipeline with the REST API source
           - Print or log the load info
           
           Example:
           ```python
           if __name__ == "__main__":
               load_pokemon_data()
           ```
    
        6. Include any necessary helper functions or additional configuration
           See https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic for more examples.
    
        7. Add appropriate type hints and docstrings
    
        8. Use best practices for dlt pipeline development, such as:
           - Proper error handling and logging
           - Using dlt.secrets for sensitive information
           - Configuring schema contracts if needed
           - Setting up incremental loading where appropriate
        
        Generate a complete Python script that implements this dlt pipeline.
        Ensure the script follows dlt best practices and is ready for production use.
        Use dlt.secrets for sensitive information and set up incremental loading where appropriate.
    
        Important: Remember to configure Azure Blob Storage credentials in .dlt/secrets.toml file.
        Example configuration for .dlt/secrets.toml:
        ```toml
        [destination.filesystem]
        bucket_url = "az://your-container-name"
    
        [destination.filesystem.credentials]
        azure_storage_account_name = "your_account_name"
        azure_storage_account_key = "your_account_key"
        ```
    
        For more information on configuring the filesystem destination for Azure Blob Storage, 
        refer to: https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem
        """

        endpoints_str = "\n".join([f"  - {e['name']}: {e['path']} (Method: {e['method']})" for e in
                                   parameters['source']['config']['endpoints']])

        input_variables = [
            "base_url", "endpoints", "auth", "pagination", "headers",
            "destination_type", "destination_details",
            "pipeline_name", "dataset_name",
            "schedule"
        ]

        formatted_params = {
            "base_url": parameters['source']['config']['base_url'],
            "endpoints": endpoints_str,
            "auth": str(parameters['source']['config']['auth']),
            "pagination": str(parameters['source']['config']['pagination']),
            "headers": str(parameters['source']['config']['headers']),
            "destination_type": parameters['destination']['type'],
            "destination_details": str(parameters['destination']['config']),
            "pipeline_name": parameters['pipeline_name'],
            "dataset_name": parameters['dataset_name'],
            "schedule": parameters['schedule']
        }

        return {
            "template": template,
            "input_variables": input_variables,
            "formatted_params": formatted_params
        }
