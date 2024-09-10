from dataforge_ai.core.plugin_interface import PluginInterface
from typing import Dict, Any
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
            max_tokens=32768
        )

    def execute(self, input_data: Dict[str, Any]) -> str:
        self.log_execution("Starting prompt generation")
        if not self.validate_input(input_data):
            raise ValueError("Invalid input data")

        prompt_type = input_data.get('prompt_type')
        parameters = input_data.get('parameters', {})

        prompt_generators = {
            'data_pipeline': self._generate_data_pipeline_prompt,
            'code_explanation': self._generate_code_explanation_prompt,
            'react_agent': self._generate_react_agent_prompt,
            'airflow_dag': self._generate_airflow_dag_prompt,
            'dlt_pipeline': self._generate_dlt_pipeline_prompt
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
                                "enum": ["data_pipeline", "code_explanation", "react_agent", "airflow_dag",
                                         "dlt_pipeline"]},
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

    def _generate_data_pipeline_prompt(self, parameters: Dict[str, Any]) -> str:
        template = """
        Create a data pipeline that extracts data from {source_type} and loads it into {destination_type}.
        The source details are: {source_details}
        The destination details are: {destination_details}
        Additional requirements: {requirements}

        Generate a Python script to implement this pipeline.
        Include error handling, logging, and best practices for data pipeline development.
        """
        formatted_params = {
            "source_type": parameters['source']['type'],
            "destination_type": parameters['destination']['type'],
            "source_details": str(parameters['source']['config']),
            "destination_details": str(parameters['destination']['config']),
            "requirements": f"Pipeline name: {parameters['pipeline_name']}, Schedule: {parameters['schedule']}"
        }
        return self._create_chain(template, list(formatted_params.keys())).invoke(formatted_params)

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

    def _generate_airflow_dag_prompt(self, parameters: Dict[str, Any]) -> str:
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
        chain = self._create_chain(template, ["pipeline_code"])
        return chain.invoke(parameters)

    def _generate_dlt_pipeline_prompt(self, parameters: Dict[str, Any]) -> str:
        template = """
        Create a data pipeline using the dlt (data load tool) library that extracts data from a REST API 
        and loads it into Azure Data Lake Storage Gen2.
    
        Source details:
        - Base URL: {base_url}
        - Endpoints:
        {endpoints}
        - Authentication: {auth}
        - Pagination: {pagination}
        - Headers: {headers}
    
        Destination details:
        - Storage Account: {storage_account}
        - File System: {file_system}
        - Directory: {directory}
        - Credentials: {credentials}
    
        Pipeline name: {pipeline_name}
        Schedule: {schedule}
    
        Please include the following components in your dlt pipeline:
    
        1. Import necessary dlt modules
        2. Define the pipeline using @dlt.pipeline decorator
        3. Create functions to load data from each endpoint using dlt.source decorator
        4. Implement pagination as specified
        5. Handle authentication as required
        6. Use dlt.run() to execute the pipeline
        7. Configure the destination using dlt.destinations.adls2()
        8. Implement proper error handling and logging
        9. Use best practices for dlt pipeline development
    
        Generate a complete Python script that implements this dlt pipeline.
        Ensure the script can be scheduled using the provided cron expression.
        """

        endpoints_str = "\n".join([f"  - {e['name']}: {e['path']} (Method: {e['method']})" for e in
                                   parameters['source']['config']['endpoints']])

        chain = self._create_chain(template, [
            "base_url", "endpoints", "auth", "pagination", "headers",
            "storage_account", "file_system", "directory", "credentials",
            "pipeline_name", "schedule"
        ])

        return chain.invoke({
            "base_url": parameters['source']['config']['base_url'],
            "endpoints": endpoints_str,
            "auth": str(parameters['source']['config']['auth']),
            "pagination": str(parameters['source']['config']['pagination']),
            "headers": str(parameters['source']['config']['headers']),
            "storage_account": parameters['destination']['config']['account_name'],
            "file_system": parameters['destination']['config']['container_name'],
            "directory": parameters['destination']['config']['folder_path'],
            "credentials": f"account_key={parameters['destination']['config']['account_key']}",
            "pipeline_name": parameters['pipeline_name'],
            "schedule": parameters['schedule']
        })
