from dataforge_ai.core.plugin_interface import PluginInterface
from langchain.agents import Tool, AgentExecutor, create_react_agent
from langchain.schema import AgentAction, AgentFinish
from langchain_core.prompts import PromptTemplate
from typing import List, Union, Dict, Any
import re


class ReactAdapter(PluginInterface):
    def __init__(self, llm, prompt_generator, pipeline_generator, dag_converter):
        super().__init__()
        self.llm = llm
        self.prompt_generator = prompt_generator
        self.pipeline_generator = pipeline_generator
        self.dag_converter = dag_converter
        self.tools = self._define_tools()
        self.prompt = self._create_prompt()
        self.output_parser = CustomOutputParser()
        self.agent = create_react_agent(self.llm, self.tools, self.prompt)
        self.agent_executor = AgentExecutor.from_agent_and_tools(
            agent=self.agent, tools=self.tools, verbose=True
        )

    def _define_tools(self) -> List[Tool]:
        """Define the tools available to the agent."""
        return [
            Tool(
                name="Generate Pipeline",
                func=self.pipeline_generator.execute,
                description="Generates a dlt pipeline based on given configuration."
            ),
            Tool(
                name="Convert to Airflow DAG",
                func=self.dag_converter.execute,
                description="Converts a dlt pipeline to an Airflow DAG for deployment."
            )
        ]

    def _create_prompt(self) -> PromptTemplate:
        """Create the prompt template for the agent."""
        base_prompt = self.prompt_generator.execute({
            "prompt_type": "react_agent",
            "parameters": {
                "task_description": "Generate a data pipeline and convert it to an Airflow DAG",
                "available_tools": [tool.name for tool in self.tools]
            }
        })

        full_prompt = f"""
        {base_prompt}

        Human: You are a data engineer tasked with generating a data pipeline and converting it to an Airflow DAG.
        You have access to the following tools:

        {{tools}}

        Use the following format:

        Question: the input question you must answer
        Thought: you should always think about what to do
        Action: the action to take, should be one of [{{tool_names}}]
        Action Input: the input to the action
        Observation: the result of the action
        ... (this Thought/Action/Action Input/Observation can repeat N times)
        Thought: I now know the final answer
        Final Answer: the final answer to the original input question

        Begin!

        Question: {{input}}
        Thought: Let's approach this step-by-step:
        {{agent_scratchpad}}
        """

        return PromptTemplate.from_template(full_prompt)

    def execute(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the ReAct reasoning process to generate a data pipeline and convert it to an Airflow DAG.

        :param input_data: A dictionary containing the configuration for data pipeline generation.
        :return: A dictionary containing the generated pipeline code and Airflow DAG.
        """
        self.log_execution("Starting ReAct reasoning process")
        if not self.validate_input(input_data):
            raise ValueError("Invalid input data")

        result = self.agent_executor.invoke(input_data)
        self.log_execution("ReAct reasoning process completed")

        # Extract the relevant information from the result
        pipeline_code = result.get('pipeline_code', 'No pipeline code generated')
        airflow_dag = result.get('airflow_dag', 'No Airflow DAG generated')

        return {
            "pipeline_code": pipeline_code,
            "airflow_dag": airflow_dag
        }

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        """
        Validate the input data for the plugin.

        :param input_data: A dictionary containing the input data for the plugin.
        :return: True if the input is valid, False otherwise.
        """
        required_keys = ['source', 'destination']
        if not all(key in input_data for key in required_keys):
            self.log_execution("Input validation failed: missing required keys", level="error")
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
                        "config": {"type": "object"}
                    },
                    "required": ["type", "config"]
                },
                "destination": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["azure_blob"]},
                        "config": {"type": "object"}
                    },
                    "required": ["type", "config"]
                }
            },
            "required": ["source", "destination"]
        }

    def get_output_schema(self) -> Dict[str, Any]:
        """
        Get the schema for the output data produced by the plugin.

        :return: A dictionary representing the output schema.
        """
        return {
            "type": "object",
            "properties": {
                "pipeline_code": {"type": "string"},
                "airflow_dag": {"type": "string"}
            },
            "required": ["pipeline_code", "airflow_dag"]
        }


class CustomOutputParser:
    def parse(self, llm_output: str) -> Union[AgentAction, AgentFinish]:
        if "Final Answer:" in llm_output:
            return AgentFinish(
                return_values={"output": llm_output.split("Final Answer:")[-1].strip()},
                log=llm_output,
            )

        regex = r"Action: (.*?)[\n]*Action Input:[\s]*(.*)"
        match = re.search(regex, llm_output, re.DOTALL)
        if not match:
            raise ValueError(f"Could not parse LLM output: `{llm_output}`")
        action = match.group(1).strip()
        action_input = match.group(2)
        return AgentAction(tool=action, tool_input=action_input.strip(" ").strip('"'), log=llm_output)
