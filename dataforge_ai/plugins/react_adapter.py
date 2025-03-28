import json

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
        return [
            Tool(
                name="Generate Pipeline",
                func=self.pipeline_generator.execute,
                description="Generates a dlt pipeline based on given configuration."
            )
            # ,
            # Tool(
            #     name="Convert to Airflow DAG",
            #     func=self.dag_converter.execute,
            #     description="Converts a dlt pipeline to an Airflow DAG for deployment."
            # )
        ]

    def _create_prompt(self) -> PromptTemplate:
        base_prompt = self.prompt_generator.execute({
            "prompt_type": "react_agent",
            "parameters": {
                "task_description": "Generate a dlt pipeline for the given API configuration",
                "available_tools": [tool.name for tool in self.tools]
            }
        })

        full_prompt = f"""
        {base_prompt}

        Human: You are a data engineer tasked with generating a data pipeline using the dlt library:

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
        {{agent_scratchpad}}
        """

        return PromptTemplate(template=full_prompt,
                              input_variables=["tools", "tool_names", "input", "agent_scratchpad"])

    def execute(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        self.log_execution("Starting ReAct reasoning process")
        if not self.validate_input(input_data):
            raise ValueError("Invalid input data")

        agent_input = {
            "input": json.dumps(input_data),
            "tools": "\n".join([f"- {tool.name}: {tool.description}" for tool in self.tools]),
            "tool_names": ", ".join([tool.name for tool in self.tools])
        }

        try:
            self.agent_executor = AgentExecutor.from_agent_and_tools(
                agent=self.agent, tools=self.tools, verbose=True, handle_parsing_errors=False
            )
            result = self.agent_executor.invoke(agent_input)
            self.log_execution("ReAct reasoning process completed")

            output = result.get('output', '')

            # Extract code from the output
            code = self._extract_code(output)

            return {
                "output": output,
                "pipeline_code": code,
                # "airflow_dag": code  # Since the pipeline is integrated into the DAG
            }
        except Exception as e:
            self.log_execution(f"Error in ReAct reasoning process: {str(e)}", level="error")
            raise

    def _extract_code(self, text: str, marker: str = "") -> str:
        # Find the marker if provided, otherwise start from the beginning
        start = text.find(marker) + 1 if marker and (start := text.find(marker)) != -1 else 0
        if marker and start == 0:
            return "No code found"

        # Locate code block boundaries
        code_start = text.find("```", start)
        if code_start == -1:
            return "No code block found"

        code_start = text.find("\n", code_start) + 1
        code_end = text.find("```", code_start) if text.find("```", code_start) != -1 else len(text)

        # Extract and return the cleaned code block
        return text[code_start:code_end].strip()

    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        required_keys = ['source', 'destination']
        if not all(key in input_data for key in required_keys):
            self.log_execution("Input validation failed: missing required keys", level="error")
            return False
        return True

    def get_input_schema(self) -> Dict[str, Any]:
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
        return {
            "type": "object",
            "properties": {
                "pipeline_code": {"type": "string"},
                # "airflow_dag": {"type": "string"}
            },
            "required": ["pipeline_code"
                         # , "airflow_dag"
                         ]
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
