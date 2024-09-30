import re

from dataforge_ai.core.microkernel import Microkernel
from dataforge_ai.plugins.genai_prompt_generator import GenAIPromptGenerator
from dataforge_ai.plugins.react_adapter import ReactAdapter
from dataforge_ai.plugins.pipeline_generator import PipelineGeneratorPlugin
from dataforge_ai.plugins.airflow_dag_converter import AirflowDAGConverterPlugin
from langchain_openai import AzureChatOpenAI
import os
from dotenv import load_dotenv


def main():
    # Initialize the Microkernel
    load_dotenv()
    kernel = Microkernel()

    # Azure OpenAI configurations
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "https://dataforge-ai-llm.openai.azure.com/")
    azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
    azure_api_key = os.getenv("AZURE_OPENAI_API_KEY", "6df3196afed0447aba19ab2612d604a2")

    # Initialize LLM
    llm = AzureChatOpenAI(
        azure_endpoint=azure_endpoint,
        azure_deployment=azure_deployment,
        api_key=azure_api_key,
        api_version="2023-03-15-preview",
        temperature=0
    )

    # Initialize and register plugins
    prompt_generator = GenAIPromptGenerator(azure_endpoint, azure_deployment, azure_api_key)
    pipeline_generator = PipelineGeneratorPlugin(llm, prompt_generator)
    dag_converter = AirflowDAGConverterPlugin(llm, prompt_generator)
    react_reasoner = ReactAdapter(llm, prompt_generator, pipeline_generator, dag_converter)

    kernel.register_plugin("prompt_generator", prompt_generator)
    kernel.register_plugin("pipeline_generator", pipeline_generator)
    kernel.register_plugin("dag_converter", dag_converter)
    kernel.register_plugin("react_reasoner", react_reasoner)

    # Define pipeline configuration for Pokemon API
    pokemon_pipeline_config = {
        "source": {
            "type": "rest_api",
            "config": {
                "base_url": "https://pokeapi.co/api/v2",
                "endpoints": [
                    {
                        "name": "pokemon_list",
                        "path": "/pokemon",
                        "method": "GET",
                        "params": {
                            "limit": 100,
                            "offset": 0
                        }
                    },
                    {
                        "name": "pokemon_details",
                        "path": "/pokemon/{pokemon_name}",
                        "method": "GET"
                    }
                ],
                "auth": {
                    "type": "none"
                },
                "pagination": {
                    "type": "offset",
                    "limit_param": "limit",
                    "offset_param": "offset",
                    "total_count_path": "count"
                },
                "headers": {
                    "Accept": "application/json"
                }
            }
        },
        "destination": {
            "type": "azure_blob",
            "config": {
                "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
                "account_key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
                "container_name": "pokemon-data",
                "folder_path": "raw_data"
            }
        },
        "dataset_name": "pokemon_data",
        "pipeline_name": "pokemon_api_to_azure_blob",
        "schedule": "0 0 * * *"  # Daily at midnight (cron format)
    }

    # Execute the pipeline generation process
    result = kernel.execute_pipeline({
        "steps": [
            {"plugin": "react_reasoner", "input": pokemon_pipeline_config}
        ]
    })

    react_result = result.get("react_reasoner", {})
    full_output = react_result.get("output", "")

    # Extract pipeline code
    pipeline_code = extract_code_blocks(full_output)
    if not pipeline_code or pipeline_code == "No code found":
        pipeline_code = react_result.get("pipeline_code", "No pipeline code generated")

    # Extract Airflow DAG
    # airflow_dag = extract_code_block(full_output, "2. **Convert to Airflow DAG**:")
    # if not airflow_dag or airflow_dag == "No code found":
    #     airflow_dag = react_result.get("airflow_dag", "No Airflow DAG generated")

    print("Generated dlt Pipeline Code:")
    print(pipeline_code)
    # print("\nGenerated Airflow DAG:")
    # print(airflow_dag)

    # Save the generated code to files
    with open("generated_pokemon_pipeline.py", "w") as f:
        f.write(pipeline_code)

    # with open("generated_pokemon_dag.py", "w") as f:
    #     f.write(airflow_dag)


def extract_code_blocks(text):
    """
    Extracts all code blocks from the given text and returns them as a single combined string.

    Args:
        text (str): The text containing code blocks to extract.

    Returns:
        str: Combined extracted code blocks as a single string, or "No code found" if no code blocks are found.
    """
    # Regular expression to capture code blocks enclosed in triple backticks (```python ... ```).
    code_blocks = re.findall(r'```python(.*?)```', text, re.DOTALL)

    if not code_blocks:
        return "No code found"

    # Join all extracted code blocks with a newline separator for readability.
    combined_code = "\n\n".join(block.strip() for block in code_blocks)
    return combined_code


if __name__ == "__main__":
    main()
