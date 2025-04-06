import os
from openai import AzureOpenAI
import sys

# Add the current directory to the path so lib can be found
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib.assistant import AIAssistant
import argparse
from lib.tools_fabric import (
    GetDBSchema as FabricGetDBSchema,
    RunSQLQuery as FabricRunSQLQuery,
    FetchDistinctValues as FabricFetchDistinctValues,
    ListViews as FabricListViews,
)
from lib.tools_search import FetchSimilarQueries


class SQLAssistant:
    def __init__(self, functions, instructions_file_name, assistant_id=None):
        self.functions = functions
        self.tools = [
            {"type": "function", "function": f.to_dict()} for f in self.functions
        ]
        self.tools.append({"type": "code_interpreter"})
        self.client = self.create_client()
        self.instructions_file_name = instructions_file_name
        self.instructions = self.load_instructions()
        self.model = os.getenv("AZURE_OPENAI_API_DEPLOYMENT")
        self.assistant_id = assistant_id
        self.assistant = self.create_assistant()

    def create_client(self):
        return AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
        )

    def load_instructions(self):
        instructions_path = os.path.join(
            os.path.dirname(__file__), "instructions", self.instructions_file_name
        )
        with open(instructions_path) as file:
            return file.read()

    def create_assistant(self):
        if self.assistant_id:
            # Load existing assistant if ID is provided
            print(f"Loading existing assistant with ID: {self.assistant_id}")
            # Create a minimal AIAssistant with just the assistant_id
            return AIAssistant(
                client=self.client,
                verbose=True,
                assistant_id=self.assistant_id,
                functions=self.functions,
            )
        else:
            # Create a new assistant
            print("Creating a new assistant")
            return AIAssistant(
                client=self.client,
                verbose=True,
                name="Insights HQ AI Assistant",
                description="Insights HQ AI Assistant",
                instructions=self.instructions,
                model=self.model,
                tools=self.tools,
                functions=self.functions,
            )

    def chat(self):
        self.assistant.chat()


# Create a method to initialize the assistant based on the database type
def initialize_assistant(database_type, assistant_id=None):
    """
    Initialize a SQLAssistant for the given database type.
    If assistant_id is provided, load the existing assistant instead of creating a new one.
    
    Args:
        database_type (str): The type of database to use ('fabric', 'postgresql', 'bigquery')
        assistant_id (str, optional): The ID of an existing assistant to load
        
    Returns:
        SQLAssistant: An initialized SQLAssistant instance
    """
    if database_type == "fabric":
        sql_functions = [
            FabricGetDBSchema(),
            FabricRunSQLQuery(),
            FabricFetchDistinctValues(),
            FabricListViews(),       
        ]
        instructions_file = "instructions_fabric.jinja2"
    else:
        raise ValueError(f"Unsupported database type: {database_type}")

    return SQLAssistant(sql_functions, instructions_file, assistant_id)


# Main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQL Assistant")
    parser.add_argument(
        "--database",
        choices=["postgresql", "bigquery", "fabric"],
        default="fabric",
        help="Specify the database type: 'postgresql' or 'bigquery' or 'fabric",
    )
    parser.add_argument(
        "--assistant-id",
        help="ID of an existing assistant to load instead of creating a new one",
    )
    args = parser.parse_args()
    
    sql_assistant = initialize_assistant(
        args.database, 
        assistant_id=args.assistant_id
    )
    sql_assistant.chat()