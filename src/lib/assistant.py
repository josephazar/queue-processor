from openai import AzureOpenAI
import openai
from openai.types.beta import Thread
from openai.types.beta.threads import Run, Message
from .function import Function, FunctionCall
from openai.types.beta.threads.run_create_params import TruncationStrategy
import json
import time


class AIAssistant:
    def __init__(
        self,
        client: AzureOpenAI,
        verbose: bool = False,
        name: str = "AI Assistant",
        description: str = "An AI Assistant",
        instructions: str = None,
        model: str = None,
        tools: list[dict] = None,
        functions: list[Function] = None,
        auto_delete: bool = True,
        assistant_id: str = None,  # Added parameter to support loading existing assistant
    ):
        self.client = client
        self.verbose = verbose
        self.threads = []
        self.functions = functions or []
        self.name = name
        self.description = description
        self.instructions = instructions
        self.model = model
        self.tools = tools
        self.auto_delete = auto_delete
        self.assistant_id = assistant_id

        # Either load existing assistant or create a new one
        try:
            if self.assistant_id:
                # Load existing assistant by ID
                if self.verbose:
                    print(f"Loading existing assistant with ID: {self.assistant_id}")
                self.assistant = self.client.beta.assistants.retrieve(self.assistant_id)
                # Ensure we update the object's assistant_id field to match
                self.assistant_id = self.assistant.id
            else:
                # Create a new assistant
                if self.verbose:
                    print(f"Creating a new assistant: {self.name}")
                self.assistant = self.client.beta.assistants.create(
                    name=self.name,
                    description=self.description,
                    instructions=self.instructions,
                    model=self.model,
                    tools=self.tools,
                    tool_resources={"code_interpreter": {"file_ids": []}},
                    temperature=0.01
                )
                # Store the newly created assistant ID
                self.assistant_id = self.assistant.id

        except openai.BadRequestError as e:
            print(f"Error creating/retrieving assistant: {e}")
            print(f"Request data: {e.param}")
            raise

    def create_thread(self) -> Thread:
        thread = self.client.beta.threads.create()
        self.threads.append(thread)
        return thread

    def get_required_functions_names(self, run: Run):
        function_names = []
        for tool in run.required_action.submit_tool_outputs.tool_calls:
            function_names.append(tool.function)
        return function_names

    def create_tool_outputs(self, run: Run, functions: list[Function] = None) -> list[dict]:
        # Use provided functions or fall back to instance functions
        functions_to_use = functions or self.functions
        
        tool_outputs = []
        arguments = []
        for tool in run.required_action.submit_tool_outputs.tool_calls:
            tool_found = False
            function_name = tool.function.name
            if tool.function.arguments:
                function_arguments = json.loads(tool.function.arguments)
            else:
                function_arguments = {}
            call_id = tool.id
            function_call = FunctionCall(
                call_id=call_id, name=function_name, arguments=function_arguments
            )
            for function in functions_to_use:
                if function.name == function_name:
                    tool_found = True
                    if self.verbose:
                        print(
                            f"\n{function_name} function has called by assistant with the following arguments: {function_arguments}"
                        )
                    response = function.run_catch_exceptions(
                        function_call=function_call
                    )
                    if self.verbose:
                        print(f"Function {function_name} responded: {response}")
                    tool_outputs.append(
                        {
                            "tool_call_id": call_id,
                            "output": response,
                        }
                    )
                    arguments.append(
                        {
                            "tool_call_name": function_name,
                            "arguments": function_arguments,
                        }
                    )

            if not tool_found:
                if self.verbose:
                    print(f"Function {function_name} called by assistant not found")
                tool_outputs.append(
                    {
                        "tool_call_id": call_id,
                        "output": f"Function {function_name} not found",
                    }
                )
        return tool_outputs, arguments

    def create_file(self, filename: str, file_id: str):
        content = self.client.files.retrieve_content(file_id)
        with open(filename.split("/")[-1], "w") as file:
            file.write(content)

    def format_message(self, message: Message) -> str:
        if getattr(message.content[0], "text", None) is not None:
            message_content = message.content[0].text
        else:
            message_content = message.content[0]
        annotations = message_content.annotations
        citations = []
        for index, annotation in enumerate(annotations):
            message_content.value = message_content.value.replace(
                annotation.text, f" [{index}]"
            )
            if file_citation := getattr(annotation, "file_citation", None):
                cited_file = self.client.files.retrieve(file_citation.file_id)
                citations.append(
                    f"[{index}] {file_citation.quote} from {cited_file.filename}"
                )
            elif file_path := getattr(annotation, "file_path", None):
                cited_file = self.client.files.retrieve(file_path.file_id)
                citations.append(f"[{index}] file: {cited_file.filename} is downloaded")
                self.create_file(filename=cited_file.filename, file_id=cited_file.id)

        message_content.value += "\n" + "\n".join(citations)
        return message_content.value

    def extract_run_message(
        self, run: Run, thread_id: str, output_role: bool = True
    ) -> str:
        messages = self.client.beta.threads.messages.list(
            thread_id=thread_id,
        ).data
        for message in messages:
            if message.run_id == run.id:
                return (
                    #f"{message.role}: " + self.format_message(message=message)
                    self.format_message(message=message)
                    if output_role
                    else self.format_message(message=message)
                )
        return "No message found"

    def extract_query(self, arguments: list[dict]) -> str:
        """Extract the last SQL query from the arguments"""
        queries = []
        for argument in arguments:
            if argument["tool_call_name"] == "run_sql_query":
                queries.append(f"{argument['arguments']['query']}")
            else:
                queries.append(argument["tool_call_name"])
        if not queries:
            return ""
        else:
            return queries[-1]

    def create_response(
        self,
        question: str,
        thread_id: str = None,
        run_instructions: str = None,
        max_retries: int = 5,
        retry_delay: int = 20,
    ) -> dict:

        if thread_id is None:
            thread = self.create_thread()
            thread_id = thread.id

        self.client.beta.threads.messages.create(
            thread_id=thread_id, role="user", content=question
        )

        retries = 0

        while retries < max_retries:
            run = self.client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=self.assistant_id,  # Use assistant_id here
                instructions=run_instructions,
                truncation_strategy=TruncationStrategy(
                    type="last_messages", 
                    last_messages=3,
                ),
            )
            arguments = []

            while run.status not in ["completed", "failed"]:
                run = self.client.beta.threads.runs.retrieve(
                    thread_id=thread_id, run_id=run.id
                )
                if run.status == "expired":
                    raise Exception(
                        f"Run expired when calling {self.get_required_functions_names(run=run)}"
                    )
                if run.status == "requires_action":
                    tool_outputs, arguments = self.create_tool_outputs(
                        run=run, functions=self.functions
                    )
                    run = self.client.beta.threads.runs.submit_tool_outputs(
                        thread_id=thread_id,
                        run_id=run.id,
                        tool_outputs=tool_outputs,
                    )
                time.sleep(0.5)

            if run.status == "failed":
                retries += 1
                print(
                    f"Run failed. Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})"
                )
                time.sleep(retry_delay)
            else:
                tokens = {
                    "prompt_tokens": run.usage.prompt_tokens,
                    "completion_tokens": run.usage.completion_tokens,
                }
                return {
                    "answer": self.extract_run_message(run=run, thread_id=thread_id),
                    "context": self.extract_query(arguments),
                    "total_tokens": tokens,
                }
        
        # If we've exhausted all retries
        raise Exception(f"Failed to get a response after {max_retries} attempts")

    def create_response_sync(
        self,
        thread_id: str,
        content: str,
        run_instructions: str = None,
    ) -> str:
        """
        Synchronous version of create_response that returns just the text response.
        """
        result_dict = self.create_response(
            question=content,
            thread_id=thread_id,
            run_instructions=run_instructions
        )
        return result_dict["answer"]

    def chat(self, file_ids: list[str] = None):
        thread = self.create_thread()
        user_input = ""
        while user_input != "bye" and user_input != "exit":
            user_input = input("\033[32m Please, input your ask (or bye to exit) : ")
            response = self.create_response(question=user_input, thread_id=thread.id)
            message = response["answer"]
            context = response["context"]
            tokens = response["total_tokens"]
            print(f"\033[33m{message}")
            print(f"\033[33m{context}")
            print(f"\033[33m{tokens}")

        if self.auto_delete:
            if file_ids:
                for file in file_ids:
                    self.delete_file(file_id=file)
            self.client.beta.threads.delete(thread_id=thread.id)
            self.client.beta.assistants.delete(assistant_id=self.assistant_id)

    def create_message(self, thread_id: str, role: str, question: str):
        self.client.beta.threads.messages.create(
            thread_id=thread_id, role="user", content=question
        )