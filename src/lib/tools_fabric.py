# tools_fabric.py
import pyodbc
import os
from .function import Function, Property
from .config import FabricConfig 
import chromadb
import json
from openai import AzureOpenAI
import instructor
from pydantic import BaseModel

class VerifiedQuery(BaseModel):
    correctedQuery: str
    read_only: bool

def verifyQuery(query, schema):
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
    )       

    system_prompt = """
    You are an SQL verification assistant. 
    You are given an SQL query and a schema of a database table. You focus on Fabric SQL databases syntax and semantics.
    You need to make sure that the query doesn't return more than 50 rows. If needed, you can modify the query to make it return less than 50 rows by adding TOP 50.
    you also need to make sure the query in only reading the data and not modifying it.
    """

    prompt = """
    Table/View Schema:
    {schema}
    
    SQL Query:
    {query}
    """.format(schema=schema, query=query)

    client = instructor.from_openai(client)

    system_message = {"role": "system", "content": 
                        system_prompt}
    
    
    response = client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_MODEL_NAME"),
        response_model=VerifiedQuery,
        messages=[system_message,{"role": "user", "content": prompt}],
        max_tokens=200,
    )

    return response

def get_connection_string(datasource_id):
    """
    Initializes FabricConfig for the given datasource_id and retrieves the connection string.
    """
    config = FabricConfig(datasource_id)
    return config.connection_string

class GetDBSchema(Function):
    def __init__(self):
        super().__init__(
            name="get_db_schema",
            description="Get the schema (columns, types) of a given view/table in the Fabric SQL database.",
            parameters=[
                Property(
                    name="view_name",
                    description="The view name (dbo) to get the schema for",
                    type="string",
                    required=True,
                ),
                Property(
                    name="datasource",
                    description="The datasource to get the schema for",
                    type="string",
                    required=True,
                )
            ],
        )
    def format_schema(self, table_data):
        """
        Formats the schema content into a structured string.
        """
        schema_info = f"Table Name: {table_data['table']}\n"
        schema_info += f"Description: {table_data['description']}\n"
        schema_info += f"Datasource: {table_data['datasource']}\n\nColumns:\n"

        for column in table_data.get("columns", []):
            col_name = column.get("name", "Unknown")
            col_desc = column.get("description", "No description available.")
            col_type = column.get("type", "Unknown type")
            schema_info += f"- {col_name} ({col_type}): {col_desc}\n"

        return schema_info
    
    def function(self, view_name, datasource):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        tables_folder = os.path.join(base_dir, "nl2sql/tables")
        if not os.path.exists(tables_folder):
            return f"Error: The tables directory '{tables_folder}' does not exist. It's not possible to get the schema."
        for filename in os.listdir(tables_folder):
            if filename.endswith(".json"):
                file_path = os.path.join(tables_folder, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as file:
                        table_data = json.load(file)

                    # Check if table matches the requested view_name and datasource
                    if table_data.get("table") == view_name and table_data.get("datasource") == datasource:
                        return self.format_schema(table_data)

                except json.JSONDecodeError:
                    return f"Error: Failed to parse JSON file '{filename}'."

        return f"Error: No schema found for view '{view_name}' with datasource '{datasource}'."
    



class ListViews(Function):
    def __init__(self):
        super().__init__(
            name="list_views",
            description="List the dbo views, datasets, and queries examples in the Fabric SQL database.",
            parameters= [
                Property(
                    name="query_text",
                    description="The query text to search for relevant tables. The search is based on semantic similarity. The query should be self-explanatory.", 
                    type="string",
                    required=True,
                ),
            ],
        )


    def function(self,query_text):
        """
        Fetch top N relevant tables based on semantic similarity search.
        """
        output = ""
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        chroma_db_path = os.path.join(base_dir, "chromadb")
        chroma_client = chromadb.PersistentClient(path=chroma_db_path)
        collection = chroma_client.get_or_create_collection(name="nl2sql-tables")
        client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_API_ENDPOINT"),
        )  
        query_embedding =  client.embeddings.create(input= query_text,model=os.getenv("AZURE_OPENAI_EMBEDDING_MODEL_NAME")).data[0].embedding
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=3,
            include=["documents", "metadatas"]
        )
        if not results["metadatas"]:
            return "No relevant tables found."
        tables_info = []
        for metadata, document in zip(results["metadatas"][0], results["documents"][0]):
            tables_info.append(f"Table: {metadata['table']}\nDatasource:{metadata['datasource']}\nDescription: {document}\n")

         
        output += "\n\n".join(tables_info)
        collection = chroma_client.get_or_create_collection(name="nl2sql-queries")
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=5,
            include=["documents", "metadatas"]
        )       
        queries_info = []
        for metadata, document in zip(results["metadatas"][0], results["documents"][0]):
            queries_info.append(f"Question: {document}\nQuery: {metadata['query']}\nReasoning: {metadata['reasoning']}\n")


        output += "\nQueries Examples:\n"
        output += "\n\n".join(queries_info)
        return output


class FetchDistinctValues(Function):
    def __init__(self):
        super().__init__(
            name="fetch_distinct_values",
            description="Fetch the first 50 distinct values of a specified column for a certain dbo view",
            parameters=[
                Property(
                    name="datasource",
                    description="The datasource to query",
                    type="string",
                    required=True,
                ),
                Property(
                    name="view_name",
                    description="The dbo view name to fetch distinct values from",
                    type="string",
                    required=True,
                ),
                Property(
                    name="column_name",
                    description="The column name to fetch distinct values",
                    type="string",
                    required=True,
                ),
            ],
        )

    def function(self, datasource, view_name, column_name):
        """
        Retrieves the top 10 most frequent distinct values in `column_name` from the specified dbo view,
        along with a count of how many times each appears.
        """
        try:
            conn = pyodbc.connect(get_connection_string(datasource))
            cursor = conn.cursor()

            # T-SQL uses TOP instead of LIMIT
            # We'll group by column_name, order by the count desc, and take top 10
            query = f"""
                SELECT [{column_name}], COUNT(*) AS qty
                FROM [dbo].[{view_name}]
                GROUP BY [{column_name}]
                ORDER BY qty DESC
                OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY;
            """
            # Alternatively, you could just do "SELECT TOP 10 ..." in T-SQL:
            #   SELECT TOP 10 [column_name], COUNT(*) AS qty
            #   FROM ...
            #   GROUP BY ...
            #   ORDER BY qty DESC

            cursor.execute(query)
            rows = cursor.fetchall()

            # Build column headers from cursor.description
            colnames = [desc[0] for desc in cursor.description]

            cursor.close()
            conn.close()

            # Format the output
            if not rows:
                return "No rows found."

            result = " | ".join(colnames) + "\n"
            for row in rows:
                result += " | ".join(map(str, row)) + "\n"
            return result

        except pyodbc.Error as e:
            # Let's check if the error message indicates invalid column or invalid object
            error_msg = str(e)

            # Common T-SQL error codes or messages:
            # - 208: "Invalid object name"
            # - 207: "Invalid column name"
            if "Invalid column name" in error_msg or "207" in error_msg:
                # Return existing columns in the view
                try:
                    conn2 = pyodbc.connect(get_connection_string(datasource))
                    cursor2 = conn2.cursor()
                    cursor2.execute(f"SELECT TOP 1 * FROM [dbo].[{view_name}]")
                    colnames = [desc[0] for desc in cursor2.description]
                    cursor2.close()
                    conn2.close()
                    return (
                        f"The column '{column_name}' does not exist in [dbo].[{view_name}]. "
                        + "The following columns exist:\n"
                        + " | ".join(colnames)
                    )
                except Exception as ex:
                    return f"Error: Column '{column_name}' not found. Additionally, could not fetch view columns ({ex})."
            elif "Invalid object name" in error_msg or "208" in error_msg:
                return f"View '{view_name}' does not exist or is invalid."
            else:
                return f"Error fetching distinct values: {e}"
            
class RunSQLQuery(Function):
    def __init__(self):
        super().__init__(
            name="run_sql_query",
            description="Run a SQL query on the Fabric SQL database",
            parameters=[
                Property(
                    name="datasource",
                    description="The datasource to run the query on",
                    type="string",
                    required=True,
                ),
                Property(
                    name="view_name",
                    description="The view name (dbo) to run the query on",
                    type="string",
                    required=True,
                ),
                Property(
                    name="query",
                    description="The SQL query to run",
                    type="string",
                    required=True,
                ),
            ],
        )
    def format_schema(self, table_data):
        """
        Formats the schema content into a structured string.
        """
        schema_info = f"Table Name: {table_data['table']}\n"
        schema_info += f"Description: {table_data['description']}\n"
        schema_info += f"Datasource: {table_data['datasource']}\n\nColumns:\n"

        for column in table_data.get("columns", []):
            col_name = column.get("name", "Unknown")
            col_desc = column.get("description", "No description available.")
            col_type = column.get("type", "Unknown type")
            schema_info += f"- {col_name} ({col_type}): {col_desc}\n"

        return schema_info
    def function(self,datasource, view_name, query):
        """
        Executes a SQL query and returns rows.
        """
        try:
            conn = pyodbc.connect(get_connection_string(datasource))
            cursor = conn.cursor()

            # get schema
            schema = ""
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
            tables_folder = os.path.join(base_dir, "nl2sql/tables")
            if not os.path.exists(tables_folder):
                return f"Error: The tables directory '{tables_folder}' does not exist. It's not possible to get the schema. The table name is incorrect."
            for filename in os.listdir(tables_folder):
                if filename.endswith(".json"):
                    file_path = os.path.join(tables_folder, filename)
                    try:
                        with open(file_path, "r", encoding="utf-8") as file:
                            table_data = json.load(file)

                        # Check if table matches the requested view_name and datasource
                        if table_data.get("table") == view_name and table_data.get("datasource") == datasource:
                            schema = self.format_schema(table_data)

                    except json.JSONDecodeError:
                        return f"Error: Failed to parse JSON file '{filename}'."
            if schema == "":
                return f"Error: No schema found for view '{view_name}' with datasource '{datasource}'."

            # verify query
            verified_query = verifyQuery(query, schema)
            print("Corrected Query: ", verified_query.correctedQuery)
            if verified_query.read_only == False:
                return "Error: The query is modifying the data. Please make sure the query is read-only."
            cursor.execute(verified_query.correctedQuery)
            results = cursor.fetchall()
            conn.close()

            if not results:
                return "No rows returned."
            
            return "\n".join([str(result) for result in results])

        except pyodbc.Error as e:
            # For demonstration, handle two typical errors:
            #   - Invalid object name (i.e. table or view doesn't exist)
            #   - Invalid column name
            # You can expand with further error handling as needed.

            error_code = e.args[0] if e.args else None
            error_msg = str(e)

            # Microsoft SQL Server "Invalid object name" is often 208
            if "Invalid object name" in error_msg or (error_code and "208" in error_code):
                return f"Error running query: {e}\nView {view_name} does not seem to exist."

            # Microsoft SQL Server "Invalid column name" is often 207
            if "Invalid column name" in error_msg or (error_code and "207" in error_code):
                # Return which columns actually exist
                try:
                    conn = pyodbc.connect(get_connection_string(datasource))
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT TOP 1 * FROM [dbo].[{view_name}]")
                    colnames = [desc[0] for desc in cursor.description]
                    return (
                        f"A column in your query doesn't exist.\n"
                        f"These columns exist in [dbo].[{view_name}]:\n"
                        + " | ".join(colnames)
                    )
                except Exception as col_err:
                    return f"Failed to retrieve columns from view {view_name}: {col_err}"
                finally:
                    conn.close()

            # Return the error message if it's not one of the expected errors
            return f"Error running query: {e}"
