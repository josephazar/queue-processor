# tools_fabric.py
import pyodbc
from .function import Function, Property
from .config import FabricConfig as config   
class GetDBSchema(Function):
    def __init__(self):
        super().__init__(
            name="get_db_schema",
            description="Get the schema (columns, types) of each dbo view in the Fabric SQL database and sample rows.",
        )

    def function(self):
        """
        Lists each dbo view, its columns and data types, and shows up to 3 sample rows for each view.
        """
        conn = pyodbc.connect(config().connection_string, autocommit=True)
        cursor = conn.cursor()

        # Step 1: Retrieve all dbo views
        cursor.execute(
            """
            SELECT v.name
            FROM sys.views AS v
            JOIN sys.schemas AS s ON v.schema_id = s.schema_id
            WHERE s.name = 'dbo'
            """
        )
        view_rows = cursor.fetchall()

        all_views_info = []

        for row in view_rows:
            view_name = row.name  # or row[0], depending on pyodbc's row structure

            # Step 2: Retrieve column info from INFORMATION_SCHEMA.COLUMNS
            col_cursor = conn.cursor()
            col_cursor.execute(
                """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """,
                (view_name,),
            )
            columns_info = col_cursor.fetchall()

            # Build a string with "column_name data_type(length) [NOT] NULL"
            columns_str_parts = []
            for col in columns_info:
                col_name = col.COLUMN_NAME
                data_type = col.DATA_TYPE
                max_len = col.CHARACTER_MAXIMUM_LENGTH
                is_nullable = col.IS_NULLABLE

                col_def = f"{col_name} {data_type}"
                # For character-based columns, show the length unless it's unlimited (-1 or None)
                if max_len and max_len != -1:
                    col_def += f"({max_len})"

                if is_nullable.upper() == "YES":
                    col_def += " NULL"
                else:
                    col_def += " NOT NULL"

                columns_str_parts.append(col_def)

            columns_block = "\n".join(columns_str_parts)

            # Step 3: Fetch up to 3 sample rows from the view
            try:
                sample_cursor = conn.cursor()
                sample_cursor.execute(f"SELECT TOP 3 * FROM [dbo].[{view_name}]")
                sample_rows = sample_cursor.fetchall()

                sample_col_names = [desc[0] for desc in sample_cursor.description]
                sample_col_names_str = "\t".join(sample_col_names)

                sample_rows_str = "\n".join(
                    ["\t".join(map(str, row)) for row in sample_rows]
                )
                if not sample_rows_str:
                    sample_rows_str = "(no rows)"

                sample_info = (
                    f"Up to 3 rows from view '{view_name}':\n{sample_col_names_str}\n{sample_rows_str}"
                )
            except pyodbc.Error as e:
                sample_info = f"Error fetching sample rows for '{view_name}': {e}"

            # Combine info for this view
            view_info = (
                f"VIEW NAME: {view_name}\n"
                f"COLUMNS:\n{columns_block}\n"
                f"{sample_info}"
            )
            all_views_info.append(view_info)

        conn.close()

        # Put everything into one large string
        # Separate each view's info with blank lines
        final_output = "\n\n".join(all_views_info)
        return final_output

class RunSQLQuery(Function):
    def __init__(self):
        super().__init__(
            name="run_sql_query",
            description="Run a SQL query on the Fabric SQL database",
            parameters=[
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

    def function(self, view_name, query):
        """
        Executes a SQL query and returns rows.
        """
        try:
            conn = pyodbc.connect(config().connection_string)
            cursor = conn.cursor()
            cursor.execute(query)
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
                    conn = pyodbc.connect(config().connection_string)
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

            # Otherwise: general fallback error
            # Let's list the available dbo views for context
            try:
                conn = pyodbc.connect(config().connection_string)
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT v.name
                    FROM sys.views AS v
                    JOIN sys.schemas AS s ON v.schema_id = s.schema_id
                    WHERE s.name = 'dbo'
                    """
                )
                all_views = [r.name for r in cursor.fetchall()]
                conn.close()

                return (
                    f"Error running query: {e}\nAvailable dbo views: {all_views}"
                )
            except Exception as fallback_err:
                return f"Error running query: {e}\nAdditionally, couldn't list views: {fallback_err}"

class ListViews(Function):
    def __init__(self):
        super().__init__(
            name="list_views",
            description="List the dbo views and datasets in the Fabric SQL database",
        )

    def function(self):
        """
        Lists all user-created views in the dbo schema.
        """
        try:
            conn = pyodbc.connect(config().connection_string)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT v.name
                FROM sys.views AS v
                JOIN sys.schemas AS s ON v.schema_id = s.schema_id
                WHERE s.name = 'dbo'
                """
            )
            views = cursor.fetchall()
            conn.close()

            view_names = [row.name for row in views]
            if not view_names:
                return "No dbo views found."
            return "\n".join(view_names)

        except pyodbc.Error as e:
            return f"Error listing views: {e}"

class FetchDistinctValues(Function):
    def __init__(self):
        super().__init__(
            name="fetch_distinct_values",
            description="Fetch the first 50 distinct values of a specified column for a certain dbo view",
            parameters=[
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

    def function(self, view_name, column_name):
        """
        Retrieves the top 10 most frequent distinct values in `column_name` from the specified dbo view,
        along with a count of how many times each appears.
        """
        try:
            conn = pyodbc.connect(config().connection_string)
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
                    conn2 = pyodbc.connect(config().connection_string)
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
            
