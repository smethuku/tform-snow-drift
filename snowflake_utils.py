import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import logging
from typing import List, Dict, Any
from dependencies import setup_logging

# Initialize logger
logger = setup_logging()
logger = logging.getLogger('app.snowflake_utils')

def get_snowflake_resources(resource: str, attributes: List[str], sql_query: str, host: str, account_name: str, user_name: str, private_key: str, snow_warehouse: str, snow_role: str, snow_db: str) -> List[Dict[str, Any]]:
    """
    Query Snowflake to retrieve resources based on a provided SQL query and extract specified attributes.

    Args:
        resource (str): Name of the resource being queried (e.g., table or view name).
        attributes (List[str]): List of attribute names to extract from the query results.
        sql_query (str): SQL query to execute against the Snowflake database.
        host (str): Snowflake host URL.
        account_name (str): Snowflake account identifier.
        user_name (str): Username for Snowflake authentication.
        private_key (str): PEM-encoded private key for Snowflake authentication.
        snow_warehouse (str): Name of the Snowflake warehouse to use.
        snow_role (str): Role to assume in Snowflake for the query.
        snow_db (str): Name of the Snowflake database to query.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries with requested attributes and values.

    Raises:
        ValueError: If inputs are invalid.
        RuntimeError: If Snowflake connection or query fails.
    """
    try:
        # Validate input parameters
        string_params = [resource, sql_query, host, account_name, user_name, private_key, snow_warehouse, snow_role, snow_db]
        if not all(isinstance(param, str) and param.strip() for param in string_params):
            raise ValueError("All string parameters must be non-empty strings")
        if not isinstance(attributes, list) or not attributes:
            raise ValueError("attributes must be a non-empty list of strings")
        if not all(isinstance(attr, str) and attr.strip() for attr in attributes):
            raise ValueError("All attributes must be non-empty strings")

        # Load and serialize the private key
        try:
            p_key = serialization.load_pem_private_key(
                private_key.encode(),
                password=None,
                backend=default_backend()
            )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        except Exception as key_error:
            logger.error(f"Invalid private key format - {key_error}")
            raise ValueError(f"Invalid private key format: {key_error}")

        # Establish connection to Snowflake
        conn = snowflake.connector.connect(
            host=host,
            user=user_name,
            private_key=pkb,
            account=account_name,
            warehouse=snow_warehouse,
            role=snow_role,
            database=snow_db,
            session_parameters={'MULTI_STATEMENT_COUNT': '0'}
        )

        # Create a cursor for executing queries
        cursor = conn.cursor()

        # Execute the SQL query
        if sql_query.strip().upper().startswith("SHOW"):
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            while cursor.nextset():
                rows.extend(cursor.fetchall())
        else:
            cursor.execute(sql_query)
            rows = cursor.fetchall()

        # Get column names from query results
        columns = [col[0].upper() for col in cursor.description]
        col_index = {col: idx for idx, col in enumerate(columns)}

        # Validate attributes in query results
        for attr in attributes:
            if attr.upper() not in col_index:
                raise RuntimeError(f"Attribute '{attr}' not found in Snowflake query results for {resource}")

        # Build list of dictionaries with requested attributes
        resources = []
        for row in rows:
            resource_dict = {attr: row[col_index[attr.upper()]] for attr in attributes}
            resources.append(resource_dict)

        return resources

    except ValueError as ve:
        logger.error(f"Invalid input - {ve}")
        raise RuntimeError(f"Invalid input: {ve}")
    except snowflake.connector.errors.DatabaseError as db_error:
        logger.error(f"Snowflake database error for {resource} - {db_error}")
        raise RuntimeError(f"Snowflake database error for {resource}: {db_error}")
    except Exception as e:
        logger.error(f"Failed to query Snowflake for {resource} - {e}")
        raise RuntimeError(f"Failed to query Snowflake for {resource}: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
