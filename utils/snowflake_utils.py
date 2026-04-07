"""
Enhanced snowflake_utils.py - Backward Compatible

This version maintains all existing functionality for:
- Warehouses
- Databases  
- Users
- Roles

And adds NEW functionality for:
- RoleGrants (via stored procedure)

No breaking changes - all existing drift detection continues to work!
"""

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import logging
from typing import List, Dict, Any
from components import dependencies 

# Initialize logger
logger = dependencies.setup_logging()
logger = logging.getLogger('app.snowflake_utils')


def get_snowflake_resources(resource: str, attributes: List[str], sql_query: str, host: str, 
                            account_name: str, user_name: str, private_key: str, 
                            snow_warehouse: str, snow_role: str, snow_db: str) -> List[Dict[str, Any]]:
    """
    Query Snowflake to retrieve resources based on a provided SQL query and extract specified attributes.
    
    This function handles both standard resources (Warehouse, Database, User, Role) and 
    special resources (RoleGrant via stored procedure).

    Args:
        resource (str): Name of the resource being queried (e.g., 'Warehouse', 'Database', 'User', 'Role', 'RoleGrant').
        attributes (List[str]): List of attribute names to extract from the query results.
        sql_query (str): SQL query to execute (or 'CALL_PROCEDURE' for RoleGrant).
        host (str): Snowflake host URL.
        account_name (str): Snowflake account identifier.
        user_name (str): Username for Snowflake authentication.
        private_key (str): PEM-encoded private key for Snowflake authentication.
        snow_warehouse (str): Name of the Snowflake warehouse to use.
        snow_role (str): Role to assume in Snowflake for the query.
        snow_db (str): Name of the Snowflake database to query.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries with requested attributes and values.
    """
    # Special handling for RoleGrant resource using stored procedure
    if resource == "RoleGrant":
        logger.info(f"Detected RoleGrant resource - using stored procedure instead of standard SQL")
        return get_role_user_grants_via_procedure(
            host, account_name, user_name, private_key, 
            snow_warehouse, snow_role, snow_db, attributes
        )
    
    # Standard implementation for all other resources (Warehouse, Database, User, Role)
    # This code remains UNCHANGED from your original file
    try:
        # Validate input parameters
        string_params = [resource, sql_query, host, account_name, user_name, private_key, snow_warehouse, snow_role, snow_db]
        if not all(isinstance(param, str) and param.strip() for param in string_params):
            logger.error("All string parameters must be non-empty strings")
            return None
        if not isinstance(attributes, list) or not attributes:
            logger.error("attributes must be a non-empty list of strings")
            return None
        if not all(isinstance(attr, str) and attr.strip() for attr in attributes):
            logger.error("All attributes must be non-empty strings")
            return None

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
            return None
            

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
            while cursor.nextset():
                rows = cursor.fetchall()
        else:
            cursor.execute(sql_query)
            rows = cursor.fetchall()

        # Get column names from query results
        columns = [col[0].upper() for col in cursor.description]
        col_index = {col: idx for idx, col in enumerate(columns)}

        # Validate attributes in query results
        for attr in attributes:
            if attr.upper() not in col_index:
                logger.error(f"Attribute '{attr}' not found in Snowflake query results for {resource}")
                return None

        # Build list of dictionaries with requested attributes
        resources = []
        for row in rows:
            resource_dict = {}
            for attr in attributes:
                value = row[col_index[attr.upper()]]
                resource_dict[attr] = value
            resources.append(resource_dict)

        return resources

    except ValueError as ve:
        logger.error(f"Invalid input - {ve}")        
        return None
    except snowflake.connector.errors.DatabaseError as db_error:
        logger.error(f"Snowflake database error for {resource} - {db_error}")        
        return None
    except Exception as e:
        logger.error(f"Failed to query Snowflake for {resource} - {e}")
        return None
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def get_role_user_grants_via_procedure(host: str, account_name: str, user_name: str, 
                                       private_key: str, snow_warehouse: str, snow_role: str, 
                                       snow_db: str, attributes: List[str]) -> List[Dict[str, Any]]:
    """
    NEW FUNCTION: Retrieve role and user grants using stored procedure.
    
    This is only called when resource == "RoleGrant".
    Does not affect existing Warehouse, Database, User, or Role drift detection.
    
    Retrieves:
    - Role grants (role → role assignments)
    - User grants (role → user assignments)
    
    The stored procedure must be located at: mydatabase.jump.get_all_grants_of_roles()
    
    Args:
        host (str): Snowflake host URL.
        account_name (str): Snowflake account identifier.
        user_name (str): Username for Snowflake authentication.
        private_key (str): PEM-encoded private key for Snowflake authentication.
        snow_warehouse (str): Name of the Snowflake warehouse to use.
        snow_role (str): Role to assume in Snowflake (must have permission to call procedure).
        snow_db (str): Name of the Snowflake database.
        attributes (List[str]): List of attribute names to extract (e.g., ['role_name', 'grantee_name', 'granted_to']).

    Returns:
        List[Dict[str, Any]]: A list of role/user grant dictionaries, or None on error.
    """
    try:
        logger.info(f"Retrieving role and user grants via stored procedure for account {account_name}")
        
        # Load and serialize the private key
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
        
        # Establish connection to Snowflake
        conn = snowflake.connector.connect(
            host=host,
            user=user_name,
            private_key=pkb,
            account=account_name,
            warehouse=snow_warehouse,
            role=snow_role,
            database=snow_db
        )
        
        cursor = conn.cursor()
        
        # Call the stored procedure using fully-qualified name
        logger.info("Calling stored procedure: mydatabase.jump.get_all_grants_of_roles()")
        cursor.execute("CALL mydatabase.jump.get_all_grants_of_roles()")
        
        # Fetch all results
        rows = cursor.fetchall()
        
        # Get column names from the result
        columns = [col[0].upper() for col in cursor.description]
        col_index = {col: idx for idx, col in enumerate(columns)}
        
        logger.info(f"Stored procedure returned columns: {columns}")
        logger.info(f"Retrieved {len(rows)} role and user grant records")
        
        # Count grants by type for logging
        if 'GRANTED_TO' in col_index:
            role_grants = sum(1 for row in rows if row[col_index['GRANTED_TO']] == 'ROLE')
            user_grants = sum(1 for row in rows if row[col_index['GRANTED_TO']] == 'USER')
            logger.info(f"Grant breakdown: {role_grants} role grants, {user_grants} user grants")
        
        # Map stored procedure columns to expected attributes
        # Procedure should return: role_name, grantee_name, granted_to
        column_mapping = {
            'ROLE_NAME': 'role_name',
            'GRANTEE_NAME': 'grantee_name',
            'GRANTED_TO': 'granted_to'
        }
        
        # Build list of grant dictionaries
        grants = []
        for row in rows:
            grant_dict = {}
            
            # Map each column from the procedure to the expected attribute names
            for proc_col, attr_name in column_mapping.items():
                if proc_col in col_index:
                    grant_dict[attr_name] = row[col_index[proc_col]]
                else:
                    logger.warning(f"Column '{proc_col}' not found in procedure results")
            
            # Ensure we have all required fields
            if 'role_name' in grant_dict and 'grantee_name' in grant_dict and 'granted_to' in grant_dict:
                grants.append(grant_dict)
            else:
                logger.warning(f"Skipping incomplete grant record: {grant_dict}")
        
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully processed {len(grants)} role and user grants")
        return grants
        
    except snowflake.connector.errors.ProgrammingError as prog_error:
        logger.error(f"Stored procedure error - {prog_error}")
        logger.error("Ensure get_all_grants_of_roles() procedure exists in mydatabase.jump schema")
        logger.error("Ensure your role has USAGE on database mydatabase and schema jump")
        logger.error("Ensure your role has EXECUTE on the procedure")
        logger.error("The procedure must return 3 columns: role_name, grantee_name, granted_to")
        return None
    except Exception as e:
        logger.error(f"Failed to retrieve grants via procedure - {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
