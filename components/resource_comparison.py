"""
Enhanced resource_comparison.py with role grant comparison support

This extends the original to handle RoleGrant resources from stored procedure results.
"""

import logging
from typing import Dict, List, Any, Set
from components.dependencies import setup_logging

# Initialize logger
logger = setup_logging()
logger = logging.getLogger('app.resource_comparison')


def compare_resources(tf_state: Dict[str, Any], sf_resources: List[Dict[str, Any]], 
                     resource: str, attributes: List[str], 
                     synonyms: Dict[str, Dict[str, Dict[str, str]]]) -> List[Dict[str, Any]]:
    """
    Compare Terraform state with Snowflake resources for a specified resource type and attributes.
    
    Routes to specialized comparison for RoleGrant, otherwise uses standard comparison.

    Args:
        tf_state (Dict[str, Any]): The Terraform state data.
        sf_resources (List[Dict[str, Any]]): List of resource dictionaries from Snowflake.
        resource (str): The type of resource to compare (e.g., 'user', 'warehouse', 'RoleGrant').
        attributes (List[str]): List of attribute names to compare.
        synonyms (Dict[str, Dict[str, Dict[str, str]]]): Nested dictionary of synonyms for attributes.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries describing detected drifts.
    """
    # Special handling for RoleGrant
    if resource == "RoleGrant":
        return compare_role_grants(tf_state, sf_resources)
    
    # Standard comparison for other resources
    return compare_standard_resources(tf_state, sf_resources, resource, attributes, synonyms)


def compare_role_grants(tf_state: Dict[str, Any], 
                       sf_grants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Compare Terraform role grants with Snowflake role grants from stored procedure.
    
    This function handles the comparison of role hierarchy grants (FR → UFR).
    
    Terraform represents role grants through:
    - snowflake_role_grants: Grants a role to other roles/users
    - snowflake_grant_account_role: Individual role grant to user/role
    
    Snowflake stored procedure returns:
    - role_name: The role being granted
    - grantee_name: Who receives the grant (role or user)
    - granted_to: Type ('ROLE' or 'USER')

    Args:
        tf_state (Dict[str, Any]): The Terraform state data.
        sf_grants (List[Dict[str, Any]]): List of role grants from Snowflake stored procedure.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries describing detected drifts.
    """
    try:
        logger.info("Comparing role grants between Terraform and Snowflake")
        
        # Validate inputs
        if not isinstance(tf_state, dict):
            logger.error("tf_state must be a dictionary")
            return None
        if not isinstance(sf_grants, list):
            logger.error("sf_grants must be a list of dictionaries")
            return None
        
        drifts = []
        
        # Extract role grants from Terraform state
        tf_role_grants = extract_terraform_role_grants(tf_state)
        logger.info(f"Found {len(tf_role_grants)} role grants in Terraform state")
        
        # Extract role grants from Snowflake (your stored procedure results)
        sf_role_grants = extract_snowflake_role_grants(sf_grants)
        logger.info(f"Found {len(sf_role_grants)} role grants in Snowflake")
        
        # Create sets of grant keys for comparison
        # Format: "ROLE_NAME|GRANTED_TO|GRANTEE_NAME"
        tf_grant_keys = create_grant_keys(tf_role_grants)
        sf_grant_keys = create_grant_keys(sf_role_grants)
        
        logger.info(f"Terraform grant keys: {len(tf_grant_keys)}")
        logger.info(f"Snowflake grant keys: {len(sf_grant_keys)}")
        
        # Find grants in Snowflake but not in Terraform (unauthorized or manual grants)
        sf_only = sf_grant_keys - tf_grant_keys
        for key in sf_only:
            parts = key.split('|')
            role, granted_to, grantee = parts[0], parts[1], parts[2]
            
            drifts.append({
                'resource': 'snowflake_role_grant',
                'role': role,
                'granted_to': granted_to,
                'grantee_name': grantee,
                'Snowflake': True,
                'Terraform': False,
                'drift_type': 'missing_in_terraform',
                'description': f"Role '{role}' is granted to {granted_to} '{grantee}' in Snowflake but not defined in Terraform"
            })
        
        # Find grants in Terraform but not in Snowflake (not applied)
        tf_only = tf_grant_keys - sf_grant_keys
        for key in tf_only:
            parts = key.split('|')
            role, granted_to, grantee = parts[0], parts[1], parts[2]
            
            drifts.append({
                'resource': 'snowflake_role_grant',
                'role': role,
                'granted_to': granted_to,
                'grantee_name': grantee,
                'Snowflake': False,
                'Terraform': True,
                'drift_type': 'missing_in_snowflake',
                'description': f"Role '{role}' is granted to {granted_to} '{grantee}' in Terraform but not found in Snowflake"
            })
        
        # Log summary
        logger.info(f"Role grant drift summary:")
        logger.info(f"  - Total drifts detected: {len(drifts)}")
        logger.info(f"  - Grants in Snowflake only: {len(sf_only)}")
        logger.info(f"  - Grants in Terraform only: {len(tf_only)}")
        
        return drifts
        
    except Exception as e:
        logger.error(f"Failed to compare role grants: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def extract_terraform_role_grants(tf_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract role grants from Terraform state.
    
    Terraform uses two resource types for role grants:
    1. snowflake_role_grants - Grants a role to multiple roles/users
    2. snowflake_grant_account_role - Individual role grant
    
    Args:
        tf_state: Terraform state dictionary

    Returns:
        List of role grant dictionaries with format:
        {'role': 'ROLE_NAME', 'granted_to': 'ROLE'|'USER', 'grantee_name': 'GRANTEE'}
    """
    grants = []
    
    for resource in tf_state.get("resources", []):
        resource_type = resource.get("type", "")
        
        # Handle snowflake_role_grants (multiple grants from one role)
        if resource_type == "snowflake_role_grants":
            for instance in resource.get("instances", []):
                attrs = instance.get("attributes", {})
                role_name = attrs.get("role_name")
                
                if not role_name:
                    continue
                
                # Grants to other roles
                for parent_role in attrs.get("roles", []):
                    grants.append({
                        'role': role_name,
                        'granted_to': 'ROLE',
                        'grantee_name': parent_role
                    })
                
                # Grants to users
                for user in attrs.get("users", []):
                    grants.append({
                        'role': role_name,
                        'granted_to': 'USER',
                        'grantee_name': user
                    })
        
        # Handle snowflake_grant_account_role (individual grants)
        elif resource_type == "snowflake_grant_account_role":
            for instance in resource.get("instances", []):
                attrs = instance.get("attributes", {})
                role_name = attrs.get("role_name")
                
                if not role_name:
                    continue
                
                # Grant to user
                user_name = attrs.get("user_name")
                if user_name:
                    grants.append({
                        'role': role_name,
                        'granted_to': 'USER',
                        'grantee_name': user_name
                    })
                
                # Grant to role
                parent_role = attrs.get("parent_role_name")
                if parent_role:
                    grants.append({
                        'role': role_name,
                        'granted_to': 'ROLE',
                        'grantee_name': parent_role
                    })
    
    logger.debug(f"Extracted {len(grants)} grants from Terraform state")
    return grants


def extract_snowflake_role_grants(sf_grants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract and normalize role grants from Snowflake stored procedure results.
    
    Your stored procedure returns:
    - role_name (or 'role'): The role being granted
    - grantee_name: Who receives the grant
    - granted_to: 'ROLE' or 'USER' (may be inferred)
    
    Args:
        sf_grants: List of dictionaries from stored procedure

    Returns:
        Normalized list of role grant dictionaries
    """
    normalized_grants = []
    
    for grant in sf_grants:
        # Handle different possible key names from stored procedure
        role = grant.get('role') or grant.get('role_name')
        grantee = grant.get('grantee_name') or grant.get('grant_name')
        granted_to = grant.get('granted_to', 'ROLE')  # Default to ROLE if not specified
        
        if not role or not grantee:
            logger.warning(f"Skipping invalid grant record: {grant}")
            continue
        
        normalized_grants.append({
            'role': role,
            'granted_to': granted_to,
            'grantee_name': grantee
        })
    
    logger.debug(f"Normalized {len(normalized_grants)} grants from Snowflake")
    return normalized_grants


def create_grant_keys(grants: List[Dict[str, Any]]) -> Set[str]:
    """
    Create unique keys for role grants to enable set-based comparison.
    
    Format: "ROLE|GRANTED_TO|GRANTEE"
    Example: "ANALYTICS_READ_FR|ROLE|ANALYTICS_READ_UFR"
    
    Args:
        grants: List of grant dictionaries

    Returns:
        Set of unique grant keys
    """
    keys = set()
    
    for grant in grants:
        role = str(grant.get('role', '')).upper()
        granted_to = str(grant.get('granted_to', '')).upper()
        grantee = str(grant.get('grantee_name', '')).upper()
        
        # Create unique key
        key = f"{role}|{granted_to}|{grantee}"
        keys.add(key)
    
    return keys


def compare_standard_resources(tf_state: Dict[str, Any], sf_resources: List[Dict[str, Any]], 
                               resource: str, attributes: List[str], 
                               synonyms: Dict[str, Dict[str, Dict[str, str]]]) -> List[Dict[str, Any]]:
    """
    Standard resource comparison (original logic from resource_comparison.py).
    
    This handles Warehouse, Database, User, Role resources.

    Args:
        tf_state (Dict[str, Any]): The Terraform state data.
        sf_resources (List[Dict[str, Any]]): List of resource dictionaries from Snowflake.
        resource (str): The type of resource to compare (e.g., 'user', 'warehouse').
        attributes (List[str]): List of attribute names to compare.
        synonyms (Dict[str, Dict[str, Dict[str, str]]]): Nested dictionary of synonyms for attributes.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries describing detected drifts.
    """
    try:
        # Validate input parameters
        if not isinstance(tf_state, dict):
            logger.error("tf_state must be a dictionary")
            return None
        if not isinstance(sf_resources, list):
            logger.error("sf_resources must be a list of dictionaries")
            return None
        if not isinstance(resource, str) or not resource.strip():
            logger.error("resource must be a non-empty string")
            return None
        if not isinstance(attributes, list) or not attributes or not all(isinstance(attr, str) and attr.strip() for attr in attributes):
            logger.error("attributes must be a non-empty list of non-empty strings")
            return None
        if not isinstance(synonyms, dict):
            logger.error("synonyms must be a dictionary")
            return None

        drifts = []
        tf_resource_type = f"snowflake_{resource.lower()}"

        # Extract resources from Terraform state
        tf_resources = []
        for res in tf_state.get("resources", []):
            if res["type"] == tf_resource_type:
                tf_resources.extend([instance["attributes"] for instance in res["instances"]])

        # Create dictionaries for comparison
        if tf_resource_type == "snowflake_user":
            tf_res_map = {res["login_name"]: res for res in tf_resources}
            sf_res_map = {res["login_name"]: res for res in sf_resources}  
        else:
            tf_res_map = {res["name"]: res for res in tf_resources}
            sf_res_map = {res["name"]: res for res in sf_resources}

        # Compare resources
        for res_name in set(tf_res_map.keys()) | set(sf_res_map.keys()):
            if res_name not in tf_res_map:
                drifts.append({
                    "resource": tf_resource_type,
                    "name": res_name,
                    "Snowflake": True,
                    "Terraform": False,
                    "drift_type": "missing_in_terraform"
                })
            elif res_name not in sf_res_map:
                drifts.append({
                    "resource": tf_resource_type,
                    "name": res_name,
                    "Snowflake": False,
                    "Terraform": True,
                    "drift_type": "missing_in_snowflake"
                })
            else:
                tf_res = tf_res_map[res_name]
                sf_res = sf_res_map[res_name]
                for attr in attributes:
                    tf_value = tf_res.get(attr)
                    sf_value = sf_res.get(attr)
                    tf_value_str = str(tf_value).upper() if tf_value is not None else "NULL"
                    sf_value_str = str(sf_value).upper() if sf_value is not None else "NULL"
                    synonyms_for_attr = synonyms.get(resource, {}).get(attr.lower(), {})
                    tf_synonym = synonyms_for_attr.get(tf_value_str, tf_value_str)
                    sf_synonym = synonyms_for_attr.get(sf_value_str, sf_value_str)
                    if tf_synonym != sf_synonym:
                        drifts.append({
                            "resource": tf_resource_type,
                            "name": res_name,
                            "attribute": attr,
                            "Snowflake": sf_value_str,
                            "Terraform": tf_value_str,
                            "drift_type": "attribute_mismatch"
                        })

        return drifts

    except ValueError as ve:
        logger.error(f"Invalid input - {ve}")
        return None
    except Exception as e:
        logger.error(f"Failed to compare resources for {resource} - {e}")
        return None
