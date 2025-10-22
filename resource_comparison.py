import logging
from typing import Dict, List, Any
from dependencies import setup_logging

# Initialize logger
logger = setup_logging()
logger = logging.getLogger('app.resource_comparison')

def compare_resources(tf_state: Dict[str, Any], sf_resources: List[Dict[str, Any]], resource: str, attributes: List[str], synonyms: Dict[str, Dict[str, Dict[str, str]]]) -> List[Dict[str, Any]]:
    """
    Compare Terraform state with Snowflake resources for a specified resource type and attributes.

    Args:
        tf_state (Dict[str, Any]): The Terraform state data.
        sf_resources (List[Dict[str, Any]]): List of resource dictionaries from Snowflake.
        resource (str): The type of resource to compare (e.g., 'user', 'warehouse').
        attributes (List[str]): List of attribute names to compare.
        synonyms (Dict[str, Dict[str, Dict[str, str]]]): Nested dictionary of synonyms for attributes.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries describing detected drifts.

    Raises:
        ValueError: If inputs are invalid.
        RuntimeError: For unexpected errors during comparison.
    """
    try:
        # Validate input parameters
        if not isinstance(tf_state, dict):
            raise ValueError("tf_state must be a dictionary")
        if not isinstance(sf_resources, list):
            raise ValueError("sf_resources must be a list of dictionaries")
        if not isinstance(resource, str) or not resource.strip():
            raise ValueError("resource must be a non-empty string")
        if not isinstance(attributes, list) or not attributes or not all(isinstance(attr, str) and attr.strip() for attr in attributes):
            raise ValueError("attributes must be a non-empty list of non-empty strings")
        if not isinstance(synonyms, dict):
            raise ValueError("synonyms must be a dictionary")

        drifts = []
        tf_resource_type = f"snowflake_{resource.lower()}"

        # Extract resources from Terraform state
        tf_resources = []
        for res in tf_state.get("resources", []):
            if not isinstance(res, dict) or "type" not in res or "instances" not in res:
                continue
            if res["type"] == tf_resource_type:
                tf_resources.extend([instance["attributes"] for instance in res["instances"] if isinstance(instance, dict) and "attributes" in instance])

        # Create dictionaries for comparison
        key_field = "login_name" if tf_resource_type == "snowflake_user" else "name"
        tf_res_map = {res[key_field]: res for res in tf_resources if key_field in res}
        sf_res_map = {res[key_field]: res for res in sf_resources if key_field in res}

        # Compare resources
        for res_name in set(tf_res_map.keys()) | set(sf_res_map.keys()):
            if res_name not in tf_res_map:
                drifts.append({
                    "resource": tf_resource_type,
                    "name": res_name,
                    "Snowflake": True,
                    "Terraform": False
                })
            elif res_name not in sf_res_map:
                drifts.append({
                    "resource": tf_resource_type,
                    "name": res_name,
                    "Snowflake": False,
                    "Terraform": True
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
                            "Terraform": tf_value_str
                        })

        return drifts

    except ValueError as ve:
        logger.error(f"Invalid input - {ve}")
        raise RuntimeError(f"Invalid input: {ve}")
    except Exception as e:
        logger.error(f"Failed to compare resources for {resource} - {e}")
        raise RuntimeError(f"Failed to compare resources for {resource}: {e}")