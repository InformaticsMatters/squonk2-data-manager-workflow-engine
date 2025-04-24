"""A module to validate and decode workflow definitions.

This is typically used by the Data Manager's Workflow Engine.
"""

import os
from typing import Any

import jsonschema
import yaml

# The (built-in) schemas...
# from the same directory as us.
_WORKFLOW_SCHEMA_FILE: str = os.path.join(
    os.path.dirname(__file__), "workflow-schema.yaml"
)

# Load the Workflow schema YAML file now.
# This must work as the file is installed along with this module.
assert os.path.isfile(_WORKFLOW_SCHEMA_FILE)
with open(_WORKFLOW_SCHEMA_FILE, "r", encoding="utf8") as schema_file:
    _WORKFLOW_SCHEMA: dict[str, Any] = yaml.load(schema_file, Loader=yaml.FullLoader)
assert _WORKFLOW_SCHEMA


def validate_schema(workflow: dict[str, Any]) -> str | None:
    """Checks the Workflow Definition against the built-in schema.
    If there's an error the error text is returned, otherwise None.
    """
    assert isinstance(workflow, dict)

    try:
        jsonschema.validate(workflow, schema=_WORKFLOW_SCHEMA)
    except jsonschema.ValidationError as ex:
        return str(ex.message)

    # OK if we get here
    return None


def get_step_names(definition: dict[str, Any]) -> list[str]:
    """Given a Workflow definition this function returns the list of
    step names, in the order they are defined.
    """
    names: list[str] = [step["name"] for step in definition.get("steps", [])]
    return names


def get_steps(definition: dict[str, Any]) -> list[dict[str, Any]]:
    """Given a Workflow definition this function returns the steps."""
    response: list[dict[str, Any]] = definition.get("steps", [])
    return response


def get_name(definition: dict[str, Any]) -> str:
    """Given a Workflow definition this function returns its name."""
    return str(definition.get("name", ""))


def get_description(definition: dict[str, Any]) -> str | None:
    """Given a Workflow definition this function returns its description (if it has one)."""
    return definition.get("description")


def get_variable_names(definition: dict[str, Any]) -> list[str]:
    """Given a Workflow definition this function returns all the names of the
    variables defined at the workflow level. These are the 'names' for inputs,
    outputs and options. This function DOES NOT de-duplicate names,
    that is the role of the validator."""
    wf_variable_names: list[str] = []
    variables: dict[str, Any] | None = definition.get("variables")
    if variables:
        wf_variable_names.extend(
            input_variable["name"] for input_variable in variables.get("inputs", [])
        )
        wf_variable_names.extend(
            output_variable["name"] for output_variable in variables.get("outputs", [])
        )
        wf_variable_names.extend(
            option_variable["name"] for option_variable in variables.get("options", [])
        )
    return wf_variable_names


def set_variables_from_options_for_step(
    definition: dict[str, Any], variables: dict[str, Any], step_name: str
) -> tuple[dict[str, Any], str | None]:
    """Given a Workflow definition, an existing map of variables and values,
    and a step name this function returns a new set of variables by adding
    variables and values that are required for the step that have been defined in the
    workflow's variables->options block.

    As an example, the following option, which is used if the step name is 'step1',
    expects 'rdkitPropertyName' to exist in the current set of variables,
    and should be copied into the new set of variables using the key 'propertyName'
    and value that is the same as the one provided in the original 'rdkitPropertyName': -

        name: rdkitPropertyName
        default: propertyName
        as:
        - option: propertyName
          step: step1

    And ... in the above example ... if the input variables map
    is {"rdkitPropertyName": "rings"} then the output map would be
    {"rdkitPropertyName": "rings", "propertyName": "rings"}

    The function returns a new variable map, with and an optional error string on error.
    """

    assert isinstance(definition, dict)
    assert isinstance(variables, dict)
    assert step_name

    new_variables: dict[str, Any] = variables.copy()

    # Success...
    return new_variables, None


def get_required_variable_names(definition: dict[str, Any]) -> list[str]:
    """Given a Workflow definition this function returns all the names of the
    variables that are required to be defined when it is RUN - i.e.
    all those the user needs to provide."""
    required_variables: list[str] = []
    variables: dict[str, Any] | None = definition.get("variables")
    if variables:
        # All inputs are required (no defaults atm)...
        required_variables.extend(
            input_variable["name"] for input_variable in variables.get("inputs", [])
        )
        # Options without defaults are required...
        # It is the role of the engine to provide the actual default for those
        # that have defaults but no user-defined value.
        required_variables.extend(
            option_variable["name"]
            for option_variable in variables.get("options", [])
            if "default" not in option_variable
        )
    return required_variables
