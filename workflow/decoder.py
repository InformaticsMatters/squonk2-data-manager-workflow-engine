"""A module to validate and decode workflow definitions.

This is typically used by the Data Manager's Workflow Engine.
"""

import os
from pprint import pprint
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
    variables: dict[str, Any] | None = definition.get("variable-mapping")
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


def get_step_output_variable_names(
    definition: dict[str, Any], step_name: str
) -> list[str]:
    """Given a Workflow definition and a Step name this function returns all the names
    of the output variables defined at the Step level. This function DOES NOT
    de-duplicate names, that is the role of the validator."""
    variable_names: list[str] = []
    steps: list[dict[str, Any]] = get_steps(definition)
    for step in steps:
        if step["name"] == step_name:
            variable_names.extend(
                output["output"] for output in step.get("outputs", [])
            )
    return variable_names


def get_step_input_variable_names(
    definition: dict[str, Any], step_name: str
) -> list[str]:
    """Given a Workflow definition and a Step name (expected to exist)
    this function returns all the names of the input
    variables defined at the step level."""
    variable_names: list[str] = []
    steps: list[dict[str, Any]] = get_steps(definition)
    for step in steps:
        if step["name"] == step_name:
            variable_names.extend(input["input"] for input in step.get("inputs", []))
    return variable_names


def get_workflow_job_input_names_for_step(
    definition: dict[str, Any], name: str
) -> list[str]:
    """Given a Workflow definition and a step name we return a list of step Job input
    variable names the step expects. To do this we iterate through the step's
    inputs to find those that are declared 'from->workflow-input'."""
    inputs: list[str] = []
    for step in definition.get("steps", {}):
        if step["name"] == name and "inputs" in step:
            # Find all the workflow inputs.
            # This gives us the name of the workflow input variable
            # and the name of the step input (Job) variable.
            inputs.extend(
                step_input["input"]
                for step_input in step["inputs"]
                if "from" in step_input and "workflow-input" in step_input["from"]
            )
    return inputs


def workflow_step_has_outputs(definition: dict[str, Any], name: str) -> bool:
    """Given a Workflow definition and a step name we return a boolean
    that is true if the step produces outputs."""
    wf_outputs = definition.get("variable-mapping", {}).get("outputs", {})
    return any(
        "from" in output and "step" in output["from"] and output["from"]["step"] == name
        for output in wf_outputs
    )


def set_variables_from_options_for_step(
    definition: dict[str, Any], variables: dict[str, Any], step_name: str
) -> dict[str, Any]:
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
    assert step_name

    result = {}
    options = definition.get("variable-mapping", {}).get("options", [])

    for opt in options:
        for step_alias in opt["as"]:
            if step_alias["step"] == step_name:
                result[step_alias["option"]] = variables[opt["name"]]
                # can break the loop because a variable can be a step
                # variable only once
                break

    # Success...
    return result


def get_required_variable_names(definition: dict[str, Any]) -> list[str]:
    """Given a Workflow definition this function returns all the names of the
    variables that are required to be defined when it is RUN - i.e.
    all those the user needs to provide."""
    required_variables: list[str] = []
    variables: dict[str, Any] | None = definition.get("variable-mapping")
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


def set_step_variables(
    *,
    workflow: dict[str, Any],
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    step_outputs: dict[str, Any],
    previous_step_outputs: list[dict[str, Any]],
    workflow_variables: dict[str, Any],
    step_name: str,
) -> dict[str, Any]:
    """Prepare input- and output variables for the following step.

    Inputs are defined in step definition but their values may
    come from previous step outputs.
    """
    result = {}

    print("ssv: wf vars:")
    pprint(workflow_variables)
    print("ssv: inputs:")
    pprint(inputs)
    print("ssv: outputs", outputs)
    print("ssv: step_outputs", step_outputs)
    print("ssv: prev step outputs", previous_step_outputs)
    print("ssv: step_name", step_name)

    for item in inputs:
        p_key = item["input"]
        p_val = ""
        val = item["from"]
        if "workflow-input" in val.keys():
            p_val = workflow_variables[val["workflow-input"]]
            result[p_key] = p_val
        elif "step" in val.keys():
            # this links the variable to previous step output
            if previous_step_outputs:
                for out in previous_step_outputs:
                    if out["output"] == val["output"]:
                        # p_val = out["as"]
                        if step_outputs["output"]:
                            p_val = step_outputs["output"]
                            print("\n!!!!!!!!!!!!!if clause!!!!!!!!!!!!!!!!!!!!!\n")
                            print(p_val)
                        else:
                            # what do I need to do here??
                            print("\n!!!!!!!!!!!!!else clause!!!!!!!!!!!!!!!!!!!!!\n")
                            print(out)
                            print(val)

                        # this bit handles multiple inputs: if a step
                        # requires input from multiple steps, add them to
                        # the list in result dict. this is the reason for
                        # mypy ignore statements, mypy doesn't understand
                        # redefinition
                        if p_key in result:
                            if not isinstance(result[p_key], set):
                                result[p_key] = {result[p_key]}  # type: ignore [assignment]
                            result[p_key].add(p_val)  # type: ignore [attr-defined]
                        else:
                            result[p_key] = p_val
            else:
                if val["output"] in workflow_variables:
                    result[p_key] = workflow_variables[val["output"]]

    for item in outputs:
        p_key = item["output"]
        # p_val = item["as"]
        # p_val = step_outputs["output"]
        p_val = "somefile.smi"
        result[p_key] = p_val

    options = set_variables_from_options_for_step(
        definition=workflow,
        variables=workflow_variables,
        step_name=step_name,
    )

    result |= options
    return result


def get_step_replication_param(*, step: dict[str, Any]) -> str | Any:
    """Return step's replication info"""
    replicator = step.get("replicate", None)
    if replicator:
        # 'using' is a dict but there can be only single value for now
        replicator = list(replicator["using"].values())[0]

    return replicator
