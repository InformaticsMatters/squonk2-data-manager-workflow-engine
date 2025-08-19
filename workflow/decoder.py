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


def get_workflow_variable_names(definition: dict[str, Any]) -> set[str]:
    """Given a Workflow definition this function returns all the names of the
    variables that need to be defined at the workflow level. These are the 'variables'
    used in every steps' variabale-mapping block.
    """
    wf_variable_names: set[str] = set()
    steps: list[dict[str, Any]] = get_steps(definition)
    for step in steps:
        if v_map := step.get("variable-mapping"):
            for v in v_map:
                if "from-workflow" in v:
                    wf_variable_names.add(v["from-workflow"]["variable"])
    return wf_variable_names


def get_step_output_variable_names(
    definition: dict[str, Any], step_name: str
) -> list[str]:
    """Given a Workflow definition and a Step name this function returns all the names
    of the output variables defined at the Step level. These are the names
    of variables that have files assocaited with them that need copying to
    the Project directory (from the Instance)."""
    variable_names: list[str] = []
    steps: list[dict[str, Any]] = get_steps(definition)
    for step in steps:
        if step["name"] == step_name:
            variable_names.extend(step.get("out", []))
    return variable_names


def get_step_input_variable_names(
    definition: dict[str, Any], step_name: str
) -> list[str]:
    """Given a Workflow definition and a Step name this function returns all the names
    of the input variables defined at the Step level. These are the names
    of variables that have files assocaited with them that need copying to
    the Instance directory (from the Project)."""
    variable_names: list[str] = []
    steps: list[dict[str, Any]] = get_steps(definition)
    for step in steps:
        if step["name"] == step_name:
            variable_names.extend(step.get("in", []))
    return variable_names


def workflow_step_has_outputs(definition: dict[str, Any], name: str) -> bool:
    """Given a Workflow definition and a step name we return a boolean
    that is true if the step produces outputs. This requires inspection
    of the 'as-yet-unused' variables block."""
    return (
        len(get_step_output_variable_names(definition=definition, step_name=name)) > 0
    )


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
    assert workflow

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

    #    options = set_variables_from_options_for_step(
    #        definition=workflow,
    #        variables=workflow_variables,
    #        step_name=step_name,
    #    )
    #
    #    result |= options
    return result


def get_step_replication_param(*, step: dict[str, Any]) -> str | Any:
    """Return step's replication info"""
    replicator = step.get("replicate", None)
    if replicator:
        # 'using' is a dict but there can be only single value for now
        replicator = list(replicator["using"].values())[0]

    return replicator
