"""A module to validate and decode workflow definitions.

This is typically used by the Data Manager's Workflow Engine.
"""

import os
from dataclasses import dataclass
from enum import Enum
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


@dataclass
class Translation:
    """A source ("in_") to destination ("out") variable map."""

    in_: str
    out: str


class ReplicationOrigin(Enum):
    """Oirgin of a replication variable."""

    STEP_VARIABLE = 1
    WORKFLOW_VARIABLE = 2


@dataclass
class ReplicationDriver:
    """A step's replication driver.
    The 'variable' is the variable for the step-to-be-executed
    whose value is 'driven' by the values of the 'source_variable'.
    The source variable is either from a step (or a workflow)."""

    origin: ReplicationOrigin
    variable: str
    source_variable: str
    source_step_name: str | None = None


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


def get_step_workflow_variable_mapping(*, step: dict[str, Any]) -> list[Translation]:
    """Returns a list of workflow vaiable name to step variable name tuples
    for the given step."""
    variable_mapping: list[Translation] = []
    if "variable-mapping" in step:
        for v_map in step["variable-mapping"]:
            if "from-workflow" in v_map:
                variable_mapping.append(
                    Translation(
                        in_=v_map["from-workflow"]["variable"], out=v_map["variable"]
                    )
                )
    return variable_mapping


def get_step_prior_step_variable_mapping(
    *, step: dict[str, Any]
) -> dict[str, list[Translation]]:
    """Returns list of translate objects, indexed by prior step name,
    that identify source step vaiable name to this step's variable name."""
    variable_mapping: dict[str, list[Translation]] = {}
    if "variable-mapping" in step:
        for v_map in step["variable-mapping"]:
            if "from-step" in v_map:
                step_name = v_map["from-step"]["name"]
                step_variable = v_map["from-step"]["variable"]
                # Tuple is "from" -> "to"
                if step_name in variable_mapping:
                    variable_mapping[step_name].append(
                        Translation(in_=step_variable, out=v_map["variable"])
                    )
                else:
                    variable_mapping[step_name] = [
                        Translation(in_=step_variable, out=v_map["variable"])
                    ]
    return variable_mapping


def get_step_replication_driver(*, step: dict[str, Any]) -> ReplicationDriver | None:
    """If the step is expected to replicate we return its replication driver,
    which consists of a (prior) step name and an (output) variable name.
    Otherwise it returns nothing."""
    if replicator := step.get("replicate"):
        # We need the variable we replicate against,
        # and the step that owns the variable.
        #
        # 'using' is a dict but there can be only single value for now
        variable: str = replicator["using"]["variable"]
        source_variable: str | None = None
        # Is the variable from a prior step?
        step_name: str | None = None
        step_v_map = get_step_prior_step_variable_mapping(step=step)
        for step_name_candidate, mappings in step_v_map.items():
            for mapping in mappings:
                if mapping.out == variable:
                    step_name = step_name_candidate
                    source_variable = mapping.in_
                    break
            if step_name:
                break
        assert step_name
        assert source_variable

        return ReplicationDriver(
            origin=ReplicationOrigin.STEP_VARIABLE,
            variable=variable,
            source_step_name=step_name,
            source_variable=source_variable,
        )

    return None
