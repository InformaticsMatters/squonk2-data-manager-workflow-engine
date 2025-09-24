"""A module to validate and decode workflow (and step) definitions.

Module philosophy
-----------------
The _main_ purpose of this module is to provide a 'validate_schema()' function
to check that a workflow definition (a dictionary) that is expected to comply with
the 'workflow-schema,yaml' schema. This function returns a string (an error) if there's
a problem with the definition.

The decoder module also provides a number of additional functions based on the needs
of the engine. As a developer you are 'encouraged' to place any logic that is expected
to navigate the scheme content in a function in this module. Any code that
is supposed to know _where_ to get content should be encoded as a function here.
For example, rather than external code navigating the 'plumbing' blocks
we have a function 'get_workflow_variable_names()' that returns the names of workflow
variables.
"""

import os
from dataclasses import dataclass
from typing import Any

import decoder.decoder as job_definition_decoder
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
class Connector:
    """A connection - connects a plumbing source variable ("in_")
    to destination variable ("out")."""

    in_: str
    out: str


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


def get_step(definition: dict[str, Any], name: str) -> dict[str, Any]:
    """Given a Workflow definition this function returns a named step
    (if it exists)."""
    steps: list[dict[str, Any]] = get_steps(definition)
    for step in steps:
        if step["name"] == name:
            return step
    return {}


def get_name(definition: dict[str, Any]) -> str:
    """Given a Workflow definition this function returns its name."""
    return str(definition.get("name", ""))


def get_description(definition: dict[str, Any]) -> str | None:
    """Given a Workflow definition this function returns its description (if it has one)."""
    return definition.get("description")


def is_workflow_output_variable(definition: dict[str, Any], variable_name: str) -> bool:
    """True if the variable name is in the workflow variables outputs list."""
    # We can safely pass on the workflow definition as its
    # root-level 'variables' block complies with job-definition variables.
    return variable_name in job_definition_decoder.get_outputs(definition)


def is_workflow_input_variable(definition: dict[str, Any], variable_name: str) -> bool:
    """True if the variable name is in the workflow variables inputs list."""
    # We can safely pass on the workflow definition as its
    # root-level 'variables' block complies with job-definition variables.
    return variable_name in job_definition_decoder.get_inputs(definition)


def get_workflow_variable_names(definition: dict[str, Any]) -> set[str]:
    """Given a Workflow definition this function returns all the names of the
    variables defined in steps that need to be defined at the workflow level.
    These are the 'variables' used in every step's 'plumbing' block.
    """
    wf_variable_names: set[str] = set()
    steps: list[dict[str, Any]] = get_steps(definition)
    for step in steps:
        if v_map := step.get("plumbing"):
            for v in v_map:
                if "from-workflow" in v:
                    wf_variable_names.add(v["from-workflow"]["variable"])
    return wf_variable_names


def get_step_workflow_variable_connections(
    *, step_definition: dict[str, Any]
) -> list[Connector]:
    """Returns a list of connectors that connect a workflow variable name
    to a step variable name for the given step definition."""
    connections: list[Connector] = []
    if "plumbing" in step_definition:
        for v_map in step_definition["plumbing"]:
            if "from-workflow" in v_map:
                connections.append(
                    Connector(
                        in_=v_map["from-workflow"]["variable"], out=v_map["variable"]
                    )
                )
    return connections


def get_step_predefined_variable_connections(
    *, step_definition: dict[str, Any]
) -> list[Connector]:
    """Returns the set of connections of pre-defined variables (in) to
    step variables (out)."""
    connections: list[Connector] = []
    if "plumbing" in step_definition:
        for v_map in step_definition["plumbing"]:
            if "from-predefined" in v_map:
                connections.append(
                    Connector(
                        in_=v_map["from-predefined"]["variable"], out=v_map["variable"]
                    )
                )
    return connections


def get_step_prior_step_connections(
    *, step_definition: dict[str, Any]
) -> dict[str, list[Connector]]:
    """Returns list of variable Connections, indexed by prior step name,
    that identify a source step variable name (an output) to an input variable in this
    step (an input)."""
    plumbing: dict[str, list[Connector]] = {}
    if "plumbing" in step_definition:
        for v_map in step_definition["plumbing"]:
            if "from-step" in v_map:
                step_name = v_map["from-step"]["name"]
                step_variable = v_map["from-step"]["variable"]
                # Tuple is "from" -> "to"
                if step_name in plumbing:
                    plumbing[step_name].append(
                        Connector(in_=step_variable, out=v_map["variable"])
                    )
                else:
                    plumbing[step_name] = [
                        Connector(in_=step_variable, out=v_map["variable"])
                    ]
    return plumbing
