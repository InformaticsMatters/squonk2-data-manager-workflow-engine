# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`im-data-manager-workflow-engine` (package directory `workflow/`) is a library, not a service.
It is imported by the Squonk2 **Data Manager** (DM), which owns the database, the REST API,
the message queue and the Kubernetes Job execution machinery. This repository contributes
only: the workflow schema, a decoder for it, a multi-level validator, and the engine's
state logic. It is published to PyPI as `im-data-manager-workflow-engine`.

## Commands

Everything runs through Poetry, and **tests must be run from the repository root** —
the unit-test instance launcher uses the relative path `tests/project-root/<project-id>`
as its simulated project directory.

```bash
poetry install --with dev --sync
pre-commit install -t commit-msg -t pre-commit

pre-commit run --all-files          # black, isort, mypy (strict, workflow/ only), pylint, yamllint
poetry run coverage run -m pytest   # what CI runs (CI adds -Werror)
poetry run coverage report

poetry run pytest tests/test_workflow_engine_examples.py                                    # one file
poetry run pytest tests/test_workflow_engine_examples.py::test_workflow_engine_example_nop_fail  # one test
```

Every test file carries `pytestmark = pytest.mark.unit`; the other markers declared in
`pytest.ini` (`integration`, `job`, `soak`, …) are inherited convention and unused here.

Commits must satisfy the Conventional Commit pattern in `.cz.yaml`
(`feat|fix|perf|refactor|remove|style|test|build|docs|chore|ci|dev|BREAKING CHANGE`);
commitizen enforces this as a `commit-msg` hook. Releases are cut by pushing a semver tag
(no `v` prefix — see the existing `2.0.1`, `2.0.0-rc.1` tags); `publish.yaml` then sets the
package version from the tag and publishes to PyPI. The version in `pyproject.toml` is not
maintained by hand.

## Architecture

### The four modules in `workflow/`

- `workflow-schema.yaml` — JSON Schema (draft-07) for a workflow definition. `kind-version`
  is currently pinned to the single value `2025.2`.
- `decoder.py` — `validate_schema()` plus accessor functions over a definition dictionary.
- `workflow_abc.py` — the ABCs the DM must implement (`WorkflowAPIAdapter`,
  `InstanceLauncher`) and the `LaunchParameters` / `LaunchResult` dataclasses.
- `workflow_validator.py` — `WorkflowValidator.validate()` at CREATE / TAG / RUN levels.
- `workflow_engine.py` — `WorkflowEngine.handle_message()`, the execution logic.

### Inversion of control

The engine holds **no state and no persistence**. It is constructed by the DM with two
injected objects defined as ABCs in `workflow_abc.py`:

- `WorkflowAPIAdapter` — reads/writes `Workflow`, `RunningWorkflow`, `RunningWorkflowStep`
  and `Instance` records. Every method returns `(dict, int)` where the int is an HTTP
  status the engine ignores (it exists so the DM can reuse its own `views.py` functions).
  A missing record is an empty dict, not an exception.
- `InstanceLauncher` — actually starts a Job as a Kubernetes Pod. The engine builds a
  `LaunchParameters` and gets back a `LaunchResult`; an error in the result is a *launch*
  error, never a Job error.

The engine does not create records. The DM creates `Workflow` and `RunningWorkflow`;
the launcher creates `Instance` and `RunningWorkflowStep`.

### Event flow

`handle_message()` takes protobuf messages from the DM's queue and is the whole entry point:

- `WorkflowMessage` with `action == "START"` → launch every **READY** step (see below).
- `WorkflowMessage` with `action == "STOP"` → mark the running workflow done (only if no
  steps are still running).
- `PodMessage` → a previously launched step finished. Non-zero `exit_code` fails the step
  *and* the running workflow; zero means re-assess the workflow and launch every step that
  is now READY.

State is reconstructed from DM records on every message — nothing is cached between calls.

### READY steps

The engine does **not** follow the order steps are written in. On every message it scans
all the steps and launches those that are READY — `_launch_ready_steps()`, backed by
`_get_step_states()` and `_get_ready_steps()`. A step is READY when it has not already
been launched *and* every step it depends on has finished successfully. Dependencies are
the `from-step` entries in a step's `plumbing`, via `decoder.get_step_dependencies()`;
nothing else in the schema expresses ordering, so a step with no `from-step` entries is
READY at START. Consequences: a workflow can start several steps at once, independent
branches run concurrently, and a step drawing on two prior steps waits for both.

Two rules follow from re-scanning every step every time:

- **A step must be launched exactly once.** The engine checks the DM's records before
  launching, but that check is not atomic with the launch, so `InstanceLauncher.launch()`
  is required to be idempotent for a given `(running_workflow_id, step_name,
  step_replication_number)` and to report a repeat via `LaunchResult.already_launched`.
  The DM is expected to back this with a database uniqueness constraint.
- **Finishing is no longer "the last step in the list finished".** When nothing can be
  launched and nothing is running, the workflow is done — successfully if every step ran,
  and as a failure if steps remain that will never become READY (a stall, which a
  validated definition should make impossible).

### Terminology (used consistently throughout)

A **Step** is a definition inside a **Workflow**. Running a Step means running a DM **Job**,
which manifests as an **Instance** (a Pod, and a DM database row). A **RunningWorkflow** is
one execution of a Workflow; a **RunningWorkflowStep** is one execution of one Step.
A Step is **READY** when it can be launched right now — it has not already been launched
and every Step it depends on has completed successfully.

### The hard part: `_prepare_step()` and `_launch()`

Nearly all engine complexity lives in `_prepare_step()`, which turns a step definition plus
prior-step state into a `StepPreparationResponse` (variables, replica count, dependent
instances, project inputs/outputs). Two behaviours drive its shape:

- **Replication (fan-out)** — if a variable in this step's `plumbing` maps to a prior step's
  *output* whose Job-definition type is `files`, this step runs once per file produced.
  `replica_variable`/`replica_values` carry that, and `_launch()` loops, overwriting the
  variable with `<instance-dir>/<file>` for each replica.
- **Combining (fan-in)** — if a variable in the plumbing maps to one of *this* step's
  *inputs* whose type is `files`, the step is a combiner. Waiting for the steps it combines
  is not its job — READY already guarantees they have all finished successfully — so the
  flag only selects how inputs are handled, and suppresses replication.
  Combiners reference prior outputs through a directory glob rather than named files: the
  step's plumbing pulls the engine's only pre-defined variable, `instance-link-glob`, via a
  `from-predefined` entry. (The `_INSTANCE_LINK_GLOB_VARIABLE = "dirsGlob"` constant in
  `workflow_engine.py` is currently unreferenced.)

`_prepare_step()` returning `replicas == 0` with `error_num == 0` means "not yet, try again
on a later message" — not a failure. Only a non-zero `error_num` fails the workflow.

Before returning, `_prepare_step()` renders the Job's command with the assembled variables
through the Job decoder; a render failure is how a missing variable is caught.

### Validation levels

Validation is deliberately front-loaded so the engine can stay assertion-based rather than
defensive — if you find yourself adding "does this variable exist?" checks to the engine,
the check probably belongs in `workflow_validator.py`. The DM calls the validator; by the
time a START message arrives the workflow is known to be runnable.

- **CREATE** — schema only. Incomplete workflows are legal so users can save work in progress.
- **TAG** — CREATE plus structural checks (e.g. no duplicate step names).
- **RUN** — TAG plus all workflow variables supplied and every step's Job known to the DM.

### Where logic belongs

Anything that needs to *navigate* the workflow definition structure goes in `decoder.py`
as a named function (`get_workflow_variable_names()`, `get_step_prior_step_connections()`,
…). The engine and validator should never walk `plumbing` blocks directly.

`workflow_engine.py` is intentionally one large module. The module docstring is explicit
that the file should not be split up merely to reduce its size — the complexity is inherent
to moving a workflow forward, and indirection would only hide it. The long docstrings at
the top of each module ("Module philosophy") are the authoritative design notes; read them
before changing a module and keep them current.

## The test harness

`tests/` contains a small in-process simulation of the Data Manager. When changing engine
behaviour you will usually need to extend it:

- `wapi_adapter.py` — `UnitTestWorkflowAPIAdapter`, a fake DM database. Because the test
  message queue is a separate `multiprocessing.Process`, "tables" are pickled to
  `tests/pickle-files/` under a lock rather than held in memory. It also exposes create/mock
  helpers the real ABC does not have, notably
  `mock_get_running_workflow_step_output_values_for_output()`, used to fake the file list a
  prior step produced (this is what drives replication in tests).
- `instance_launcher.py` — `UnitTestInstanceLauncher` runs a Job as a real subprocess
  (`python tests/jobs/<command>`) in the simulated project directory, then dispatches a
  `PodMessage` carrying the subprocess exit code. It **wipes the project directory on
  construction**, so each test starts clean.
- `message_queue.py` / `message_dispatcher.py` — a `Process`-based stand-in for the DM's
  RabbitMQ, serialising protobufs so the engine is exercised across a process boundary.
- `job-definitions/job-definitions.yaml` — the fake Job catalogue. Adding a Job for a test
  means adding an entry here *and* a matching script in `tests/jobs/`.
- `workflow-definitions/*.yaml` — one YAML per scenario, loaded by base name.

End-to-end tests follow the pattern in `test_workflow_engine_examples.py`: use the
`basic_engine` fixture, call `start_workflow(md, da, "<definition-basename>", variables)`,
then `wait_for_workflow(da, r_wfid, expect_success=...)`, then assert on the resulting
records and on files in the project directory via `project_file_exists()`.
