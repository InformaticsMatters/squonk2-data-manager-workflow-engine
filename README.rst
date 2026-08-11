Informatics Matters Data Manager Workflow Engine
================================================

.. image:: https://badge.fury.io/py/im-data-manager-workflow-engine.svg
    :target: https://badge.fury.io/py/im-data-manager-workflow-engine
    :alt: PyPI package (latest)

.. image:: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-engine/actions/workflows/build.yaml/badge.svg
    :target: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-engine/actions/workflows/build.yaml
    :alt: Build

.. image:: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-engine/actions/workflows/publish.yaml/badge.svg
    :target: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-engine/actions/workflows/publish.yaml
    :alt: Publish

A package that simplifies the validation and decoding of Data Manager
Workflow definitions.

Installation (Python)
=====================

The Job decoder is published on `PyPI`_ and can be installed from there:

    pip install im-data-manager-workflow-engine

Once installed you can validate the workflow definition (expected to be a dictionary
formed from the definition YAML file) with:

    >>> from workflow import decoder
    >>> error: Optional[str] = decoder.validate_schema(workflow)

.. _PyPI: https://pypi.org/project/im-data-manager-workflow-engine

Contributing
============

The project's written in Python and uses `uv`_ for dependency and package
management. We also use `pre-commit`_ to manage our pre-commit hooks, which
rely on `black`_, `mypy`_, `pylint`_, amongst others.

The Python version is pinned in ``.python-version``; uv will install it for you
if you don't have it.

From within a VS Code `devcontainer`_ environment (recommended)::

    uv sync
    uv run pre-commit install -t commit-msg -t pre-commit

And then start by running the pre-commit hooks to ensure you're stating with a
_clean_ project::

    uv run pre-commit run --all-files

And then run the tests::

    uv run coverage run -m pytest
    uv run coverage report

.. _devcontainer: https://code.visualstudio.com/docs/devcontainers/containers
.. _uv: https://docs.astral.sh/uv
.. _pre-commit: https://pre-commit.com
.. _black: https://github.com/psf/black
.. _mypy: https://github.com/python/mypy
.. _pylint: https://pypi.org/project/pylint/

Get in touch
============

- Report bugs, suggest features or view the source code `on GitHub`_.

.. _on GitHub: https://github.com/informaticsmatters/squonk2-data-manager-workflow-engine
