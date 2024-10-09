Informatics Matters Data Manager Workflow Decoder
=================================================

.. image:: https://badge.fury.io/py/im-data-manager-workflow-decoder.svg
    :target: https://badge.fury.io/py/im-data-manager-workflow-decoder
    :alt: PyPI package (latest)

.. image:: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-decoder/actions/workflows/build.yaml/badge.svg
    :target: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-decoder/actions/workflows/build.yaml
    :alt: Build

.. image:: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-decoder/actions/workflows/publish.yaml/badge.svg
    :target: https://github.com/InformaticsMatters/squonk2-data-manager-workflow-decoder/actions/workflows/publish.yaml
    :alt: Publish

A package that simplifies the validation and decoding of Data Manager
Workflow definitions.

Installation (Python)
=====================

The Job decoder is published on `PyPI`_ and can be installed from there:

    pip install im-data-manager-workflow-decoder

Once installed you can validate the workflow definition (expected to be a dictionary
formed from the definition YAML file) with:

    >>> from workflow import decoder
    >>> error: Optional[str] = decoder.validate_schema(workflow)

.. _PyPI: https://pypi.org/project/im-data-manager-workflow-decoder

Contributing
============

The project's written in Python and uses `Poetry`_ for dependency and package
management. We also use `pre-commit`_ to manage our pre-commit hooks, which
rely on `black`_, `mypy`_, `pylint`_, amongst others.

Create your environment::

    poetry shell
    poetry install --with dev
    pre-commit install -t commit-msg -t pre-commit

And then start by running the pre-commit hooks to ensure you're stating with a
_clean_ project::

    pre-commit run --all

And then run the tests::

    pytest

.. _Poetry: https://python-poetry.org
.. _pre-commit: https://pre-commit.com
.. _black: https://github.com/psf/black
.. _mypy: https://github.com/python/mypy
.. _pylint: https://pypi.org/project/pylint/

Get in touch
============

- Report bugs, suggest features or view the source code `on GitHub`_.

.. _on GitHub: https://github.com/informaticsmatters/squonk2-data-manager-workflow-decoder
