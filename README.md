# Informatics Matters Data Manager Workflow Decoder
A package that simplifies the validation and decoding of Data Manager
Workflow definitions.

## Installation (Python)
The Job decoder is published on PyPI and can be installed from there:

    pip install im-data-manager-workflow-decoder

Once installed you can validate the definition (expected to be a dictionary
formed from the definition YAML file) with:

>>> from workflow import decoder
>>> error: Optional[str] = decoder.validate_schema(workflow)
