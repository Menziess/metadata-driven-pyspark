# [metadata-driven](/README.md)

[![Build Status](https://dev.azure.com/Menziess/data-processing/_apis/build/status/Menziess.metadata-driven-pyspark?branchName=master)](https://dev.azure.com/Menziess/data-processing/_build/latest?definitionId=18&branchName=master)

A package that can be called with metadata in json format.

Imagine running a Databricks job from DataFactory. The Databricks job is just a pure function that accepts the details of the job from DataFactory (stored in git of course).

The complexity of the processing can be expanded upon, but the package will force a consistent, and hopefully tested way of reading, transforming, joining, aggregating, and writing data.

## Development

1. Clone the repository.
1. Next, if you haven't done so already, create a virtual environment by running the next command, or by creating your own virualenv using your preferred tool.

        pip install pipenv
        pipenv install -d
        pipenv shell

1. Install the project in `editable/develop` mode:

        make dev

1. Run the python code:

        python -m metadata_driven.main mnt/demo/*.csv

1. Now that the project is installed, we want to see whether the tests run succesfully:

        make lint
        make test

1. Now, some caching folders may have been generated. Whenever you want to clean up your project, run:

        make clean

1. Make sure that you create a feature branch if you are about to make changs to this repository:

        git checkout -b feature/my-feature

1. After implementing your feature, run the tests again:

        make lint && make test


## TODO

1. Install `jsonschema` for proper template validation.
