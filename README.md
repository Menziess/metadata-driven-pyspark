---
page_type: documentation
languages:
- python
products:
- azure
description: "An example python package using databricks-connect from vscode."
author: "Stefan Schenk"
---

# [python-packaging-databricks](/README.md)

[![Build Status](https://dev.azure.com/stefanschenk/python-packaging/_apis/build/status/python-packaging-databricks?branchName=master)](https://dev.azure.com/stefanschenk/python-packaging/_build/latest?definitionId=2&branchName=master)

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
