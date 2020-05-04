.EXPORT_ALL_VARIABLES:

WHEEL_NAME=metadata_driven-0.0.0-py3-none-any.whl
CLUSTER_ID=0109-142921-marks646

help:
	@echo "Tasks in \033[1;32mpython-project\033[0m:"
	@cat Makefile

lint:
	mypy src --ignore-missing-imports
	flake8 src --ignore=$(shell cat .flakeignore)

dev:
	python setup.py develop

test: dev
	pytest --doctest-modules --junitxml=junit/test-results.xml
	bandit -r src -f xml -o junit/security.xml || true

build: clean
	pip install wheel
	python setup.py bdist_wheel

clean:
	@rm -rf .pytest_cache/ .mypy_cache/ junit/ build/ dist/
	@find . -not -path './.venv*' -path '*/__pycache__*' -delete
	@find . -not -path './.venv*' -path '*/*.egg-info*' -delete

install-package-databricks:
	@echo Installing 'dist/$${WHEEL_NAME}' on databricks...
	databricks fs cp dist/$${WHEEL_NAME} dbfs:/libraries/ --overwrite
	databricks libraries install --whl dbfs:/libraries/$${WHEEL_NAME} --cluster-id $${CLUSTER_ID}
