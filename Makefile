.PHONY: setup build install check test clean

VENV 		?= $(CONDA_PREFIX)
PYTHON 		?= $(VENV)/bin/python3
PIP 		?= $(VENV)/bin/pip
CMD			?= $(VENV)/bin/palmspy

target: setup build

setup: requirements.txt
	@echo "\nInstall dependencies...\n"
	$(PIP) install -r requirements.txt

build:
	@echo "\nBuild the package...\n"
	$(PYTHON) setup.py sdist bdist_wheel

install:
	@echo "\nInstall the package...\n"
	$(PIP) install ./dist/palmspy-*-py3-none-any.whl --force-reinstall

check:
	@echo "\nSanity check...\n"
	$(CMD) --help

test:
	@echo "\nTesting...\n"
	@echo "Run simulation (it takes a few minutes)\n"
	@if [ -d "PALMSpy_output" ]; then \
		rm -rf PALMSpy_output; \
	fi
	@mkdir PALMSpy_output
	$(CMD) --gps-path ./tests/raw-data/gps --acc-path ./tests/raw-data/acc --config-file ./tests/raw-data/settings.json
	@echo "Done. Results saved in the folder PALMSpy_output\n"
	@echo "Test results\n"
	@pytest --no-header -v tests/test_merge.py

clean:
	@echo "\nCleanup...\n"
	@rm -rf build
	@rm -rf dist
	@rm -rf palmspy.egg-info