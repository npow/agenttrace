VENV := .venv
PYTHON := $(VENV)/bin/python

.PHONY: venv install test dev-install

venv:
	python3.11 -m venv $(VENV)

install: venv
	$(VENV)/bin/pip install -q -e ".[dev]"

test: install
	$(PYTHON) -m pytest tests/ -q

dev-install: install
	@echo "Venv ready. Activate with: source .venv/bin/activate"
