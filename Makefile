VENV := .venv
PYTHON := $(VENV)/bin/python

.PHONY: venv install test dev-install run-retro

venv:
	python3.11 -m venv $(VENV)

install: venv
	$(VENV)/bin/pip install -q -e packages/agenttrace[dev] -e packages/retro

test: install
	$(PYTHON) -m pytest packages/agenttrace/tests/ -q

run-retro: install
	$(PYTHON) -m flask --app retro.server run --port 5001

dev-install: install
	@echo "Venv ready. Activate with: source .venv/bin/activate"
