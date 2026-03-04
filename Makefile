SHELL := /bin/bash
.ONESHELL:
MAKEFLAGS += --no-print-directory

.PHONY: install activate update deps test format check_uv

DOCS_DIR := tmp/audits/gx/uncommitted/data_docs/local_site
DOCS_PORT := 8080


# This block defines the "dictionary" logic once.
# We use $$ everywhere so Make passes the dollar signs to the Bash shell.
define SETUP_ENV_VARS
	declare -A ENV
	ENV["PACKAGE"]=$$(grep -m 1 'name' pyproject.toml | sed -E 's/name = "(.*)"/\1/')
	ENV["PYTHON_VERSION"]=$$(grep -m 1 'python' pyproject.toml | sed -nE 's/.*[~^]=?([0-9]+\.[0-9]+).*/\1/p')
	ENV["ENV_DIR"]="${HOME}/envs/$${ENV["PACKAGE"]}"
	ENV["ENV_PATH"]="$${ENV["ENV_DIR"]}/bin/activate"
endef


# --- 0. Requirement Check ---
check_uv:
	@command -v uv >/dev/null 2>&1 || { \
		echo >&2 "Error: 'uv' is not installed."; \
		echo >&2 "Please install it via: wget -qO- https://astral.sh/uv/install.sh | sh"; \
		exit 1; \
	}

# --- 1. Install ---
install:
	@$(SETUP_ENV_VARS)
	if [ ! -d "$${ENV["ENV_DIR"]}" ]; then
		echo "Installing environment for $${ENV["PACKAGE"]}..."
		uv venv "$${ENV["ENV_DIR"]}" --python "$${ENV["PYTHON_VERSION"]}"
		source "$${ENV["ENV_PATH"]}" && uv sync --active
		echo "Install complete. Use 'make activate' to activate it"
	else
		echo "Environment for $${ENV["PACKAGE"]} already exists at $${ENV["ENV_PATH"]}. Use 'make activate' to activate it, or 'make update' to update it."

	fi
	unset ENV

# --- 2. Activate ---
activate:
	@if [ "$$IN_MAKE_SHELL" = "true" ]; then
		echo "Already inside a sub-shell, either exit the sub-shell with 'exit'/ctrl+d or start a new terminal session."
		exit 0
	fi
	$(SETUP_ENV_VARS)
	if [ ! -f "$${ENV["ENV_PATH"]}" ]; then
		echo "Error: Environment not found. Run 'make install' first."
		exit 1
	fi
	echo "--- Entering $${ENV["PACKAGE"]} environment and sourcing .env (type 'exit' to leave) ---"
	bash --rcfile <(echo "source ~/.bashrc; source scripts/load_dot_env.sh; source $${ENV["ENV_PATH"]}; export IN_MAKE_SHELL=true")
	unset ENV

# --- 3. Update ---
update:
	@$(SETUP_ENV_VARS)
	if [ ! -f "$${ENV["ENV_PATH"]}" ]; then
		echo "Error: Environment not found. Run 'make install' first."
		exit 1
	fi
	echo "Updating environment for $${ENV["PACKAGE"]}..."
	rm uv.lock
	uv venv  --clear "$${ENV["ENV_DIR"]}" --python "$${ENV["PYTHON_VERSION"]}"
	source "$${ENV["ENV_PATH"]}" && uv sync --active
	unset ENV
	echo "Update complete."


test:
	@echo "Running tests via uv..."
	@$(SETUP_ENV_VARS)
	bash --rcfile <(echo "source ~/.bashrc; source scripts/load_dot_env.sh; source $${ENV["ENV_PATH"]}; export IN_MAKE_SHELL=true") \
	-c "uv run --with pytest pytest -q"
	unset ENV

format:
	@echo "Running ruff formatter/linter via temporary uv environment..."
	uv run --with ruff ruff check . --fix
	uv run --with ruff ruff format .



# --- Docker Commands ---

# Run containers in the background
infra_up:
	@echo "Starting Docker containers..."
	docker compose -f docker-compose-infra.yaml up -d
	@echo "Containers are running. Use 'make infra_status' to check."

# Stop and remove containers
infra_down:
	@echo "Stopping Docker containers..."
	docker compose -f docker-compose-infra.yaml down

# Check the health of your services (useful for your minio-init)
infra_status:
	@echo "Current container status:"
	docker compose ps

audit_docs:
	@if [ ! -d "$(DOCS_DIR)" ]; then \
		echo "Error: Data Docs directory not found at $(DOCS_DIR)."; \
		echo "Run your audit script first to generate the reports."; \
		exit 1; \
	fi
	@echo "--- Starting Data Docs Server at http://localhost:$(DOCS_PORT) ---"
	@echo "Press Ctrl+C to stop the server."
	@python -m http.server $(DOCS_PORT) --directory $(DOCS_DIR)