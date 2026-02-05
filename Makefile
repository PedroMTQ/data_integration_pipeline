SHELL := /bin/bash
.ONESHELL:
MAKEFLAGS += --no-print-directory

.PHONY: install activate update deps test format check_uv


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
	echo "--- Entering $${ENV["PACKAGE"]} environment (type 'exit' to leave) ---"
	# We only export the guard and the prompt. The 'ENV' dictionary dies with this recipe.
	bash --rcfile <(echo "source ~/.bashrc; source $${ENV["ENV_PATH"]}; export IN_MAKE_SHELL=true")
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


# --- New: Test ---
test: check_uv
	@$(SETUP_ENV_VARS)
	if [ ! -d "$${ENV["ENV_DIR"]}" ]; then
		echo "Error: Environment not found. Run 'make install' first."
		exit 1
	fi
	
	source "$${ENV["ENV_PATH"]}"
	# Check if pytest is available, if not, add it
	if ! command -v pytest >/dev/null 2>&1; then
		echo "pytest not found. Installing..."
		uv add pytest --dev --active
	fi
	
	echo "Running tests for $${ENV["PKG"]}..."
	pytest
	unset ENV

# --- New: Format ---
format: check_uv
	@$(SETUP_ENV_VARS)
	if [ ! -d "$${ENV["ENV_DIR"]}" ]; then
		echo "Error: Environment not found. Run 'make install' first."
		exit 1
	fi

	echo "Running ruff formatter/linter..."
	source "$${ENV["ENV_PATH"]}"
	# Check if ruff is available
	if ! command -v ruff >/dev/null 2>&1; then
		echo "ruff not found. Installing..."
		uv add ruff --dev --active
	fi
	
	ruff check . --fix
	ruff format .
	unset ENV