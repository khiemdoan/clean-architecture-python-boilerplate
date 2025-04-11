# Clean Architecture Python Boilerplate

Just a simple boilerplate to create new projects for general purposes.

## Virtual Environment

Create virtual environment

```sh
uv venv
```

Generate `uv.lock`

```sh
uv lock -v -U
```

Active environment

```sh
source .venv/bin/activate
```

Install main dependencies for production

```sh
uv sync --frozen
```

Install main + dev dependencies

```zsh
uv sync --group tools --group linting --group testing
```

**Note**: In order to prevent conflicts in the production environment, it is important to utilize fixed versions of the main dependencies. If there are any packages that require updating, we will handle the process manually.
