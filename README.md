# Clean Architecture Python Boilerplate

Just a simple boilerplate to create new projects for general purposes.

## Virtual Environment

Create virtual environment

```zsh
pdm venv create
```

Generate `pdm.lock`

```zsh
pdm lock -G linting,testing
```

Active environment

```zsh
source .venv/bin/activate
```

Install main dependencies for production

```zsh
pdm sync --prod --no-editable
```

Install main + dev dependencies

```zsh
pdm install
```

**Note**: In order to prevent conflicts in the production environment, it is important to utilize fixed versions of the main dependencies. If there are any packages that require updating, we will handle the process manually.
