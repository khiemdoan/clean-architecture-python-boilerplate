# Clean Architecture Python Boilerplate

Just a simple boilerplate to create new projects for general purposes.

## Virtual Environment

Active environment

```zsh
poetry shell
```

Install main dependencies for production

```zsh
poetry install --no-root --only=main
```

Install main + dev dependencies

```zsh
poetry install --no-root
```

Install linting

```zsh
poetry install --no-root --only=lint
```

**Note**: In order to prevent conflicts in the production environment, it is important to utilize fixed versions of the main dependencies. If there are any packages that require updating, we will handle the process manually.
