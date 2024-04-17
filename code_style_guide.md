# tensorlakehouse code style guide

## python language

### black formatter
"Black aims for consistency, generality, readability and reducing git diffs." [[see black webpage](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html#code-style)]
```shell
python -m black tensorlakehouse-openeo-driver --check --diff --color --exclude libs
```

### flake8

flake8 verifies pep8, pyflakes, and circular complexity [[see flake8 webpage](https://flake8.pycqa.org/en/latest/)]

```shell
python -m flake8 tensorlakehouse-openeo-driver/ --ignore=N802,E501,W503 --max-line-length=100
```

### mypy

"Mypy is a static type checker for Python. Type checkers help ensure that you're using variables and functions in your code correctly." [[see mypy webpage](https://mypy.readthedocs.io/en/stable/index.html)]

```shell
mypy tensorlakehouse-openeo-driver/ --ignore-missing-imports
```