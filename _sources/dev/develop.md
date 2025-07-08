# How To Contribute

We welcome any contributions to R2X. Please follow this guidelines to maintain code quality and consistency.

### Install developer dependencies

Install R2X development dependencies using the following command:

```console
python -m pip install -e ".[dev]"
```

## Code quality

Maintaining consistent code quality is crucial for the R2X project. To ensure
uniformity and adherence to coding standards, we employ the use [Ruff](https://docs.astral.sh/ruff/).
Ruff is and "opinionated" formatter and linters designed to enhance code readability, maintainability,
and consistency that is extremely fast.

The usage of these tools is seamlessly integrated into the R2X workflow via
pre-commit hooks. Before any code is committed to the repository, these checks
are automatically run. This ensures that all code contributions meet the
established quality standards, minimizing the chances of introducing formatting
inconsistencies or potential issues.

To set up the pre-commit hooks on your local version run the following command:

```console
pre-commit install
```

## Pull requests guidelines

Ensure to add a link to any related issue in the Pull Request message. By doing this, GitHub can
automatically close the issue for us.

Once all the changes are approved, you can squash your commits:

```bash
git rebase -i --autosquash main
```

And force-push:

```bash
git push -f
```

If this seems all too complicated, you can push or force-push each new commit,
and we will squash them ourselves if needed, before merging.


## Useful Python refereces

- [f-string formats](https://cissandbox.bentley.edu/sandbox/wp-content/uploads/2022-02-10-Documentation-on-f-strings-Updated.pdf) - For learning how to properly use f-strings.
- [Ruff rules](https://docs.astral.sh/ruff/rules/) - Our preferred formatter and linter.
- [Commit style](https://www.conventionalcommits.org/en/v1.0.0/) - Where we took our git commit convention style.
- [Software design](https://learn.scientific-python.org/development/principles/design/) - Best software practices.
- [mamba](https://mamba.readthedocs.io/en/latest/) - The recommended python package manager.
- [uv](https://github.com/astral-sh/uv) - The new challenger to take over mamba (from the developers of mamba. So maybe, mamba 2.0?).
