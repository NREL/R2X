# Git Convention

The R2X project follow a trunk based development and we try our best to have short lived branches.
If you want to contribute to R2X first,

## Branching R2X

1. Create a new branch: `git switch -c feature-or-bugfix-name`
    1. Follow our [branch-name-convention](#branch-name-convention) for selecting the appropiate
       name,
1. Edit the code and/or the documentation

**Before committing:**

1. Run `pytest` to run the tests (fix any issue)
1. If you updated the documentation or the project dependencies:
    1. run `make html` using the sphinx makefile (inside of the `docs` folder).
    1. go to `build/index.html` and check that everything looks good

Once your changes have passed the test, you can write your commit using our [commit message
convention](#commit-message-convention)

```{note}
If you are unsure about how to fix or ignore a warning, just let the continuous integration fail,
and we will help you during the review.
```

Don't bother updating the changelog, we will take care of this.

(branch-name-convention)=
## Branch name convention

We have adopted a standard practices like in big sofware companies that basically states that if you are
a core developers are allowed to use `<type>/<scope>` for branch names. For new developers, we
strongly suggest to prefix your initials `<name>/<type>/<scope>` (e.g., ps/ci/testing) to smooth review process.

Similar to [Commit message convention](#commit-message-convention), a branch can have the following types:

- `build`: About packaging, building wheels, etc.
- `chore`: About packaging or repo/files management.
- `ci`: About Continuous Integration.
- `deps`: Dependencies update.
- `docs`: About documentation.
- `feat`: New feature.
- `fix`: Bug fix.
- `perf`: About performance.
- `refactor`: Changes that are not features or bug fixes.
- `style`: A change in code style/format.
- `tests`: About tests.

(commit-message-convention)=
## Commit message convention

Commit messages must follow our convention based on the
[Angular style](https://gist.github.com/stephenparish/9941e89d80e2bc58a153#format-of-the-commit-message)
or the [Karma convention](https://karma-runner.github.io/4.0/dev/git-commit-msg.html):

```
<type>[(scope)]: Subject

[Body]
```

```{note}
**Subject and body must be valid Markdown.**
```

Subject must have proper casing (uppercase for first letter
if it makes sense), but no dot at the end, and no punctuation
in general.

Scope and body are optional. Type can be:

- `build`: About packaging, building wheels, etc.
- `chore`: About packaging or repo/files management.
- `ci`: About Continuous Integration.
- `deps`: Dependencies update.
- `docs`: About documentation.
- `feat`: New feature.
- `fix`: Bug fix.
- `perf`: About performance.
- `refactor`: Changes that are not features or bug fixes.
- `style`: A change in code style/format.
- `tests`: About tests.

If you write a body, please add trailers at the end
(for example issues and PR references, or co-authors),
without relying on GitHub's flavored Markdown:

```
Body.

Issue #10: https://github.com/namespace/project/issues/10
Related to PR namespace/other-project#15: https://github.com/namespace/other-project/pull/15
```

These "trailers" must appear at the end of the body,
without any blank lines between them. The trailer title
can contain any character except colons `:`.
We expect a full URI for each trailer, not just GitHub autolinks
(for example, full GitHub URLs for commits and issues,
not the hash or the #issue-number).

We do not enforce a line length on commit messages summary and body,
but please avoid very long summaries, and very long lines in the body,
unless they are part of code blocks that must not be wrapped.
