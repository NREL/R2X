# PowerSystem Schemas
The code in this directory generates models from the PowerSystemSchemas repository.

## Templates
`./templates` contains `Mustache` template files from the OpenAPI code generator.
We customize the templates so that the generated models derive from `infrasys.Component`
instead of `pydantic.BaseModel`.

Whenever the version of the code generator is upgraded, we need to regenerate the templates
to check for updates. The source of truth for the code generator version is in the code
generation script, `schemas/generate_sienna_models.sh`.

Here is an example command to generate a templates directory:
```bash
$ docker run \
    -v ${PWD}/templates:/templates \
    docker.io/openapitools/openapi-generator-cli:latest \
    author template -g python -o /templates
```

## Generation instructions

**Prerequisites**: 
- Docker must be installed.
- A UNIX-like envionment (no Windows shells).

1. Change to the root directory of the R2X repository.
2. Optional: Read customization options at the top of schemas/generate_sienna_models.sh.
3. Generate the models:
```bash
$ bash schemas/generate_sienna_models.sh
```
4. Run `git diff` and verify the changes.
5. Run the tests.
6. Commit the changes open a pull request.
