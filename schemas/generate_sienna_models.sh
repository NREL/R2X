# Generates PowerSystem models for use in R2X.
#   Customizations via environment variables:
#     - Set R2X_PSY_SCHEMAS_TAG to use a specific tag or branch of PowerSystemSchemas.
#     - Set R2X_CONTAINER_EXEC if you want to use something other than docker (e.g., podman).
#     - Set R2X_OPENAPI_CODEGEN_CLI_VERSION to use a specific docker tag for the CLI.
#     - Set R2X_SCHEMA_DIR to the path of an existing PowerSystemSchemas repository.
#       This is useful to avoid cloning the repo during development.
#       Causes R2X_PSY_SCHEMAS_TAG to be ignored. Uses whatever is in the local repo.

set -e

# Don't know why, but docker is not respecting $TMPDIR on my Mac.
# It is configured to allow file-sharing on /var/folders.
OPENAPI_CLIENT_DIR=/tmp/psy_schemas_openapi_client
MODEL_DIR=${PWD}/src/r2x/models/generated
OPENAPI_TEMPLATE_DIR=${PWD}/schemas/templates
TEMPLATE_FILE=${OPENAPI_TEMPLATE_DIR}/model_generic.mustache

if [ -z ${R2X_PSY_SCHEMAS_TAG} ]; then
    R2X_PSY_SCHEMAS_TAG=main  # TODO: this should be a version
fi

if [ -z ${R2X_OPENAPI_CODEGEN_CLI_VERSION} ]; then
    R2X_OPENAPI_CODEGEN_CLI_VERSION=v7.10.0
fi

if [ -z ${R2X_SCHEMA_DIR} ]; then
    SCHEMA_DIR=${TMPDIR}/psy_schemas
    if [ -d ${SCHEMA_DIR} ]; then
        rm -rf ${SCHEMA_DIR}
    fi
    git clone git@github.com:NREL-Sienna/PowerSystemSchemas ${SCHEMA_DIR}
    cd ${SCHEMA_DIR}
    if [ $(git rev-parse --abbrev-ref HEAD) != "main" ]; then
        git checkout ${R2X_PSY_SCHEMAS_TAG}
    fi
    cd -
    cloned_schema_repo=true
else
    SCHEMA_DIR=$(realpath ${R2X_SCHEMA_DIR})
    cloned_schema_repo=false
fi

if [ -z ${R2X_CONTAINER_EXEC} ]; then
    R2X_CONTAINER_EXEC=docker
fi

echo "Running openapi-generator-cli version=${R2X_OPENAPI_CODEGEN_CLI_VERSION}"

rm -rf ${OPENAPI_CLIENT_DIR}
${R2X_CONTAINER_EXEC} run \
    -v ${SCHEMA_DIR}:/schemas \
    -v ${OPENAPI_CLIENT_DIR}:/client_dir \
    -v ${OPENAPI_TEMPLATE_DIR}:/template_dir \
    docker.io/openapitools/openapi-generator-cli:${R2X_OPENAPI_CODEGEN_CLI_VERSION} \
    generate \
        --generator-name=python \
        --input-spec=/schemas/openapi.json \
        --output=/client_dir \
        --template-dir=/template_dir

ruff format ${OPENAPI_CLIENT_DIR}
rm -f ${MODEL_DIR}/*
mv ${OPENAPI_CLIENT_DIR}/openapi_client/models/*.py ${MODEL_DIR}

rm -rf ${OPENAPI_CLIENT_DIR}
if [ "${cloned_schema_repo}" = true ]; then
    rm -rf ${SCHEMA_DIR}
fi
echo "Generated model files in ${MODEL_DIR}"
