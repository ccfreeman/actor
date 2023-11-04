FROM python:3.11-slim-buster as base

##########
### STEP 2
##########
FROM base AS python-env

ENV PIPENV_INSTALL_TIMEOUT=9000
ENV PIPENV_TIMEOUT=9000
ENV PIP_DEFAULT_TIMEOUT=9000
ENV PIP_TRUSTED_HOST=files.pythonhosted.org

COPY Pipfile Pipfile.lock ./

RUN apt-get update \
    && pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --upgrade pip \
    && pip install pipenv --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org\
    && PIPENV_VENV_IN_PROJECT=1 pipenv sync

##########
### STEP 3
##########
FROM base

# Copy virtual env from python-env stage
COPY --from=python-env /.venv /.venv
ENV PATH=/.venv/bin:$PATH

COPY ./actor /app/actor

WORKDIR /app
