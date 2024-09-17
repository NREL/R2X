(installation)=
# Installation

Greetings, Traveler!

This guide should be sufficient to setup R2X for translating most of the models.
If it does not, please contact any R2X developer and we will promptly provide
assistance.

The easiest way to install R2X is by cloning the github repo in your local
disk. There are multiple ways of doing this, however, we recommend using the
bare Git CLI. For cloning the repo, you can use either of the following
commands:

```{eval-rst}
.. tabs::

    .. tab:: SSH

        .. code-block:: bash

            git clone git@github.nrel.gov:PCM/R2X.git

    .. tab:: HTTPS

        .. code-block:: bash

            git clone https://github.nrel.gov/PCM/R2X.git
```

We recommend setting up a ssh key for your github account to avoid writing your password everytime you want to pull/push changes from the main branch.
If you have not, follow this [instructions](https://docs.github.com/en/authentication/connecting-to-github-with-ssh).

```{note}
In a near future, we are going to distribute R2X as a PyPi package. Stay tuned!
```

### Configuring Python environment

There are multiple ways of setting up your Python environment.
However, we highly suggest to use [Anaconda](https://www.anaconda.com/download)
or [Mamba](https://mamba.readthedocs.io/en/latest/) since it solves most of the
issues with packaging.
You are free to use your favorite python version manager
to install the Python version and the R2X dependencies. Here is a list of snippets you could use:

```{eval-rst}
.. tabs::
    .. tab:: mamba

        .. code-block:: console

            mamba env create -f environment.yml && mamba activate r2x

    .. tab:: conda

        .. code-block:: console

            conda env create -f environment.yml && conda activate r2x

    .. tab:: pip
        .. code-block:: console

            python -m pip install .

```

Remember that you need to be in the exact folder where you downloaded the repo to run the following command.

```{important}
If you just installed the package manager for the first time and are
running on Windows, you may first have to run the following and restart
your shell: `mamba init powershell`
```

To verify your installation, try to run the following command:

```console
python -m r2x --version
```
