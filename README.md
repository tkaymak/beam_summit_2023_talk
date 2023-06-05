# Apache Beam Example Pattern for Enriching a Stream using State and Timers for Python

This repo contains the code for the Apache Beam Summit talk 2023 Pattern for Enriching a Stream using State and Timers for Python.
Link to the Talk: https://beamsummit.org/sessions/2023/too-big-to-fail-a-beam-pattern-for-enriching-a-stream-using-state-and-timers/

## Before you begin

Make sure you have a [Python 3](https://www.python.org/) development environment ready.
If you don't, you can download and install it from the
[Python downloads page](https://www.python.org/downloads/).

```sh
# Create a new Python virtual environment.
python -m venv env

# Activate the virtual environment.
source env/bin/activate
```

Install the project's dependencies from the [`requirements.txt`](requirements.txt) file.

```py
pip install -U pip

# Install the project as a local package, this installs all the dependencies as well.
pip install -e .
```

> ℹ️ Once you are done, you can run the `deactivate` command to go back to your global Python installation.

### Running the pipeline

Running your pipeline in Python is as easy as running the script file directly.

```sh
# You can run the script file directly.
python main.py

# To run passing command line arguments.
python main.py --input-text="🎉"

# To run the tests.
python -m unittest -v
```

## GitHub Actions automated testing

This project already comes with automated testing via [GitHub Actions](https://github.com/features/actions).

To configure it, look at the [`.github/workflows/test.yaml`](.github/workflows/test.yaml) file.

## Using other runners

To keep this template small, it only includes the [Direct Runner](https://beam.apache.org/documentation/runners/direct/).

For a comparison of what each runner currently supports, look at the [Beam Capability Matrix](https://beam.apache.org/documentation/runners/capability-matrix/).

To add a new runner, visit the runner's page for instructions on how to include it.

## Contributing

Thank you for your interest in contributing!
All contributions are welcome! 🎉🎊

Please refer to the [`CONTRIBUTING.md`](CONTRIBUTING.md) file for more information.

# License

This software is distributed under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE](LICENSE) for details.
