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

# License

This software is distributed under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE](LICENSE) for details.
