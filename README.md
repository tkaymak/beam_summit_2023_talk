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

Running your pipeline in Python is as easy as running the tests directly.

```sh
# To run the pipeline as a Testpipeline and experience how it works through the logs.
python -m unittest -v

# To run passing command line arguments (these are ignored in the app.py right now).
python main.py --lookup_input_topic="🎉" --core_input_topic="🎉"


```

# License
Copyright 2023 Google LLC

Disclaimer: This is not an official Google product (experimental or otherwise), it is just code that happens to be owned by Google.

This software is distributed under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE](LICENSE) for details.
