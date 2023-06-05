# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    name="Beam Pipeline",
    version="1.0",
    description="Python Apache Beam pipeline.",
    author="Tobi Kaymak",
    author_email="kaymak@google.com",
    packages=find_packages(),
    install_requires=requirements,
)
