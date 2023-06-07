# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

# For more information on unittest, see:
#   https://docs.python.org/3/library/unittest.html

import unittest
from unittest.mock import patch

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions


import my_app
from my_app import CoreType

@patch("apache_beam.Pipeline", TestPipeline)
@patch("builtins.print", lambda x: x)
class TestApp(unittest.TestCase):
    def test_run_direct_runner(self):
        # Note that the order of the elements doesn't matter.

        import logging

        logging.getLogger().setLevel(logging.INFO)

        pipeline_options = PipelineOptions(streaming=True)
        pipeline_options.view_as(SetupOptions).save_main_session = True
        pipeline_options.view_as(StandardOptions).streaming = True
        expected = [CoreType(id=123, color='gold', can_dance=True, current_serial=322, timestamp='2023-05-28T10:19:27+00:00'), 
                    CoreType(id=123, color='yellow', can_dance=True, current_serial=322, timestamp='2023-05-28T10:19:29+00:00'), 
                    CoreType(id=124, color='blue', can_dance=False, current_serial=422, timestamp='2023-05-28T10:19:28+00:00'), 
                    CoreType(id=125, color='grey', can_dance=True, current_serial=521, timestamp='2023-05-28T10:19:29+00:00')]
        my_app.run(
            lookup_input_topic="",
            core_input_topic="",
            beam_options=pipeline_options,
            test=lambda elements: assert_that(elements, equal_to(expected)),
        )


if __name__ == "__main__":
    unittest.main()
