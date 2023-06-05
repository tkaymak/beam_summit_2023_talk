# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions


from my_app import app


if __name__ == "__main__":
    
    pipeline_options = PipelineOptions(streaming=True)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    import argparse
    import logging

    logging.getLogger().setLevel(logging.INFO)
    # The arguments here are placeholders, the app.py needs to adjusted to make use of them
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--lookup_input_topic",
        default="",
        help="Lookup Pub/Sub Topic",
    )
    parser.add_argument(
        "--core_input_topic",
        default="",
        help="Core Pub/Sub Topic",
    )
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(save_main_session=True, setup_file="./setup.py")
    app.run(
        lookup_input_topic=args.lookup_input_topic,
        core_input_topic=args.core_input_topic,
        beam_options=beam_options,
    )