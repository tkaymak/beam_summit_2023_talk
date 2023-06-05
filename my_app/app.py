# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

# Instead of pre-warming the stateful DoFn in the pipeline, 
# a script that starts the pipeline loads the state (e.g. from BigQuery) into Pub/Sub beforehand. 
# This can be a simple single-threaded Python script, since only the latest state can be loaded in the correct order using SQL. 
# It is important that the messages receive their original timestamp when they are published
# 
# The pipeline then starts and reads from both Pub/Sub topics (one with the core, one with the lookup updates)
# Both streams are mapped to NamedTuple
# A flatten unites the two PCollections
# A StatefulDoFn with timers reads the elements
# If the element has a Serial Number field, this is stored in the State
# Otherwise the state is read to get the serial number for the message
# If there is no state for the serial number (e.g. because of a time delay), buffer the message in a BufferState
# 
# The code for reading from Pub/Sub has been altered, the pipeline is using a TestStream so that the user can investigate how it works.
# IMPORTANT: Since there can not be two TestStreams in Python right now, both of the source streams are treated as one big source stream.
# Functions have been adjusted to distinguish between elements and a Filter has been added to filter empty elements.

import json
import logging
import typing
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import apache_beam as beam
from apache_beam import ParDo, coders
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream, TimestampedValue
from apache_beam.transforms.userstate import (BagStateSpec,
                                              CombiningValueStateSpec,
                                              TimeDomain, TimerSpec, on_timer)
from apache_beam.utils.timestamp import Duration, Timestamp


class LogElements(beam.PTransform):

  class _LoggingFn(beam.DoFn):

    def __init__(self, prefix=''):
      super(LogElements._LoggingFn, self).__init__()
      self.prefix = prefix

    def process(self, element, **kwargs):
      logging.info(self.prefix + str(element))
      yield element

  def __init__(self, label=None, prefix=''):
    super(LogElements, self).__init__(label)
    self.prefix = prefix

  def expand(self, input):
    input | beam.ParDo(self._LoggingFn(self.prefix))


class Util:

  @staticmethod
  def convert_object_to_bq_row(event) -> dict:
    return event._asdict()

  @staticmethod
  def parse_datetime(input_timestamp: str) -> datetime:
    try:
      dt = datetime.strptime(input_timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
      dt = datetime.strptime(input_timestamp, '%Y-%m-%dT%H:%M:%S%z')

    return dt


class CoreType(typing.NamedTuple):
  id: str
  color: str
  can_dance: bool
  current_serial: int
  timestamp: str

  @staticmethod
  def convert_json_to_core_event_object(input_json):
    if 'color' in input_json:
      return CoreType(
          id=input_json['id'],
          color=input_json['color'],
          can_dance=input_json['can_dance'],
          current_serial=0,
          timestamp=input_json['timestamp'],
      )

  def __lt__(self, other: object) -> bool:
    if not isinstance(other, CoreType):
      return NotImplemented

    dt1 = Util.parse_datetime(self.timestamp)
    dt2 = Util.parse_datetime(other.timestamp)

    return dt1 < dt2


class LookupType(typing.NamedTuple):
  id: str
  serial_number: int
  timestamp: str

  @staticmethod
  def convert_json_to_lookup_event_object(input_json):
    if 'serial_number' in input_json:
      return LookupType(
          id=input_json['id'],
          serial_number=int(input_json['serial_number']),
          timestamp=input_json['timestamp'],
      )

  def __lt__(self, other: object) -> bool:
    if not isinstance(other, LookupType):
      return NotImplemented

    dt1 = Util.parse_datetime(self.timestamp)
    dt2 = Util.parse_datetime(other.timestamp)

    return dt1 < dt2

def get_input_stream():
  stream = (
      TestStream()
      .add_elements([
          # timestamp value in seconds since epoch
          TimestampedValue(
              '{"id":123,"color":"gold","can_dance":true,"timestamp":"2023-05-28T10:19:27+00:00"}',
              timestamp=0,
          ),
          TimestampedValue(
              '{"id":123,"serial_number":321,"timestamp":"2023-05-28T10:19:27+00:00"}',
              timestamp=0,
          ),
          TimestampedValue(
              '{"id":124,"color":"blue","can_dance":false,"timestamp":"2023-05-28T10:19:28+00:00"}',
              timestamp=4,
          ),
          TimestampedValue(
              '{"id":124,"serial_number":422,"timestamp":"2023-05-28T10:19:28+00:00"}',
              timestamp=4,
          ),
          TimestampedValue(
              '{"id":125,"color":"grey","can_dance":true,"timestamp":"2023-05-28T10:19:29+00:00"}',
              timestamp=6,
          ),
          TimestampedValue(
              '{"id":123,"color":"yellow","can_dance":true,"timestamp":"2023-05-28T10:19:29+00:00"}',
              timestamp=6,
          ),
          TimestampedValue(
              '{"id":125,"serial_number":521,"timestamp":"2023-05-28T10:19:29+00:00"}',
              timestamp=6,
          ),
          TimestampedValue(
              '{"id":123,"serial_number":322,"timestamp":"2023-05-28T10:19:29+00:00"}',
              timestamp=6,
          ),
      ])
      .advance_watermark_to(6)
      .advance_processing_time(6)
      # Uncomment the following line to play with late data
      # .add_elements([
      #     TimestampedValue('{"id":124,"color":"violet","can_dance":false,"timestamp":"2023-05-28T10:19:27+00:00"}', timestamp=0),
      #     TimestampedValue('{"id":124,"serial_number":421,"timestamp":"2023-05-28T10:19:27+00:00"}', timestamp=0)
      # ])
      .advance_watermark_to(31)
      .advance_processing_time(31)
      .advance_watermark_to_infinity()
  )
  return stream

class StatefulJoinFn(beam.DoFn):
  BUFFER_TIMER = TimerSpec('expiry', TimeDomain.REAL_TIME)
  # Event time timer for Garbage Collection
  GC_TIMER = TimerSpec('gc_timer', TimeDomain.WATERMARK)

  CORE_BUFFER_BAG = BagStateSpec('core', coders.registry.get_coder(CoreType))
  CORE_COUNT_STATE = CombiningValueStateSpec('count_core', combine_fn=sum)

  LOOKUP_BUFFER_BAG = BagStateSpec(
      'lookup', coders.registry.get_coder(LookupType)
  )
  LOOKUP_COUNT_STATE = CombiningValueStateSpec('count_lookup', combine_fn=sum)

  def __init__(self):
    self.time_seconds = 30

  def process(
      self,
      input_element: Union[Tuple[str, CoreType], Tuple[str, LookupType]],
      element_timestamp=beam.DoFn.TimestampParam,
      core_count_state=beam.DoFn.StateParam(CORE_COUNT_STATE),
      core_state=beam.DoFn.StateParam(CORE_BUFFER_BAG),
      lookup_count_state=beam.DoFn.StateParam(LOOKUP_COUNT_STATE),
      lookup_state=beam.DoFn.StateParam(LOOKUP_BUFFER_BAG),
      timer=beam.DoFn.TimerParam(BUFFER_TIMER),
      gc_timer=beam.DoFn.TimerParam(GC_TIMER),
  ) -> Iterable[CoreType]:
    import time

    # 0. Read both streams, map to NamedTuple, Flatten the PCollections
    # Done, see below in pipeline construction
    # 1. identifiy the type of element
    element = input_element[1]
    if hasattr(element, 'serial_number'):
      # It is a lookup
      if lookup_count_state.read() == 0:
        # Start timer if no element exists already
        logging.info('Intializing lookup timer for key {}'.format(element.id))
        # timer.set(time.time() + self.time_seconds)
        timer.set(element_timestamp.seconds() + self.time_seconds)
        logging.info(
            'Element timestamp seconds: {}, self.time_seconds: {}'.format(
            element_timestamp.seconds(),
            self.time_seconds)
        )
        lookup_count_state.add(1)

      # Add lookup to bag
      lookup_state.add(element)

    elif hasattr(element, 'id'):
      # It is a Core
      if core_count_state.read() == 0:
        # Start timer if no element exists already
        logging.info('Intializing core timer for key {}'.format(element.id))
        # timer.set(time.time() + self.time_seconds)
        logging.info(
            'Element timestamp seconds: {}, self.time_seconds: {}'.format(
            element_timestamp.seconds(),
            self.time_seconds)
        )
        timer.set(element_timestamp.seconds() + self.time_seconds)
        core_count_state.add(1)

      # Add core to bag
      core_state.add(element)

    else:
      logging.error('Unknown element type')
      raise AttributeError

    # Clear all after 30 days
    expiration_time = Timestamp(micros=element_timestamp.micros) + Duration(
        seconds=60 * 60 * 24 * 30
    )
    gc_timer.set(expiration_time)

  def enrich_and_emit(
      core_state=beam.DoFn.StateParam(CORE_BUFFER_BAG),
      lookup_state=beam.DoFn.StateParam(LOOKUP_BUFFER_BAG),
  ) -> Iterable[CoreType]:

    # Read all lookups and build up their view:
    lookup_list: List[Lookup] = []
    lookup_dict: Dict[str, Lookup] = {}

    for lookup in lookup_state.read():
      lookup_list.append(lookup)

    # Sort the list, so that the newer value is overwriting the older value in
    # the next step
    lookup_list.sort()
    for lookup in lookup_list:
      lookup_dict[lookup.id] = lookup

    core_list: List[CoreType] = []

    # Enrich all core events with lookups
    for core in core_state.read():
      lookup = lookup_dict.get(core.id)
      if lookup:
        logging.info(
            'core.timestamp: {}, core.id: {}, lookup.serial_number: {}'.format(
            core.timestamp,
            core.id,
            lookup.serial_number)
        )
        yield CoreType(
            id=core.id,
            color=core.color,
            can_dance=core.can_dance,
            current_serial=lookup.serial_number,
            timestamp=core.timestamp,
        )
      else:
        # TODO: Raise alert if lookup is empty
        logging.warning('We have not found a lookup for key %s!', core.id)

  @on_timer(BUFFER_TIMER)
  def timer_fn(
      self,
      core_count_state=beam.DoFn.StateParam(CORE_COUNT_STATE),
      core_state=beam.DoFn.StateParam(CORE_BUFFER_BAG),
      lookup_count_state=beam.DoFn.StateParam(LOOKUP_COUNT_STATE),
      lookup_state=beam.DoFn.StateParam(LOOKUP_BUFFER_BAG),
  ):
    logging.info('Releasing buffer (from on_timer)')
    import time

    event = StatefulJoinFn.enrich_and_emit(core_state, lookup_state)
    for e in event:
      yield beam.window.TimestampedValue(
          e, Timestamp(micros=time.time()).seconds()
      )

    # Clear the core state variables
    core_count_state.clear()
    core_state.clear()

  @on_timer(GC_TIMER)
  def gc_timer_fn(
      self,
      core_count_state=beam.DoFn.StateParam(CORE_COUNT_STATE),
      core_state=beam.DoFn.StateParam(CORE_BUFFER_BAG),
      lookup_count_state=beam.DoFn.StateParam(LOOKUP_COUNT_STATE),
      lookup_state=beam.DoFn.StateParam(LOOKUP_BUFFER_BAG),
      buffer_timer=beam.DoFn.TimerParam(BUFFER_TIMER),
  ):
    logging.info('Releasing buffer (from gc_timer_fn)')
    event = StatefulJoinFn.enrich_and_emit(core_state, lookup_state)
    import time

    for e in event:
      yield beam.window.TimestampedValue(
          e, Timestamp(micros=time.time()).seconds()
      )

    # Clear all the state variables and the other timer
    # We need to know about for how long and how much information we need
    # to store to make an informed decision 
    core_count_state.clear()
    core_state.clear()
    lookup_count_state.clear()
    lookup_state.clear()
    buffer_timer.clear()

def run(lookup_input_topic: str, 
        core_input_topic:str,
        beam_options: Optional[PipelineOptions] = None,
        test: Callable[[beam.PCollection], None] = lambda _: None,
    ):

  logging.getLogger('JoinData').setLevel(logging.INFO)
  stream = get_input_stream()

  with beam.Pipeline(options=beam_options) as p:
  #with TestPipeline(options=beam_options) as p:

    stream_data = (p | 'Read Data' >> stream)

    core_data = (
        stream_data
        # | 'Read Core PubSub' >> beam.io.ReadFromPubSub(
        #     topic=known_args.core_input_topic)
        # | 'Map to Core Schema' >> beam.Map(lambda x: json.loads(x.decode("utf8"))).with_output_types(Core))
        # | 'Read Core' >> beam.Create(get_input_core())
        | 'Map to Core'
        >> beam.Map(lambda x: json.loads(x)).with_output_types(CoreType)
        | 'Parse Core Event'
        >> beam.Map(
            CoreType.convert_json_to_core_event_object
        ).with_output_types(CoreType)
        | 'Filter Empty Core' >> beam.Filter(lambda element: element is not None)
        | 'Create Keys with core_ids'
        >> beam.WithKeys(lambda core: core.id).with_output_types(
            Tuple[str, CoreType]
        )
    )

    lookup_data = (
        stream_data
        # | 'Read Lookup PubSub' >> beam.io.ReadFromPubSub(
        #    topic=known_args.lookup_input_topic)
        # | 'Map to Lookup Schema' >> beam.Map(lambda x: json.loads(x.decode("utf8"))).with_output_types(LookupType))
        # | 'Read Lookup' >> beam.Create(get_input_lookup())
        | 'Map to Lookup'
        >> beam.Map(lambda x: json.loads(x)).with_output_types(LookupType)
        | 'Parse Lookup Event'
        >> beam.Map(
            LookupType.convert_json_to_lookup_event_object
        ).with_output_types(LookupType)
        | 'Filter Empty Lookup' >> beam.Filter(lambda element: element is not None)
        | 'Create Keys with lookup_ids'
        >> beam.WithKeys(lambda lookup: lookup.id).with_output_types(
            Tuple[str, LookupType]
        )
    )

    merged_data = (core_data, lookup_data) |  beam.Flatten().with_output_types(Union[Tuple[int, CoreType], Tuple[int, LookupType]])

    enriched_data = (
        merged_data | 'Enrich2' >> ParDo(StatefulJoinFn()).with_input_types(Union[Tuple[int, CoreType], Tuple[int, LookupType]])
    )

    result = (enriched_data | 'Log' >> beam.Map(print))
    test(result)