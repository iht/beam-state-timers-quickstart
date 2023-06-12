#    Copyright 2023 Google LLC
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#        http://www.apache.org/licenses/LICENSE-2.0
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import enum
import json
from typing import Optional, Iterable, List, NamedTuple

import apache_beam as beam
from apache_beam import PCollection, TimeDomain
from apache_beam import coders
from apache_beam.io.fileio import WriteToFiles
from apache_beam.io.textio import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec, BagStateSpec, TimerSpec, \
    CombiningValueStateSpec, on_timer
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp
from dateutil import parser


# ---------------------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------------------

# Great intro to Beam schemas in python: https://www.youtube.com/watch?v=zx4p-UNSmrA

# First we create a class that inherits from NamedTuple, this is our Schema
#
# To actually create an instance of TaxiPoint you can leverage dictionary unpacking
# Let's say you have a dictionary d = {"ride_id": asdf, ...,"passenger_count": 8}
# This dictionary's keys match the fields of TaxiPoint. 
# In this case, you use dictionary unpacking '**' to make class construction easy.
# Dictionary unpacking is when passing a dictionary to a function, 
# the key-value pairs can be unpacked into keyword arguments in a function call 
# where the dictionary keys match the parameter names of the function.
# So the call to the constructor looks like ==> TaxiPoint(**d)
class TaxiPoint(NamedTuple):
    ride_id: str
    point_idx: int
    latitude: float
    longitude: float
    timestamp: str
    meter_reading: float
    meter_increment: float
    ride_status: str
    passenger_count: int


class SessionReason(enum.Enum):
    DROPOFF_SEEN = 1
    GARBAGE_COLLECTION = 2


class TaxiSession(NamedTuple):
    ride_id: str
    duration: float
    start_timestamp: str
    end_timestamp: str
    n_points: int
    start_status: str
    end_status: str
    session_reason: SessionReason


# Second we let Beam know about our Schema by registering it
beam.coders.registry.register_coder(TaxiPoint, beam.coders.RowCoder)
beam.coders.registry.register_coder(TaxiSession, beam.coders.RowCoder)


# ---------------------------------------------------------------------------------------
# Parsing functions
# ---------------------------------------------------------------------------------------

def json_to_taxi_point(s: str) -> TaxiPoint:
    d: dict = json.loads(s)
    return TaxiPoint(**d)


def add_timestamp(p: TaxiPoint) -> TaxiPoint:
    ts: float = parser.parse(p.timestamp).timestamp()
    return TimestampedValue(p, ts)


def max_timestamp_combine(input_iterable):
    return max(input_iterable, default=0)


# --------------------------------------------------------------------------------------
# TASK: Write a DoFn to find sessions using state & timers
# --------------------------------------------------------------------------------------

class FindSessions(beam.DoFn):
    """This DoFn applies state & timers to try to infer the session data from the received
    points."""

    # The state for the key
    KEY_STATE = ReadModifyWriteStateSpec('state', coders.StrUtf8Coder())

    # Elements bag of taxi ride events
    TAXI_RIDE_EVENTS_BAG = BagStateSpec('taxi_ride_events_bag',
                                        coders.registry.get_coder(TaxiPoint))

    # Event time timer for Garbage Collection
    GC_TIMER = TimerSpec('gc_timer', TimeDomain.WATERMARK)

    # The maximum element timestamp seen so far.
    MAX_TIMESTAMP = CombiningValueStateSpec('max_timestamp_seen', max_timestamp_combine)

    def process(self, element: tuple[str, TaxiPoint],
                element_timestamp=beam.DoFn.TimestampParam,
                key_state=beam.DoFn.StateParam(KEY_STATE),
                taxi_ride_events_bag=beam.DoFn.StateParam(TAXI_RIDE_EVENTS_BAG),
                max_timestamp_seen=beam.DoFn.StateParam(MAX_TIMESTAMP),
                gc_timer=beam.DoFn.TimerParam(GC_TIMER)) -> Iterable[TaxiSession]:
        key, taxi_point = element
        key_state.write(key)
        taxi_ride_events_bag.add(taxi_point)
        max_timestamp_seen.add(element_timestamp.micros)

        if taxi_point.ride_status == 'dropoff':
            session: TaxiSession = self._calculate_session(key_state.read(),
                                                           taxi_ride_events_bag.read(),
                                                           SessionReason.DROPOFF_SEEN)
            # Get max timestamp
            max_ts: Timestamp = Timestamp(micros=max_timestamp_seen.read())
            yield TimestampedValue(session, max_ts)

            taxi_ride_events_bag.clear()
            max_timestamp_seen.clear()
            gc_timer.clear()
        else:
            # Set the timer to be 30 minutes to keep track of inactive keys
            max_ts: int = Timestamp(micros=max_timestamp_seen.read()).seconds()
            expiration_seconds: int = max_ts + 60*30
            expiration_ts: Timestamp = Timestamp(seconds=expiration_seconds)
            gc_timer.set(expiration_ts)

    @on_timer(GC_TIMER)
    def expiry_callback(
            self,
            key_state=beam.DoFn.StateParam(KEY_STATE),
            taxi_ride_events_bag=beam.DoFn.StateParam(TAXI_RIDE_EVENTS_BAG),
            max_timestamp_seen=beam.DoFn.StateParam(MAX_TIMESTAMP)) -> Iterable[TaxiSession]:
        session: TaxiSession = self._calculate_session(key_state.read(),
                                                       taxi_ride_events_bag.read(),
                                                       SessionReason.GARBAGE_COLLECTION)

        max_ts: Timestamp = Timestamp(micros=max_timestamp_seen.read())
        yield TimestampedValue(session, max_ts)

        taxi_ride_events_bag.clear()
        max_timestamp_seen.clear()

    @staticmethod
    def _calculate_session(key: str,
                           taxi_ride_events_bag: Iterable[TaxiPoint],
                           session_reason: SessionReason) -> TaxiSession:
        points: List[TaxiPoint] = list(taxi_ride_events_bag)
        points.sort(key=lambda p: p.timestamp)
        first_event: TaxiPoint = points[0]
        last_event: TaxiPoint = points[-1]

        start_time: float = parser.parse(first_event.timestamp).timestamp()
        end_time: float = parser.parse(last_event.timestamp).timestamp()

        journey_length_seconds = end_time - start_time

        r = TaxiSession(ride_id=key,
                        duration=journey_length_seconds,
                        n_points=len(points),
                        start_timestamp=first_event.timestamp,
                        end_timestamp=last_event.timestamp,
                        start_status=first_event.ride_status,
                        end_status=last_event.ride_status,
                        session_reason=session_reason)

        return r


@beam.ptransform_fn
def taxi_stats_transform(json_strs: PCollection[str]) -> PCollection[TaxiSession]:
    points: PCollection[TaxiPoint] = json_strs | "parse json strings" >> beam.Map(
        json_to_taxi_point)
    tstamp: PCollection[TaxiPoint] = points | "timestamping" >> beam.Map(add_timestamp)
    key: PCollection[tuple[str, TaxiPoint]] = tstamp | "key" >> beam.WithKeys(
        lambda e: e.ride_id)
    sessions: PCollection[TaxiSession] = key | "stats" >> beam.ParDo(FindSessions())
    output: PCollection[TaxiSession] = sessions | "cep" >> beam.Filter(
        lambda s: s.duration > 3600)
    return output


# ------------------------------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------------------------------

def run(beam_options: Optional[PipelineOptions] = None):
    with beam.Pipeline(options=beam_options) as pipeline:
        rides: PCollection[str] = pipeline | "Read ndjson input" >> ReadFromText(
            file_pattern=beam_options.input_filename)
        calculations: PCollection[TaxiSession] = rides | "calculations" >> taxi_stats_transform()
        writeablecalculations: PCollection[str] = calculations | beam.Map(json.dumps)
        writeablecalculations | WriteToFiles(path=beam_options.output_filename)
