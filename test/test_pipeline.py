import unittest

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import equal_to, assert_that
from apache_beam.transforms.window import FixedWindows
from dateutil import parser

from my_pipeline import TaxiPoint, json_to_taxi_point, add_timestamp, TaxiSession, FindSessions, \
    SessionReason


class TestEndToEndPipeline(unittest.TestCase):
    def test_stateful_dofn(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        data = """{"ride_id": "1", "point_idx": 59, "latitude": 40.73469, "longitude": -73.97916000000001, "timestamp": "2023-04-21T16:21:27.0-04:00", "meter_reading": 3.6609714, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "1", "point_idx": 72, "latitude": 40.73559, "longitude": -73.97947, "timestamp": "2023-04-21T16:22:24.40786-04:00", "meter_reading": 4.467626, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "1", "point_idx": 99, "latitude": 40.737860000000005, "longitude": -73.97783000000001, "timestamp": "2023-04-21T16:24:22.41146-04:00", "meter_reading": 6.142986, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "1", "point_idx": 209, "latitude": 40.743680000000005, "longitude": -73.98303, "timestamp": "2023-04-21T16:32:23.16685-04:00", "meter_reading": 12.968526, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "1", "point_idx": 223, "latitude": 40.74423, "longitude": -73.98432000000001, "timestamp": "2023-04-21T16:33:24.3539-04:00", "meter_reading": 13.837231, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "1", "point_idx": 237, "latitude": 40.74495, "longitude": -73.98501, "timestamp": "2023-04-21T16:34:25.54095-04:00", "meter_reading": 14.7059355, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "1", "point_idx": 264, "latitude": 40.746500000000005, "longitude": -73.98587, "timestamp": "2023-04-21T16:36:23.0-04:00", "meter_reading": 16.381296, "meter_increment": 0.06205036, "ride_status": "dropoff", "passenger_count": 2}
{"ride_id": "2", "point_idx": 223, "latitude": 40.74423, "longitude": -73.98432000000001, "timestamp": "2023-04-21T16:43:24.3539-04:00", "meter_reading": 13.837231, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "2", "point_idx": 237, "latitude": 40.74495, "longitude": -73.98501, "timestamp": "2023-04-21T16:44:25.54095-04:00", "meter_reading": 14.7059355, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "2", "point_idx": 278, "latitude": 40.74541000000001, "longitude": -73.98677, "timestamp": "2023-04-21T16:47:25.3539-04:00", "meter_reading": 17.25, "meter_increment": 0.06205036, "ride_status": "dropoff", "passenger_count": 2}
{"ride_id": "3", "point_idx": 264, "latitude": 40.746500000000005, "longitude": -73.98587, "timestamp": "2023-04-21T16:48:23.54455-04:00", "meter_reading": 16.381296, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}
{"ride_id": "3", "point_idx": 264, "latitude": 40.746500000000005, "longitude": -73.98587, "timestamp": "2023-04-21T16:48:23.54455-04:00", "meter_reading": 16.381296, "meter_increment": 0.06205036, "ride_status": "enroute", "passenger_count": 2}"""

        expected_output: list[TaxiSession] = [
            TaxiSession(
                ride_id='1',
                duration=15.0 * 60.0 - 4.0,
                start_timestamp="2023-04-21T16:21:27.0-04:00",
                end_timestamp="2023-04-21T16:36:23.0-04:00",
                n_points=7,
                start_status='enroute',
                end_status='dropoff',
                session_reason=SessionReason.DROPOFF_SEEN
            ),
            TaxiSession(
                ride_id='2',
                duration=4.0 * 60.0 + 1,
                start_timestamp="2023-04-21T16:43:24.3539-04:00",
                end_timestamp="2023-04-21T16:47:25.3539-04:00",
                n_points=3,
                start_status='enroute',
                end_status='dropoff',
                session_reason=SessionReason.DROPOFF_SEEN
            )]
            # ),
            # TaxiSession(ride_id='2',
            #             duration=0.0,
            #             start_timestamp='2023-04-21T16:56:23.54455-04:00',
            #             end_timestamp='2023-04-21T16:56:23.54455-04:00',
            #             n_points=2,
            #             start_status='enroute',
            #             end_status='enroute',
            #             session_reason=SessionReason.GARBAGE_COLLECTION
            #             )]

        ts: float = parser.parse(expected_output[0].start_timestamp).timestamp() - 1

        test_stream = TestStream().advance_watermark_to(ts).add_elements(
            data.split("\n")).advance_watermark_to_infinity()

        with TestPipeline(options=options) as p:
            lines: PCollection[str] = p | test_stream | beam.WindowInto(FixedWindows(6000))
            points: PCollection[TaxiPoint] = lines | "parse json strings" >> beam.Map(
                json_to_taxi_point)
            tstamp: PCollection[TaxiPoint] = points | "timestamping" >> beam.Map(add_timestamp)
            key: PCollection[tuple[str, TaxiPoint]] = tstamp | "key" >> beam.WithKeys(
                lambda e: e.ride_id)
            sessions: PCollection[TaxiSession] = key | "stats" >> beam.ParDo(FindSessions())
            count = sessions | beam.combiners.Count.Globally().without_defaults()

            assert_that(count, equal_to([2]), label="CheckCount")
            assert_that(sessions, equal_to(expected_output))


if __name__ == "__main__":
    unittest.main()
