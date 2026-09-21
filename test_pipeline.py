import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline import DeduplicateDoFn


def test_deduplication_logic():
    """Valida que la lógica de deduplicación reduzca elementos repetidos a uno solo."""
    input_data = [
        ("EV100", [{"event_id": "EV100", "user": "A"}, {"event_id": "EV100", "user": "A"}]),
        ("EV200", [{"event_id": "EV200", "user": "B"}])
    ]

    expected_output = [
        {"event_id": "EV100", "user": "A"},
        {"event_id": "EV200", "user": "B"}
    ]

    with TestPipeline() as p:
        pcoll = p | beam.Create(input_data)
        result = pcoll | beam.ParDo(DeduplicateDoFn())
        assert_that(result, equal_to(expected_output))


def test_unique_events_pass_through():
    """Valida que eventos unicos sin duplicados pasen intactos."""
    input_data = [
        ("EV300", [{"event_id": "EV300", "action": "click"}])
    ]

    expected_output = [
        {"event_id": "EV300", "action": "click"}
    ]

    with TestPipeline() as p:
        pcoll = p | beam.Create(input_data)
        result = pcoll | beam.ParDo(DeduplicateDoFn())
        assert_that(result, equal_to(expected_output))