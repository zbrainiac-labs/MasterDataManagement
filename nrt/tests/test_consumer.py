"""Unit tests for the Kafka consumer loop (no Kafka, no Postgres)."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from nrt_mdm import consumer


def _msg(topic="topic.crm.a", value=b'{"id": "A1"}', ts_ms=1_780_000_000_000, error=None):
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.value.return_value = value
    msg.timestamp.return_value = (1, ts_ms)
    msg.error.return_value = error
    return msg


class TestProcessMessage:
    def test_delegates_to_pipeline(self):
        with patch.object(consumer, "process_event") as process:
            consumer.process_message(_msg(), "conn", "producer")
        kwargs = process.call_args.kwargs
        assert kwargs["source_system"] == "crm_a"
        assert kwargs["payload"] == {"id": "A1"}
        assert kwargs["event_ts"] == datetime.fromtimestamp(1_780_000_000, tz=timezone.utc)
        assert (kwargs["pg_conn"], kwargs["producer"]) == ("conn", "producer")

    def test_unknown_topic_is_skipped(self):
        with patch.object(consumer, "process_event") as process:
            consumer.process_message(_msg(topic="topic.other"), None, None)
        process.assert_not_called()

    @pytest.mark.parametrize("value", [b"{not json", None])
    def test_malformed_payload_is_logged_and_skipped(self, value, caplog):
        with patch.object(consumer, "process_event") as process:
            consumer.process_message(_msg(value=value), None, None)
        process.assert_not_called()
        assert "Malformed JSON" in caplog.text


class TestRunConsumer:
    def _run(self, *polled, process_side_effect=None):
        kafka, conn = MagicMock(), MagicMock()
        kafka.poll.side_effect = [*polled, KeyboardInterrupt]
        with patch.object(consumer, "_create_consumer", return_value=kafka), \
                patch.object(consumer, "_create_producer", return_value=MagicMock()), \
                patch.object(consumer, "_get_pg_conn", return_value=conn), \
                patch.object(consumer, "KafkaError", MagicMock(_PARTITION_EOF=-191)), \
                patch.object(consumer, "process_message", side_effect=process_side_effect) as process:
            consumer.run_consumer()
        return kafka, conn, process

    def test_commits_each_processed_message(self):
        msg = _msg()
        kafka, conn, process = self._run(None, msg)
        process.assert_called_once()
        kafka.commit.assert_called_once_with(message=msg)
        kafka.close.assert_called_once()
        conn.close.assert_called_once()

    def test_errors_and_eof_are_not_processed(self):
        eof = MagicMock(**{"code.return_value": -191})
        other = MagicMock(**{"code.return_value": 1})
        _, _, process = self._run(_msg(error=eof), _msg(error=other))
        process.assert_not_called()

    def test_failed_message_rolls_back_and_is_not_committed(self):
        kafka, conn, _ = self._run(_msg(), process_side_effect=RuntimeError("boom"))
        conn.rollback.assert_called_once()
        kafka.commit.assert_not_called()

    def test_subscribes_to_all_mapped_topics(self):
        kafka, _, _ = self._run()
        assert set(kafka.subscribe.call_args.args[0]) == set(consumer.TOPIC_MAPPER)


def test_topic_map_matches_mappers():
    assert set(consumer.TOPIC_TO_SOURCE) == set(consumer.TOPIC_MAPPER)
    assert json.loads(_msg().value()) == {"id": "A1"}
