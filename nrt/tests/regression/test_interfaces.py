"""TST-05 Interface tests — validate Kafka and REST API transports.

These tests require Docker containers running (Postgres, Kafka, mdm-engine, mdm-api).
They verify that events sent via Kafka or REST produce correct results end-to-end.

Skip with: pytest -m "not integration"
"""

import json
import os
import time

import httpx
import pytest
from confluent_kafka import Producer, Consumer

from .contracts import SAMPLE_PAYLOADS, CDC_OUTPUT_SCHEMA

try:
    from jsonschema import validate
except ImportError:
    pytest.skip("jsonschema not installed", allow_module_level=True)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
API_BASE_URL = os.environ.get("MDM_API_URL", "http://localhost:8000")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")

GOLDEN_TOPIC = "topic.mdm.golden"


def _kafka_available():
    """Check if Kafka is reachable."""
    try:
        p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 2000})
        p.list_topics(timeout=2)
        return True
    except Exception:
        return False


def _api_available():
    """Check if REST API is reachable."""
    try:
        resp = httpx.get(f"{API_BASE_URL}/api/v1/health", timeout=2.0)
        return resp.status_code == 200
    except Exception:
        return False


skip_no_infra = pytest.mark.skipif(
    not _kafka_available() or not _api_available(),
    reason="Docker infrastructure not running (Kafka/API not reachable)",
)


# ---------------------------------------------------------------------------
# REST API Interface Tests
# ---------------------------------------------------------------------------

@skip_no_infra
class TestRESTInterface:
    """Test the REST API transport end-to-end."""

    def test_ingest_crm_a_returns_cdc(self):
        """POST /api/v1/ingest/crm_a with valid payload returns CDC conforming to schema."""
        ts = int(time.time() * 1000)
        payload = {
            "src_customer_id": f"REST-IT-A-{ts}",
            "first_name": "Resttest",
            "last_name": "Alphaone",
            "email": f"rest-a-{ts}@interface-test.com",
            "phone": f"+1800{ts % 10000000:07d}",
        }
        resp = httpx.post(
            f"{API_BASE_URL}/api/v1/ingest/crm_a",
            json=payload,
            timeout=30.0,
        )
        assert resp.status_code == 200
        result = resp.json()
        validate(instance=result, schema=CDC_OUTPUT_SCHEMA)
        assert result["event_type"] == "INSERT"
        assert result["changed"] is True
        assert "latency_ms" in result

    def test_ingest_crm_b_returns_cdc(self):
        """POST /api/v1/ingest/crm_b returns valid CDC."""
        ts = int(time.time() * 1000)
        payload = {
            "customer_key": f"REST-IT-B-{ts}",
            "name": "Resttest Betatwo",
            "email_address": f"rest-b-{ts}@interface-test.com",
            "mobile": f"+1801{ts % 10000000:07d}",
        }
        resp = httpx.post(
            f"{API_BASE_URL}/api/v1/ingest/crm_b",
            json=payload,
            timeout=30.0,
        )
        assert resp.status_code == 200
        result = resp.json()
        validate(instance=result, schema=CDC_OUTPUT_SCHEMA)
        assert result["event_type"] == "INSERT"

    def test_ingest_crm_c_returns_cdc(self):
        """POST /api/v1/ingest/crm_c returns valid CDC."""
        ts = int(time.time() * 1000)
        payload = {
            "ticket_customer_id": f"REST-IT-C-{ts}",
            "caller_name": "Resttest Gammathree",
            "callback_email": f"rest-c-{ts}@interface-test.com",
            "callback_phone": f"+1802{ts % 10000000:07d}",
        }
        resp = httpx.post(
            f"{API_BASE_URL}/api/v1/ingest/crm_c",
            json=payload,
            timeout=30.0,
        )
        assert resp.status_code == 200
        result = resp.json()
        validate(instance=result, schema=CDC_OUTPUT_SCHEMA)
        assert result["event_type"] == "INSERT"

    def test_invalid_source_returns_400(self):
        """POST to invalid source system returns 400."""
        resp = httpx.post(
            f"{API_BASE_URL}/api/v1/ingest/crm_invalid",
            json={"foo": "bar"},
            timeout=10.0,
        )
        assert resp.status_code == 400

    def test_get_customer_after_ingest(self):
        """GET /api/v1/customers/{id} returns the golden record after ingest."""
        ts = int(time.time() * 1000)
        payload = {
            "src_customer_id": f"REST-GET-{ts}",
            "first_name": "Gettest",
            "last_name": "Readback",
            "email": f"get-{ts}@interface-test.com",
            "phone": f"+1803{ts % 10000000:07d}",
        }
        resp = httpx.post(
            f"{API_BASE_URL}/api/v1/ingest/crm_a",
            json=payload,
            timeout=30.0,
        )
        assert resp.status_code == 200
        customer_id = resp.json()["customer_id"]

        # Read it back
        resp2 = httpx.get(
            f"{API_BASE_URL}/api/v1/customers/{customer_id}",
            timeout=10.0,
        )
        assert resp2.status_code == 200
        record = resp2.json()
        assert record["customer_id"] == customer_id
        assert record["first_name"] == "Gettest"

    def test_get_sources_after_ingest(self):
        """GET /api/v1/customers/{id}/sources returns source records."""
        ts = int(time.time() * 1000)
        payload = {
            "src_customer_id": f"REST-SRC-{ts}",
            "first_name": "Srctest",
            "last_name": "Sources",
            "email": f"src-{ts}@interface-test.com",
            "phone": f"+1804{ts % 10000000:07d}",
        }
        resp = httpx.post(
            f"{API_BASE_URL}/api/v1/ingest/crm_a",
            json=payload,
            timeout=30.0,
        )
        customer_id = resp.json()["customer_id"]

        resp2 = httpx.get(
            f"{API_BASE_URL}/api/v1/customers/{customer_id}/sources",
            timeout=10.0,
        )
        assert resp2.status_code == 200
        sources = resp2.json()
        assert len(sources) >= 1
        assert sources[0]["source_system"] == "CRM_A"

    def test_get_history_after_update(self):
        """GET /api/v1/customers/{id}/history returns SCD2 rows after update."""
        ts = int(time.time() * 1000)
        key = f"REST-HIST-{ts}"
        payload1 = {
            "src_customer_id": key,
            "first_name": "Histtest",
            "last_name": "History",
            "email": f"hist-{ts}@interface-test.com",
            "phone": f"+1805{ts % 10000000:07d}",
        }
        resp1 = httpx.post(f"{API_BASE_URL}/api/v1/ingest/crm_a", json=payload1, timeout=30.0)
        customer_id = resp1.json()["customer_id"]

        # Update
        payload2 = {**payload1, "email": f"hist-updated-{ts}@interface-test.com"}
        httpx.post(f"{API_BASE_URL}/api/v1/ingest/crm_a", json=payload2, timeout=30.0)

        resp3 = httpx.get(f"{API_BASE_URL}/api/v1/customers/{customer_id}/history", timeout=10.0)
        assert resp3.status_code == 200
        history = resp3.json()
        assert len(history) >= 2  # original + update

    def test_duplicate_returns_no_change(self):
        """Sending same event twice via REST returns NO_CHANGE on second call."""
        ts = int(time.time() * 1000)
        key = f"REST-DUP-{ts}"
        payload = {
            "src_customer_id": key,
            "first_name": "Dupzzztest",
            "last_name": "Nevermatches",
            "email": f"dup-unique-{ts}@zzz-no-match-domain-{ts}.com",
            "phone": f"+999{ts % 100000000000:011d}",
        }

        r1 = httpx.post(f"{API_BASE_URL}/api/v1/ingest/crm_a", json=payload, timeout=30.0)
        assert r1.json()["event_type"] in ("INSERT", "UPDATE"), f"Unexpected: {r1.json()}"

        r2 = httpx.post(f"{API_BASE_URL}/api/v1/ingest/crm_a", json=payload, timeout=30.0)
        assert r2.json()["event_type"] == "NO_CHANGE"
        assert r2.json()["changed"] is False

    def test_health_endpoint(self):
        """GET /api/v1/health returns ok."""
        resp = httpx.get(f"{API_BASE_URL}/api/v1/health", timeout=5.0)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Kafka Interface Tests
# ---------------------------------------------------------------------------

@skip_no_infra
class TestKafkaInterface:
    """Test the Kafka transport end-to-end (produce -> consume CDC)."""

    def _produce_and_wait_cdc(self, topic: str, key: str, payload: dict, timeout: float = 15.0) -> dict | None:
        """Produce a message to a CRM topic and wait for CDC on golden topic."""
        # Subscribe to golden topic BEFORE producing
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"test-kafka-if-{int(time.time() * 1000)}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([GOLDEN_TOPIC])
        # Warm up consumer — poll until assignment
        for _ in range(5):
            consumer.poll(timeout=1.0)
        time.sleep(1)

        # Produce event
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(payload).encode(),
            timestamp=int(time.time() * 1000),
        )
        producer.flush()

        # Poll for any CDC event (we just need one to arrive after our produce)
        deadline = time.time() + timeout
        cdc_event = None
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            try:
                evt = json.loads(msg.value())
                if "event_type" in evt:
                    cdc_event = evt
                    break
            except (json.JSONDecodeError, TypeError):
                pass

        consumer.close()
        return cdc_event

    def test_kafka_crm_a_produces_cdc(self):
        """Event on topic.crm.a produces a golden CDC event."""
        ts = int(time.time() * 1000)
        key = f"KFK-IT-A-{ts}"
        payload = {
            "src_customer_id": key,
            "first_name": "Kafkatest",
            "last_name": "Alpha",
            "email": f"kfk-a-{ts}@interface-test.com",
            "phone": f"+1900{ts % 10000000:07d}",
        }

        cdc = self._produce_and_wait_cdc("topic.crm.a", key, payload)
        assert cdc is not None, "No CDC event received within timeout"
        assert cdc.get("event_type") in ("INSERT", "UPDATE")

    def test_kafka_crm_b_produces_cdc(self):
        """Event on topic.crm.b produces a golden CDC event."""
        ts = int(time.time() * 1000)
        key = f"KFK-IT-B-{ts}"
        payload = {
            "customer_key": key,
            "name": "Kafkatest Beta",
            "email_address": f"kfk-b-{ts}@interface-test.com",
            "mobile": f"+1901{ts % 10000000:07d}",
        }

        cdc = self._produce_and_wait_cdc("topic.crm.b", key, payload)
        assert cdc is not None, "No CDC event received within timeout"
        assert cdc.get("event_type") in ("INSERT", "UPDATE")

    def test_kafka_crm_c_produces_cdc(self):
        """Event on topic.crm.c produces a golden CDC event."""
        ts = int(time.time() * 1000)
        key = f"KFK-IT-C-{ts}"
        payload = {
            "ticket_customer_id": key,
            "caller_name": "Kafkatest Gamma",
            "callback_email": f"kfk-c-{ts}@interface-test.com",
            "callback_phone": f"+1902{ts % 10000000:07d}",
        }

        cdc = self._produce_and_wait_cdc("topic.crm.c", key, payload)
        assert cdc is not None, "No CDC event received within timeout"
        assert cdc.get("event_type") in ("INSERT", "UPDATE")

    def test_kafka_update_produces_cdc_update(self):
        """Sending two events for same key produces UPDATE CDC."""
        ts = int(time.time() * 1000)
        key = f"KFK-UPD-{ts}"
        payload1 = {
            "src_customer_id": key,
            "first_name": "Kafkaupd",
            "last_name": "Update",
            "email": f"kfk-upd-{ts}@interface-test.com",
            "phone": f"+1903{ts % 10000000:07d}",
        }
        payload2 = {**payload1, "email": f"kfk-upd2-{ts}@interface-test.com"}

        # First insert
        cdc1 = self._produce_and_wait_cdc("topic.crm.a", key, payload1)
        assert cdc1 is not None

        # Update
        time.sleep(1)  # Ensure consumer resubscribes fresh
        cdc2 = self._produce_and_wait_cdc("topic.crm.a", key, payload2)
        assert cdc2 is not None
        assert cdc2.get("event_type") == "UPDATE"
