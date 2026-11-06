"""Snapshot test for the seeded test-data generator.

The fingerprint was recorded before the complexity refactor (Faker 40.44.0,
pinned in local-ci/sonar-self-scan.sh). The generator must keep producing
byte-identical data for the same seed, because the bulk and NRT demos and
their expected match counts depend on it.
"""

import hashlib
import importlib.util
import random
from pathlib import Path

import pytest

faker = pytest.importorskip("faker")

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "generate_test_data.py"
EXPECTED_FINGERPRINT = "43213f4ad9fbc99934815365f2f3da7290b2290b9df074bf92948fb4fefac75c"


def _load():
    spec = importlib.util.spec_from_file_location("generate_test_data", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _generate(gen):
    faker.Faker.seed(42)
    random.seed(42)
    cust_a = gen.generate_customers_crm_a(600)
    cust_b = gen.generate_customers_crm_b(count=400, crm_a_customers=cust_a, overlap_count=180)
    addr_a = gen.generate_addresses_crm_a(cust_a)
    addr_b = gen.generate_addresses_crm_b(cust_b, addr_a, cust_a)
    cust_c = gen.generate_customers_crm_c(count=500, crm_a_customers=cust_a, crm_b_customers=cust_b,
                                          overlap_count_a=100, overlap_count_b=50)
    addr_c = gen.generate_addresses_crm_c(cust_c, addr_a, cust_a)
    updates = [
        gen.generate_daily_updates_customers(cust_b, change_rate=0.1),
        gen.generate_daily_updates_addresses(addr_b, change_rate=0.1),
    ]
    return [cust_a, cust_b, cust_c, addr_a, addr_b, addr_c, *updates]


def test_seeded_output_is_unchanged():
    data = _generate(_load())
    assert hashlib.sha256(repr(data).encode()).hexdigest() == EXPECTED_FINGERPRINT


def test_generator_covers_dq_scenarios():
    gen = _load()
    _, cust_b, cust_c, *_ = _generate(gen)
    assert any(c.email == "invalid-email-format" for c in cust_b)
    assert any(c.phone in gen.PLACEHOLDER_PHONES for c in cust_b)
    assert any(c.phone == "N/A" for c in cust_c)
    assert len(cust_b) == 400
    assert len(cust_c) == 500


@pytest.mark.parametrize("fmt, pattern", [
    ("full", r"^\+1\d{10}$"), ("dashes", r"^\d{3}-\d{3}-\d{4}$"),
    ("parens", r"^\(\d{3}\) \d{3}-\d{4}$"), ("short", r"^\d{7}$"), ("other", r"^\d{10}$"),
])
def test_phone_formats(fmt, pattern):
    import re
    assert re.match(pattern, _load().generate_phone(fmt))


def test_typo_name_keeps_short_names():
    assert _load().typo_name("Al") == "Al"


def test_main_writes_initial_and_daily_files(tmp_path, monkeypatch, capsys):
    gen = _load()
    monkeypatch.setattr(gen, "OUTPUT_DIR", str(tmp_path / "output"))
    faker.Faker.seed(42)
    random.seed(42)
    gen.main()

    out = tmp_path / "output"
    initial = sorted(p.name.split("_", 1)[1] for p in (out / "initial").rglob("*.csv"))
    assert initial == [f"crm_{s}_{k}.csv" for s in "abc" for k in ("addresses", "customers")]
    assert len(list((out / "update" / "B" / "customer").glob("*.csv"))) == 30
    header = (out / "initial" / "B" / "customer").glob("*.csv").__next__().read_text().splitlines()[0]
    assert header == "customer_key,name,email_address,mobile"
    assert "TEST DATA GENERATION REPORT" in capsys.readouterr().out
