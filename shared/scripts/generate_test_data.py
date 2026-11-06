#!/usr/bin/env python3
"""
generate_test_data.py
Test Data Generator for MDM Showcase

Generates synthetic CRM data that exercises all matching and survivorship rules:
- Deterministic matches (email, phone)
- Probabilistic matches (fuzzy name, address similarity, SOUNDEX)
- Survivorship scenarios (most recent, source priority, most complete)
- DQ rule violations and bonuses
"""

import csv
import os
import random
import shutil
import string
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional
from faker import Faker

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

fake = Faker()
Faker.seed(42)
random.seed(42)

DISPOSABLE_DOMAINS = ['mailinator.com', 'tempmail.com', 'guerrillamail.com', '10minutemail.com']
PLACEHOLDER_PHONES = ['0000000000', '1111111111', '1234567890']

@dataclass
class Customer:
    id: str
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    phone: Optional[str]

@dataclass
class Address:
    id: str
    customer_id: str
    street: Optional[str]
    city: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]


def generate_phone(format_type: str = 'full') -> str:
    """Generate phone in various formats to test normalization."""
    digits = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    if format_type == 'full':
        return f"+1{digits}"
    elif format_type == 'dashes':
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif format_type == 'parens':
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif format_type == 'short':
        return digits[3:]
    else:
        return digits


def typo_name(name: str) -> str:
    """Introduce typos for fuzzy matching tests (Jaro-Winkler ~85%)."""
    if len(name) < 3:
        return name
    ops = ['swap', 'drop', 'replace']
    op = random.choice(ops)
    idx = random.randint(1, len(name) - 2)
    if op == 'swap' and idx < len(name) - 1:
        return name[:idx] + name[idx+1] + name[idx] + name[idx+2:]
    elif op == 'drop':
        return name[:idx] + name[idx+1:]
    elif op == 'replace':
        similar = {'a': 'e', 'e': 'i', 'i': 'y', 'o': 'u', 's': 'z', 'c': 'k'}
        char = name[idx].lower()
        replacement = similar.get(char, char)
        return name[:idx] + replacement + name[idx+1:]
    return name


CORTEX_TEST_PAIRS = [
    ('William', 'Bill'),
    ('Robert', 'Bob'),
    ('Elizabeth', 'Liz'),
    ('Michael', 'Mike'),
    ('Richard', 'Dick'),
    ('James', 'Jim'),
    ('Margaret', 'Peggy'),
    ('Charles', 'Chuck'),
    ('Theodore', 'Ted'),
]

FAKE_NAMES = [
    ('Test', 'User'),
    ('Asdf', 'Qwerty'),
]


def generate_customers_crm_c(
    count: int,
    crm_a_customers: list[Customer],
    crm_b_customers: list[Customer],
    overlap_count_a: int,
    overlap_count_b: int
) -> list[Customer]:
    """Generate CRM C (Call Center) customers with overlap to A and B."""
    customers = []

    overlap_a_pool = crm_a_customers[len(CORTEX_TEST_PAIRS):]
    overlap_b_pool = crm_b_customers[len(CORTEX_TEST_PAIRS) + len(FAKE_NAMES):]
    overlap_a = random.sample(overlap_a_pool, min(overlap_count_a, len(overlap_a_pool)))
    overlap_b = random.sample(overlap_b_pool, min(overlap_count_b, len(overlap_b_pool)))

    idx = 1
    for orig in overlap_a:
        customers.append(_CRM_C_FROM_A[idx % 3](f"C{idx:06d}", orig))
        idx += 1

    for orig in overlap_b:
        customers.append(Customer(
            id=f"C{idx:06d}",
            first_name=orig.first_name or fake.first_name(),
            last_name=orig.last_name or fake.last_name(),
            email=orig.email,
            phone=generate_phone('parens')
        ))
        idx += 1

    unique_count = count - len(overlap_a) - len(overlap_b)
    for _ in range(unique_count):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email, phone = _crm_c_unique_contact(idx % 8)
        customers.append(Customer(id=f"C{idx:06d}", first_name=first_name, last_name=last_name,
                                  email=email, phone=phone))
        idx += 1

    return customers


def _normalized_phone_or_new(phone: str) -> str:
    """Same number re-formatted as +1XXXXXXXXXX (MATCH-D02), or a fresh one."""
    normalized = ''.join(filter(str.isdigit, phone))
    return f"+1{normalized[-10:]}" if len(normalized) >= 10 else generate_phone('full')


def _crm_c_same_email(cid: str, orig: Customer) -> Customer:
    return Customer(id=cid, first_name=orig.first_name, last_name=orig.last_name,
                    email=orig.email, phone=generate_phone('dashes'))


def _crm_c_upper_name_same_phone(cid: str, orig: Customer) -> Customer:
    first_name = orig.first_name.upper() if orig.first_name else fake.first_name()
    last_name = orig.last_name.upper() if orig.last_name else fake.last_name()
    email = fake.email()
    return Customer(id=cid, first_name=first_name, last_name=last_name,
                    email=email, phone=_normalized_phone_or_new(orig.phone))


def _crm_c_typo_first_name(cid: str, orig: Customer) -> Customer:
    first_name = typo_name(orig.first_name) if orig.first_name else fake.first_name()
    return Customer(id=cid, first_name=first_name, last_name=orig.last_name,
                    email=orig.email, phone=generate_phone('full'))


_CRM_C_FROM_A = (_crm_c_same_email, _crm_c_upper_name_same_phone, _crm_c_typo_first_name)


def _crm_c_unique_contact(dq_scenario: int) -> tuple[Optional[str], Optional[str]]:
    """(email, phone) for a non-overlapping CRM C customer, with DQ violations."""
    if dq_scenario == 0:
        return 'no-reply@callcenter.internal', generate_phone('full')
    if dq_scenario == 1:
        return fake.email(), 'N/A'
    if dq_scenario == 2:
        return None, generate_phone('full')
    email = fake.email()
    return email, generate_phone(random.choice(['full', 'dashes', 'parens']))


def generate_customers_crm_a(count: int) -> list[Customer]:
    """Generate CRM A customers with seeded Cortex AI test cases."""
    customers = []

    for idx, (canonical, _nickname) in enumerate(CORTEX_TEST_PAIRS):
        shared_last = fake.last_name()
        customers.append(Customer(
            id=f"A{idx+1:06d}",
            first_name=canonical,
            last_name=shared_last,
            email=f"{canonical.lower()}.{shared_last.lower()}@crm-a-{idx}.com",
            phone=generate_phone('full')
        ))

    start = len(CORTEX_TEST_PAIRS) + 1
    for i in range(start, count + 1):
        customers.append(Customer(
            id=f"A{i:06d}",
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            email=fake.email(),
            phone=generate_phone(random.choice(['full', 'dashes', 'parens']))
        ))
    return customers


def generate_customers_crm_b(
    count: int,
    crm_a_customers: list[Customer],
    overlap_count: int
) -> list[Customer]:
    """
    Generate CRM B customers with controlled overlap for testing.
    
    Test scenarios:
    - MATCH-D01: Same email
    - MATCH-D02: Same phone (different format)
    - MATCH-P01: Similar name (typos)
    - Survivorship: Various value combinations
    """
    customers = []
    
    for idx, (_canonical, nickname) in enumerate(CORTEX_TEST_PAIRS):
        orig = crm_a_customers[idx]
        customers.append(Customer(
            id=f"B{idx+1:06d}",
            first_name=nickname,
            last_name=orig.last_name,
            email=f"{nickname.lower()}.{orig.last_name.lower()}@crm-b-{idx}.com",
            phone=generate_phone('full')
        ))

    for fidx, (fake_first, fake_last) in enumerate(FAKE_NAMES):
        customers.append(Customer(
            id=f"B{len(CORTEX_TEST_PAIRS) + fidx + 1:06d}",
            first_name=fake_first,
            last_name=fake_last,
            email=fake.email(),
            phone=generate_phone('full')
        ))

    cortex_offset = len(CORTEX_TEST_PAIRS) + len(FAKE_NAMES)
    remaining_overlap = overlap_count - len(CORTEX_TEST_PAIRS)
    non_cortex_a = crm_a_customers[len(CORTEX_TEST_PAIRS):]
    overlap_customers = random.sample(non_cortex_a, min(remaining_overlap, len(non_cortex_a)))

    for i, orig in enumerate(overlap_customers):
        customers.append(_CRM_B_OVERLAP[i % 8](f"B{cortex_offset + i + 1:06d}", orig))

    unique_count = count - len(overlap_customers) - cortex_offset
    for i in range(unique_count):
        idx = cortex_offset + len(overlap_customers) + i + 1
        first_name = fake.first_name()
        last_name = fake.last_name()
        first_name, email, phone = _crm_b_unique_contact(i % 10, first_name, last_name)
        customers.append(Customer(
            id=f"B{idx:06d}",
            first_name=first_name if first_name else None,
            last_name=last_name,
            email=email,
            phone=phone
        ))

    return customers


# --- CRM B overlap scenarios (index = position % 8) -------------------------

def _crm_b_same_email(cid: str, orig: Customer) -> Customer:
    """MATCH-D01: different person data, same email."""
    first_name, last_name = fake.first_name(), fake.last_name()
    return Customer(id=cid, first_name=first_name, last_name=last_name,
                    email=orig.email, phone=generate_phone('full'))


def _crm_b_same_phone(cid: str, orig: Customer) -> Customer:
    """MATCH-D02: same phone, re-formatted."""
    first_name, last_name, email = fake.first_name(), fake.last_name(), fake.email()
    return Customer(id=cid, first_name=first_name, last_name=last_name,
                    email=email, phone=_normalized_phone_or_new(orig.phone))


def _crm_b_typo_names(cid: str, orig: Customer) -> Customer:
    """MATCH-P01: similar names (typos)."""
    first_name = typo_name(orig.first_name) if orig.first_name else fake.first_name()
    last_name = typo_name(orig.last_name) if orig.last_name else fake.last_name()
    email = fake.email()
    return Customer(id=cid, first_name=first_name, last_name=last_name,
                    email=email, phone=generate_phone('full'))


def _crm_b_empty_first_name(cid: str, orig: Customer) -> Customer:
    """Survivorship S2: empty first name falls back to the other source."""
    return Customer(id=cid, first_name='', last_name=orig.last_name,
                    email=orig.email, phone=generate_phone('full'))


def _crm_b_short_phone(cid: str, orig: Customer) -> Customer:
    """Survivorship S5: same name, name-based email, short phone."""
    domain = orig.email.split('@')[1] if orig.email and '@' in orig.email else 'example.com'
    if orig.first_name and orig.last_name:
        new_email = f"{orig.first_name.lower()}.{orig.last_name.lower()}@{domain}"
    else:
        new_email = fake.email()
    return Customer(id=cid, first_name=orig.first_name, last_name=orig.last_name,
                    email=new_email, phone=generate_phone('short'))


def _crm_b_disposable_email(cid: str, orig: Customer) -> Customer:
    """DQ-002: disposable email domain."""
    email = f"{fake.user_name()}@{random.choice(DISPOSABLE_DOMAINS)}"
    return Customer(id=cid, first_name=orig.first_name, last_name=orig.last_name,
                    email=email, phone=generate_phone('full'))


def _crm_b_placeholder_phone(cid: str, orig: Customer) -> Customer:
    """DQ-008: placeholder phone."""
    return Customer(id=cid, first_name=orig.first_name, last_name=orig.last_name,
                    email=orig.email, phone=random.choice(PLACEHOLDER_PHONES))


_CRM_B_OVERLAP = (
    _crm_b_same_email, _crm_b_same_phone, _crm_b_typo_names, _crm_b_same_email,
    _crm_b_empty_first_name, _crm_b_short_phone, _crm_b_disposable_email, _crm_b_placeholder_phone,
)


def _crm_b_unique_contact(dq_scenario: int, first_name: str, last_name: str) -> tuple[str, Optional[str], Optional[str]]:
    """(first_name, email, phone) for a non-overlapping CRM B customer, with DQ violations."""
    if dq_scenario == 0:
        return first_name, 'invalid-email-format', generate_phone('full')
    if dq_scenario == 1:
        email = f"{fake.user_name()}@{random.choice(DISPOSABLE_DOMAINS)}"
        return first_name, email, generate_phone('full')
    if dq_scenario == 2:
        email = fake.email()
        return '', email, generate_phone('full')
    if dq_scenario == 3:
        return first_name, fake.email(), 'abc123'
    if dq_scenario == 4:
        email = fake.email()
        return first_name, email, random.choice(PLACEHOLDER_PHONES)
    if dq_scenario == 5:
        return first_name, None, None
    if dq_scenario == 6:
        return first_name, f"{first_name.lower()}.{last_name.lower()}@company.com", generate_phone('full')
    email = fake.email()
    return first_name, email, generate_phone('full')


def generate_addresses_crm_c(
    customers: list[Customer],
    crm_a_addresses: list[Address],
    crm_a_customers: list[Customer]
) -> list[Address]:
    """Generate addresses for CRM C (Call Center) customers."""
    addresses = []
    addr_id = 1

    a_customer_addrs = {}
    for addr in crm_a_addresses:
        if addr.customer_id not in a_customer_addrs:
            a_customer_addrs[addr.customer_id] = []
        a_customer_addrs[addr.customer_id].append(addr)

    for cust in customers:
        if random.random() < 0.3:
            match_idx = int(cust.id[1:]) - 1
            if match_idx < len(crm_a_customers):
                orig_cust_id = crm_a_customers[match_idx].id
                if orig_cust_id in a_customer_addrs:
                    orig_addr = random.choice(a_customer_addrs[orig_cust_id])
                    addresses.append(Address(
                        id=f"AC{addr_id:06d}",
                        customer_id=cust.id,
                        street=orig_addr.street,
                        city=orig_addr.city,
                        postal_code=orig_addr.postal_code,
                        country=orig_addr.country
                    ))
                    addr_id += 1
                    continue

        addresses.append(Address(
            id=f"AC{addr_id:06d}",
            customer_id=cust.id,
            street=fake.street_address(),
            city=fake.city(),
            postal_code=fake.postcode(),
            country=random.choice(['US', 'CA', 'UK', 'DE', 'FR'])
        ))
        addr_id += 1

    return addresses


def generate_addresses_crm_a(customers: list[Customer]) -> list[Address]:
    """Generate addresses for CRM A customers."""
    addresses = []
    addr_id = 1
    
    for cust in customers:
        num_addresses = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
        
        for _ in range(num_addresses):
            addresses.append(Address(
                id=f"AA{addr_id:06d}",
                customer_id=cust.id,
                street=fake.street_address(),
                city=fake.city(),
                postal_code=fake.postcode(),
                country=random.choice(['US', 'CA', 'UK', 'DE', 'FR'])
            ))
            addr_id += 1
    
    return addresses


def generate_addresses_crm_b(
    customers: list[Customer],
    crm_a_addresses: list[Address],
    crm_a_customers: list[Customer]
) -> list[Address]:
    """Generate addresses for CRM B with some matching addresses."""
    addresses = []
    addr_id = 1
    
    a_customer_addrs = _addresses_by_customer(crm_a_addresses)

    for cust in customers:
        num_addresses = random.choices([1, 2], weights=[0.7, 0.3])[0]

        orig_addr = _matching_crm_a_address(cust, crm_a_customers, a_customer_addrs)
        if orig_addr is not None:
            if random.random() < 0.3:
                street = orig_addr.street.replace('Street', 'St.').replace('Avenue', 'Ave.')
            else:
                street = orig_addr.street
            addresses.append(Address(
                id=f"AB{addr_id:06d}",
                customer_id=cust.id,
                street=street,
                city=orig_addr.city,
                postal_code=orig_addr.postal_code,
                country=orig_addr.country
            ))
            addr_id += 1
            num_addresses -= 1

        for _ in range(num_addresses):
            street, city, postal_code, country = _random_crm_b_address(random.randint(0, 5))
            addresses.append(Address(
                id=f"AB{addr_id:06d}",
                customer_id=cust.id,
                street=street,
                city=city,
                postal_code=postal_code,
                country=country
            ))
            addr_id += 1

    return addresses


def _addresses_by_customer(addresses: list[Address]) -> dict[str, list[Address]]:
    by_customer: dict[str, list[Address]] = {}
    for addr in addresses:
        by_customer.setdefault(addr.customer_id, []).append(addr)
    return by_customer


def _matching_crm_a_address(cust: Customer, crm_a_customers: list[Customer],
                            a_customer_addrs: dict[str, list[Address]]) -> Optional[Address]:
    """With 50% chance, reuse an address of the CRM A customer at the same position."""
    match_idx = int(cust.id[1:]) - 1
    if match_idx >= len(crm_a_customers):
        return None
    orig_cust_id = crm_a_customers[match_idx].id
    if orig_cust_id in a_customer_addrs and random.random() < 0.5:
        return random.choice(a_customer_addrs[orig_cust_id])
    return None


def _random_crm_b_address(addr_scenario: int) -> tuple[str, Optional[str], str, str]:
    """(street, city, postal_code, country); scenarios 0/1 carry DQ violations."""
    if addr_scenario == 0:
        street = fake.street_address()[:3]
        city = fake.city()
        return street, city, fake.postcode(), 'US'
    if addr_scenario == 1:
        street = fake.street_address()
        return street, None, fake.postcode(), 'US'
    street = fake.street_address()
    city = fake.city()
    postal_code = fake.postcode()
    return street, city, postal_code, random.choice(['US', 'CA', 'UK', 'DE', 'FR'])


def write_customer_csv_a(customers: list[Customer], filepath: str):
    """Write CRM A customer CSV."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['src_customer_id', 'first_name', 'last_name', 'email', 'phone'])
        for c in customers:
            writer.writerow([c.id, c.first_name or '', c.last_name or '', c.email or '', c.phone or ''])


def write_customer_csv_b(customers: list[Customer], filepath: str):
    """Write CRM B customer CSV (different column structure)."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_key', 'name', 'email_address', 'mobile'])
        for c in customers:
            full_name = f"{c.first_name or ''} {c.last_name or ''}".strip()
            writer.writerow([c.id, full_name, c.email or '', c.phone or ''])


def write_address_csv_a(addresses: list[Address], filepath: str):
    """Write CRM A address CSV."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['src_address_id', 'src_customer_id', 'street', 'city', 'postal_code', 'country'])
        for a in addresses:
            writer.writerow([a.id, a.customer_id, a.street or '', a.city or '', a.postal_code or '', a.country or ''])


def write_customer_csv_c(customers: list[Customer], filepath: str):
    """Write CRM C (Call Center) customer CSV."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ticket_customer_id', 'caller_name', 'callback_email', 'callback_phone'])
        for c in customers:
            full_name = f"{c.first_name or ''} {c.last_name or ''}".strip()
            writer.writerow([c.id, full_name, c.email or '', c.phone or ''])


def write_address_csv_c(addresses: list[Address], filepath: str):
    """Write CRM C (Call Center) address CSV."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['addr_ref', 'ticket_customer_id', 'location', 'town', 'postcode', 'country'])
        for a in addresses:
            writer.writerow([a.id, a.customer_id, a.street or '', a.city or '', a.postal_code or '', a.country or ''])


def write_address_csv_b(addresses: list[Address], filepath: str):
    """Write CRM B address CSV (different column names)."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['addr_id', 'customer_key', 'address_line', 'city', 'zip', 'country_code'])
        for a in addresses:
            writer.writerow([a.id, a.customer_id, a.street or '', a.city or '', a.postal_code or '', a.country or ''])


def print_test_coverage_report(
    crm_a_customers: list[Customer],
    crm_b_customers: list[Customer],
    crm_c_customers: list[Customer],
    crm_a_addresses: list[Address],
    crm_b_addresses: list[Address],
    crm_c_addresses: list[Address]
):
    """Print report of test data coverage."""
    print("\n" + "="*70)
    print("TEST DATA GENERATION REPORT")
    print("="*70)
    
    print(f"\n{'Category':<30} {'Count':>10}")
    print("-"*40)
    print(f"{'CRM A Customers':<30} {len(crm_a_customers):>10}")
    print(f"{'CRM B Customers':<30} {len(crm_b_customers):>10}")
    print(f"{'CRM C Customers':<30} {len(crm_c_customers):>10}")
    print(f"{'Total Raw Records':<30} {len(crm_a_customers) + len(crm_b_customers) + len(crm_c_customers):>10}")
    print(f"{'CRM A Addresses':<30} {len(crm_a_addresses):>10}")
    print(f"{'CRM B Addresses':<30} {len(crm_b_addresses):>10}")
    print(f"{'CRM C Addresses':<30} {len(crm_c_addresses):>10}")
    
    print(f"\n{'Cortex Test Pairs (nickname)':<30} {len(CORTEX_TEST_PAIRS):>10}")
    print(f"{'Cortex Test Cases (fake name)':<30} {len(FAKE_NAMES):>10}")

    print("\n" + "-"*70)
    print("CORTEX AI TEST COVERAGE")
    print("-"*70)
    for canonical, nickname in CORTEX_TEST_PAIRS:
        print(f"  CRM_A: {canonical:<12} ↔ CRM_B: {nickname:<10}  (same last name, different email/phone)")
    for fake_first, fake_last in FAKE_NAMES:
        print(f"  Fake Name:  {fake_first} {fake_last}")

    print("\n" + "-"*70)
    print("MATCHING RULE COVERAGE")
    print("-"*70)
    
    crm_a_emails = {a.email for a in crm_a_customers if a.email}
    email_matches = sum(1 for b in crm_b_customers if b.email and b.email in crm_a_emails)
    print(f"MATCH-D01 (Email Exact):       ~{email_matches} pairs")
    
    def normalize_phone(p):
        return ''.join(filter(str.isdigit, p or ''))[-10:] if p else ''
    
    crm_a_phones = {normalize_phone(a.phone) for a in crm_a_customers if len(normalize_phone(a.phone)) >= 10}
    phone_matches = sum(1 for b in crm_b_customers 
                       if len(normalize_phone(b.phone)) >= 10 and normalize_phone(b.phone) in crm_a_phones)
    print(f"MATCH-D02 (Phone Normalized):  ~{phone_matches} pairs")
    print(f"MATCH-P01-P05 (Fuzzy):         ~{len(crm_b_customers) // 3} pairs (estimated)")
    
    print("\n" + "-"*70)
    print("SURVIVORSHIP RULE COVERAGE")
    print("-"*70)
    print("S1: Most Recent Name           - CRM B newer timestamps for overlaps")
    print("S2: Non-Empty Fallback         - Empty first_name in CRM B scenarios")
    print("S3: Source Priority Email      - Both sources have valid emails")
    print("S4: Validity Override          - CRM A with invalid email format")
    print("S5: Most Complete Phone        - Short vs E.164 format phones")
    print("S6: Null vs Non-null           - NULL values in various fields")
    
    print("\n" + "-"*70)
    print("DQ RULE COVERAGE")
    print("-"*70)
    
    invalid_emails = sum(1 for c in crm_b_customers if c.email and '@' not in c.email)
    disposable_emails = sum(1 for c in crm_b_customers 
                          if c.email and any(d in c.email for d in DISPOSABLE_DOMAINS))
    empty_names = sum(1 for c in crm_b_customers if not c.first_name or c.first_name == '')
    placeholder_phones = sum(1 for c in crm_b_customers if c.phone in PLACEHOLDER_PHONES)
    no_contact = sum(1 for c in crm_b_customers if not c.email and not c.phone)
    
    print(f"DQ-001 (Invalid Email):        {invalid_emails} records")
    print(f"DQ-002 (Disposable Domain):    {disposable_emails} records")
    print(f"DQ-003 (Empty First Name):     {empty_names} records")
    print(f"DQ-008 (Placeholder Phone):    {placeholder_phones} records")
    print(f"DQ-C01 (No Contact Method):    {no_contact} records")
    
    print("\n" + "="*70)
    print(f"Output files written to: {OUTPUT_DIR}")
    print("="*70 + "\n")


def generate_daily_updates_customers(
    base_customers: list[Customer],
    change_rate: float = 0.1
) -> list[Customer]:
    """
    Generate daily update file with changes to existing customers.
    
    SCD Type 2 test scenarios:
    - Email changes (triggers new version)
    - Phone changes (triggers new version)
    - Name corrections (triggers new version)
    - Address updates via linked addresses
    """
    updates = []
    num_changes = int(len(base_customers) * change_rate)
    customers_to_change = random.sample(base_customers, num_changes)

    for i, cust in enumerate(customers_to_change):
        updates.append(_CUSTOMER_UPDATES[i % 5](cust))

    return updates


# --- Daily customer update scenarios (index = position % 5) ------------------

def _customer_with(cust: Customer, **changes) -> Customer:
    return Customer(**{**vars(cust), **changes})


def _update_email(cust: Customer) -> Customer:
    return _customer_with(cust, email=fake.email())


def _update_phone(cust: Customer) -> Customer:
    return _customer_with(cust, phone=generate_phone('full'))


def _update_name_case(cust: Customer) -> Customer:
    first_name = cust.first_name.upper() if cust.first_name else fake.first_name()
    last_name = cust.last_name.upper() if cust.last_name else fake.last_name()
    return _customer_with(cust, first_name=first_name, last_name=last_name)


def _update_job_change(cust: Customer) -> Customer:
    if cust.first_name and cust.last_name:
        new_email = f"{cust.first_name.lower()}.{cust.last_name.lower()}@newcompany.com"
    else:
        new_email = fake.email()
    return _customer_with(cust, email=new_email, phone=generate_phone('full'))


def _update_last_name(cust: Customer) -> Customer:
    return _customer_with(cust, last_name=fake.last_name() if random.random() < 0.5 else cust.last_name)


_CUSTOMER_UPDATES = (_update_email, _update_phone, _update_name_case, _update_job_change, _update_last_name)


def generate_daily_updates_addresses(
    base_addresses: list[Address],
    change_rate: float = 0.15
) -> list[Address]:
    """
    Generate daily update file with address changes.
    
    SCD Type 2 test scenarios:
    - Customer moved to new address
    - Address corrections (typo fixes)
    - Country changes
    """
    updates = []
    num_changes = int(len(base_addresses) * change_rate)
    addresses_to_change = random.sample(base_addresses, num_changes)

    for i, addr in enumerate(addresses_to_change):
        updates.append(_ADDRESS_UPDATES[i % 4](addr))

    return updates


# --- Daily address update scenarios (index = position % 4) -------------------

def _address_with(addr: Address, **changes) -> Address:
    return Address(**{**vars(addr), **changes})


def _update_moved(addr: Address) -> Address:
    street = fake.street_address()
    city = fake.city()
    return _address_with(addr, street=street, city=city, postal_code=fake.postcode())


def _update_corrected(addr: Address) -> Address:
    if addr.street:
        street = addr.street.replace('St.', 'Street').replace('Ave.', 'Avenue')
    else:
        street = fake.street_address()
    return _address_with(addr, street=street, city=addr.city.upper() if addr.city else fake.city())


def _update_country(addr: Address) -> Address:
    return _address_with(addr, country=random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'ES', 'IT']))


def _update_postcode(addr: Address) -> Address:
    return _address_with(addr, postal_code=fake.postcode())


_ADDRESS_UPDATES = (_update_moved, _update_corrected, _update_country, _update_postcode)


# ---------------------------------------------------------------------------
# Scale configurations for reuse by load tests
# ---------------------------------------------------------------------------

SCALES = {
    "small": {"a": 600, "b": 400, "c": 500, "overlap_b": 180, "overlap_ca": 100, "overlap_cb": 50},
    "medium": {"a": 40_000, "b": 35_000, "c": 25_000, "overlap_b": 12_000, "overlap_ca": 6_000, "overlap_cb": 3_000},
    "large": {"a": 400_000, "b": 350_000, "c": 250_000, "overlap_b": 120_000, "overlap_ca": 60_000, "overlap_cb": 30_000},
}


def generate_all(scale: str = "small") -> tuple[list, list, list]:
    """Generate customer data at the given scale without writing CSVs.

    Returns (customers_a, customers_b, customers_c) — lists of Customer objects.
    Reuses all overlap, DQ violation, and matching logic from the main generator.
    """
    cfg = SCALES[scale]
    customers_a = generate_customers_crm_a(cfg["a"])
    customers_b = generate_customers_crm_b(
        count=cfg["b"],
        crm_a_customers=customers_a,
        overlap_count=cfg["overlap_b"],
    )
    customers_c = generate_customers_crm_c(
        count=cfg["c"],
        crm_a_customers=customers_a,
        crm_b_customers=customers_b,
        overlap_count_a=cfg["overlap_ca"],
        overlap_count_b=cfg["overlap_cb"],
    )
    return customers_a, customers_b, customers_c


def _out_dir(phase: str, system: str, kind: str) -> str:
    return os.path.join(OUTPUT_DIR, phase, system, kind)


def _out_file(phase: str, system: str, kind: str, date: str) -> str:
    plural = 'customers' if kind == 'customer' else 'addresses'
    return os.path.join(_out_dir(phase, system, kind), f'{date}_crm_{system.lower()}_{plural}.csv')


def _write_daily_updates(sources: list, update_date: str) -> None:
    """Generate and write one day of updates (all customer sources, then all address sources)."""
    cust_updates = [(s, generate_daily_updates_customers(c, change_rate=0.01), w)
                    for s, c, _, w, _ in sources]
    addr_updates = [(s, generate_daily_updates_addresses(a, change_rate=0.015), w)
                    for s, _, a, _, w in sources]
    for kind, updates in (('customer', cust_updates), ('address', addr_updates)):
        for system, rows, write in updates:
            if rows:
                write(rows, _out_file('update', system, kind, update_date))


def main():
    print("Generating test data for MDM Showcase...")
    
    if os.path.exists(OUTPUT_DIR):
        print(f"Cleaning output directory: {OUTPUT_DIR}")
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    crm_a_customers = generate_customers_crm_a(600)
    
    crm_b_customers = generate_customers_crm_b(
        count=400,
        crm_a_customers=crm_a_customers,
        overlap_count=180
    )
    
    crm_a_addresses = generate_addresses_crm_a(crm_a_customers)
    crm_b_addresses = generate_addresses_crm_b(crm_b_customers, crm_a_addresses, crm_a_customers)

    crm_c_customers = generate_customers_crm_c(
        count=500,
        crm_a_customers=crm_a_customers,
        crm_b_customers=crm_b_customers,
        overlap_count_a=100,
        overlap_count_b=50
    )
    crm_c_addresses = generate_addresses_crm_c(crm_c_customers, crm_a_addresses, crm_a_customers)

    # (system, customers, addresses, customer writer, address writer)
    sources = [
        ('A', crm_a_customers, crm_a_addresses, write_customer_csv_a, write_address_csv_a),
        ('B', crm_b_customers, crm_b_addresses, write_customer_csv_b, write_address_csv_b),
        ('C', crm_c_customers, crm_c_addresses, write_customer_csv_c, write_address_csv_c),
    ]
    for phase in ('initial', 'update'):
        for system, *_ in sources:
            for kind in ('customer', 'address'):
                os.makedirs(_out_dir(phase, system, kind), exist_ok=True)

    num_months = 1
    num_days = num_months * 30
    initial_date = (datetime.now(timezone.utc) - timedelta(days=num_days)).strftime('%Y-%m-%d')

    for system, customers, addresses, write_cust, write_addr in sources:
        write_cust(customers, _out_file('initial', system, 'customer', initial_date))
        write_addr(addresses, _out_file('initial', system, 'address', initial_date))

    print(f"Initial load: {initial_date}")
    print(f"Generating {num_days} days of updates...")

    for day in range(1, num_days + 1):
        update_date = (datetime.now(timezone.utc) - timedelta(days=num_days) + timedelta(days=day)).strftime('%Y-%m-%d')
        _write_daily_updates(sources, update_date)
        if day % 30 == 0:
            print(f"  Month {day // 30}: {update_date}")
    
    print_test_coverage_report(crm_a_customers, crm_b_customers, crm_c_customers, crm_a_addresses, crm_b_addresses, crm_c_addresses)
    
    print("\n" + "-"*70)
    print("SCD TYPE 2 TEST DATA SUMMARY")
    print("-"*70)
    print(f"Initial load:     {os.path.join(OUTPUT_DIR, 'initial')}/")
    print(f"                  {initial_date}_*.csv")
    print(f"Daily updates:    {os.path.join(OUTPUT_DIR, 'update')}/")
    print(f"                  {num_days} days ({num_months} months) of incremental changes")
    print("                  - ~1% customer changes per day")
    print("                  - ~1.5% address changes per day")
    print("\nDirectory structure:")
    for phase, system in [(p, s) for p in ('initial', 'update') for s in 'AB'] + [('initial', 'C'), ('update', 'C')]:
        for kind in ('customer', 'address'):
            print(f"  output/{phase}/{system}/{kind}/")
    print("-"*70 + "\n")


if __name__ == '__main__':
    main()
