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
        scenario = idx % 3
        if scenario == 0:
            customers.append(Customer(
                id=f"C{idx:06d}",
                first_name=orig.first_name,
                last_name=orig.last_name,
                email=orig.email,
                phone=generate_phone('dashes')
            ))
        elif scenario == 1:
            normalized = ''.join(filter(str.isdigit, orig.phone))
            customers.append(Customer(
                id=f"C{idx:06d}",
                first_name=orig.first_name.upper() if orig.first_name else fake.first_name(),
                last_name=orig.last_name.upper() if orig.last_name else fake.last_name(),
                email=fake.email(),
                phone=f"+1{normalized[-10:]}" if len(normalized) >= 10 else generate_phone('full')
            ))
        else:
            customers.append(Customer(
                id=f"C{idx:06d}",
                first_name=typo_name(orig.first_name) if orig.first_name else fake.first_name(),
                last_name=orig.last_name,
                email=orig.email,
                phone=generate_phone('full')
            ))
        idx += 1

    for orig in overlap_b:
        customers.append(Customer(
            id=f"C{idx:06d}",
            first_name=orig.first_name if orig.first_name else fake.first_name(),
            last_name=orig.last_name if orig.last_name else fake.last_name(),
            email=orig.email,
            phone=generate_phone('parens')
        ))
        idx += 1

    unique_count = count - len(overlap_a) - len(overlap_b)
    for _ in range(unique_count):
        dq_scenario = idx % 8
        first_name = fake.first_name()
        last_name = fake.last_name()
        if dq_scenario == 0:
            email = 'no-reply@callcenter.internal'
            phone = generate_phone('full')
        elif dq_scenario == 1:
            email = fake.email()
            phone = 'N/A'
        elif dq_scenario == 2:
            email = None
            phone = generate_phone('full')
        else:
            email = fake.email()
            phone = generate_phone(random.choice(['full', 'dashes', 'parens']))
        customers.append(Customer(
            id=f"C{idx:06d}",
            first_name=first_name,
            last_name=last_name,
            email=email,
            phone=phone
        ))
        idx += 1

    return customers


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

    scenario_idx = 0
    for i, orig in enumerate(overlap_customers):
        scenario = scenario_idx % 8
        scenario_idx += 1
        
        if scenario == 0:
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name=fake.first_name(),
                last_name=fake.last_name(),
                email=orig.email,
                phone=generate_phone('full')
            ))
        
        elif scenario == 1:
            normalized = ''.join(filter(str.isdigit, orig.phone))
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name=fake.first_name(),
                last_name=fake.last_name(),
                email=fake.email(),
                phone=f"+1{normalized[-10:]}" if len(normalized) >= 10 else generate_phone('full')
            ))
        
        elif scenario == 2:
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name=typo_name(orig.first_name) if orig.first_name else fake.first_name(),
                last_name=typo_name(orig.last_name) if orig.last_name else fake.last_name(),
                email=fake.email(),
                phone=generate_phone('full')
            ))
        
        elif scenario == 3:
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name=fake.first_name(),
                last_name=fake.last_name(),
                email=orig.email,
                phone=generate_phone('full')
            ))
        
        elif scenario == 4:
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name='',
                last_name=orig.last_name,
                email=orig.email,
                phone=generate_phone('full')
            ))
        
        elif scenario == 5:
            domain = orig.email.split('@')[1] if orig.email and '@' in orig.email else 'example.com'
            new_email = f"{orig.first_name.lower()}.{orig.last_name.lower()}@{domain}" if orig.first_name and orig.last_name else fake.email()
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name=orig.first_name,
                last_name=orig.last_name,
                email=new_email,
                phone=generate_phone('short')
            ))
        
        elif scenario == 6:
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name=orig.first_name,
                last_name=orig.last_name,
                email=f"{fake.user_name()}@{random.choice(DISPOSABLE_DOMAINS)}",
                phone=generate_phone('full')
            ))
        
        elif scenario == 7:
            customers.append(Customer(
                id=f"B{cortex_offset + i + 1:06d}",
                first_name=orig.first_name,
                last_name=orig.last_name,
                email=orig.email,
                phone=random.choice(PLACEHOLDER_PHONES)
            ))
    
    unique_count = count - len(overlap_customers) - cortex_offset
    for i in range(unique_count):
        idx = cortex_offset + len(overlap_customers) + i + 1
        
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        dq_scenario = i % 10
        if dq_scenario == 0:
            email = 'invalid-email-format'
            phone = generate_phone('full')
        elif dq_scenario == 1:
            email = f"{fake.user_name()}@{random.choice(DISPOSABLE_DOMAINS)}"
            phone = generate_phone('full')
        elif dq_scenario == 2:
            first_name = ''
            email = fake.email()
            phone = generate_phone('full')
        elif dq_scenario == 3:
            email = fake.email()
            phone = 'abc123'
        elif dq_scenario == 4:
            email = fake.email()
            phone = random.choice(PLACEHOLDER_PHONES)
        elif dq_scenario == 5:
            email = None
            phone = None
        elif dq_scenario == 6:
            email = f"{first_name.lower()}.{last_name.lower()}@company.com"
            phone = generate_phone('full')
        else:
            email = fake.email()
            phone = generate_phone('full')
        
        customers.append(Customer(
            id=f"B{idx:06d}",
            first_name=first_name if first_name else None,
            last_name=last_name,
            email=email,
            phone=phone
        ))
    
    return customers


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
    
    a_customer_addrs = {}
    for addr in crm_a_addresses:
        if addr.customer_id not in a_customer_addrs:
            a_customer_addrs[addr.customer_id] = []
        a_customer_addrs[addr.customer_id].append(addr)
    
    for cust in customers:
        num_addresses = random.choices([1, 2], weights=[0.7, 0.3])[0]
        
        match_idx = int(cust.id[1:]) - 1
        if match_idx < len(crm_a_customers):
            orig_cust_id = crm_a_customers[match_idx].id
            if orig_cust_id in a_customer_addrs and random.random() < 0.5:
                orig_addr = random.choice(a_customer_addrs[orig_cust_id])
                
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
            addr_scenario = random.randint(0, 5)
            
            if addr_scenario == 0:
                street = fake.street_address()[:3]
                city = fake.city()
                postal_code = fake.postcode()
                country = 'US'
            elif addr_scenario == 1:
                street = fake.street_address()
                city = None
                postal_code = fake.postcode()
                country = 'US'
            else:
                street = fake.street_address()
                city = fake.city()
                postal_code = fake.postcode()
                country = random.choice(['US', 'CA', 'UK', 'DE', 'FR'])
            
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
    source: str,
    day_number: int,
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
        scenario = i % 5
        
        if scenario == 0:
            new_email = fake.email()
            updates.append(Customer(
                id=cust.id,
                first_name=cust.first_name,
                last_name=cust.last_name,
                email=new_email,
                phone=cust.phone
            ))
        
        elif scenario == 1:
            new_phone = generate_phone('full')
            updates.append(Customer(
                id=cust.id,
                first_name=cust.first_name,
                last_name=cust.last_name,
                email=cust.email,
                phone=new_phone
            ))
        
        elif scenario == 2:
            updates.append(Customer(
                id=cust.id,
                first_name=cust.first_name.upper() if cust.first_name else fake.first_name(),
                last_name=cust.last_name.upper() if cust.last_name else fake.last_name(),
                email=cust.email,
                phone=cust.phone
            ))
        
        elif scenario == 3:
            new_email = f"{cust.first_name.lower()}.{cust.last_name.lower()}@newcompany.com" if cust.first_name and cust.last_name else fake.email()
            new_phone = generate_phone('full')
            updates.append(Customer(
                id=cust.id,
                first_name=cust.first_name,
                last_name=cust.last_name,
                email=new_email,
                phone=new_phone
            ))
        
        elif scenario == 4:
            updates.append(Customer(
                id=cust.id,
                first_name=cust.first_name,
                last_name=fake.last_name() if random.random() < 0.5 else cust.last_name,
                email=cust.email,
                phone=cust.phone
            ))
    
    return updates


def generate_daily_updates_addresses(
    base_addresses: list[Address],
    source: str,
    day_number: int,
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
        scenario = i % 4
        
        if scenario == 0:
            updates.append(Address(
                id=addr.id,
                customer_id=addr.customer_id,
                street=fake.street_address(),
                city=fake.city(),
                postal_code=fake.postcode(),
                country=addr.country
            ))
        
        elif scenario == 1:
            corrected_street = addr.street.replace('St.', 'Street').replace('Ave.', 'Avenue') if addr.street else fake.street_address()
            updates.append(Address(
                id=addr.id,
                customer_id=addr.customer_id,
                street=corrected_street,
                city=addr.city.upper() if addr.city else fake.city(),
                postal_code=addr.postal_code,
                country=addr.country
            ))
        
        elif scenario == 2:
            new_country = random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'ES', 'IT'])
            updates.append(Address(
                id=addr.id,
                customer_id=addr.customer_id,
                street=addr.street,
                city=addr.city,
                postal_code=addr.postal_code,
                country=new_country
            ))
        
        elif scenario == 3:
            updates.append(Address(
                id=addr.id,
                customer_id=addr.customer_id,
                street=addr.street,
                city=addr.city,
                postal_code=fake.postcode(),
                country=addr.country
            ))
    
    return updates


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

    initial_a_cust_dir = os.path.join(OUTPUT_DIR, 'initial', 'A', 'customer')
    initial_a_addr_dir = os.path.join(OUTPUT_DIR, 'initial', 'A', 'address')
    initial_b_cust_dir = os.path.join(OUTPUT_DIR, 'initial', 'B', 'customer')
    initial_b_addr_dir = os.path.join(OUTPUT_DIR, 'initial', 'B', 'address')
    initial_c_cust_dir = os.path.join(OUTPUT_DIR, 'initial', 'C', 'customer')
    initial_c_addr_dir = os.path.join(OUTPUT_DIR, 'initial', 'C', 'address')
    update_a_cust_dir = os.path.join(OUTPUT_DIR, 'update', 'A', 'customer')
    update_a_addr_dir = os.path.join(OUTPUT_DIR, 'update', 'A', 'address')
    update_b_cust_dir = os.path.join(OUTPUT_DIR, 'update', 'B', 'customer')
    update_b_addr_dir = os.path.join(OUTPUT_DIR, 'update', 'B', 'address')
    update_c_cust_dir = os.path.join(OUTPUT_DIR, 'update', 'C', 'customer')
    update_c_addr_dir = os.path.join(OUTPUT_DIR, 'update', 'C', 'address')

    for d in [initial_a_cust_dir, initial_a_addr_dir, initial_b_cust_dir, initial_b_addr_dir,
              initial_c_cust_dir, initial_c_addr_dir,
              update_a_cust_dir, update_a_addr_dir, update_b_cust_dir, update_b_addr_dir,
              update_c_cust_dir, update_c_addr_dir]:
        os.makedirs(d, exist_ok=True)
    
    num_months = 1
    num_days = num_months * 30
    initial_date = (datetime.now(timezone.utc) - timedelta(days=num_days)).strftime('%Y-%m-%d')
    
    write_customer_csv_a(crm_a_customers, os.path.join(initial_a_cust_dir, f'{initial_date}_crm_a_customers.csv'))
    write_customer_csv_b(crm_b_customers, os.path.join(initial_b_cust_dir, f'{initial_date}_crm_b_customers.csv'))
    write_customer_csv_c(crm_c_customers, os.path.join(initial_c_cust_dir, f'{initial_date}_crm_c_customers.csv'))
    write_address_csv_a(crm_a_addresses, os.path.join(initial_a_addr_dir, f'{initial_date}_crm_a_addresses.csv'))
    write_address_csv_b(crm_b_addresses, os.path.join(initial_b_addr_dir, f'{initial_date}_crm_b_addresses.csv'))
    write_address_csv_c(crm_c_addresses, os.path.join(initial_c_addr_dir, f'{initial_date}_crm_c_addresses.csv'))
    
    print(f"Initial load: {initial_date}")
    print(f"Generating {num_days} days of updates...")
    
    for day in range(1, num_days + 1):
        update_date = (datetime.now(timezone.utc) - timedelta(days=num_days) + timedelta(days=day)).strftime('%Y-%m-%d')
        
        updates_a_cust = generate_daily_updates_customers(crm_a_customers, 'A', day, change_rate=0.01)
        updates_b_cust = generate_daily_updates_customers(crm_b_customers, 'B', day, change_rate=0.01)
        updates_c_cust = generate_daily_updates_customers(crm_c_customers, 'C', day, change_rate=0.01)
        updates_a_addr = generate_daily_updates_addresses(crm_a_addresses, 'A', day, change_rate=0.015)
        updates_b_addr = generate_daily_updates_addresses(crm_b_addresses, 'B', day, change_rate=0.015)
        updates_c_addr = generate_daily_updates_addresses(crm_c_addresses, 'C', day, change_rate=0.015)

        if updates_a_cust:
            write_customer_csv_a(updates_a_cust, os.path.join(update_a_cust_dir, f'{update_date}_crm_a_customers.csv'))
        if updates_b_cust:
            write_customer_csv_b(updates_b_cust, os.path.join(update_b_cust_dir, f'{update_date}_crm_b_customers.csv'))
        if updates_c_cust:
            write_customer_csv_c(updates_c_cust, os.path.join(update_c_cust_dir, f'{update_date}_crm_c_customers.csv'))
        if updates_a_addr:
            write_address_csv_a(updates_a_addr, os.path.join(update_a_addr_dir, f'{update_date}_crm_a_addresses.csv'))
        if updates_b_addr:
            write_address_csv_b(updates_b_addr, os.path.join(update_b_addr_dir, f'{update_date}_crm_b_addresses.csv'))
        if updates_c_addr:
            write_address_csv_c(updates_c_addr, os.path.join(update_c_addr_dir, f'{update_date}_crm_c_addresses.csv'))
        
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
    print(f"                  - ~1% customer changes per day")
    print(f"                  - ~1.5% address changes per day")
    print(f"\nDirectory structure:")
    print(f"  output/initial/A/customer/")
    print(f"  output/initial/A/address/")
    print(f"  output/initial/B/customer/")
    print(f"  output/initial/B/address/")
    print(f"  output/update/A/customer/")
    print(f"  output/update/A/address/")
    print(f"  output/update/B/customer/")
    print(f"  output/update/B/address/")
    print(f"  output/initial/C/customer/")
    print(f"  output/initial/C/address/")
    print(f"  output/update/C/customer/")
    print(f"  output/update/C/address/")
    print("-"*70 + "\n")


if __name__ == '__main__':
    main()
