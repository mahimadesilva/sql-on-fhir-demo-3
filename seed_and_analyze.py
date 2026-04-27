"""
Seed the FHIR server with 20 synthetic patients and their conditions,
then use the SQL on FHIR $run endpoint (PostgreSQL database path) to
extract and analyze the most common conditions.

Usage:
    python scripts/seed_and_analyze.py            # seed + analyze
    python scripts/seed_and_analyze.py --skip-seed  # analyze only

Requirements:
    pip install requests
"""

import argparse
import random
import uuid
from collections import Counter

import requests

BASE_URL = "http://localhost:9090/fhir/r4"
HEADERS = {"Content-Type": "application/fhir+json"}

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
    "Michael", "Linda", "William", "Barbara", "David", "Susan",
    "Richard", "Jessica", "Joseph", "Sarah", "Thomas", "Karen",
    "Charles", "Lisa",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore",
    "Jackson", "Martin",
]

CITIES = [
    ("Springfield", "IL", "62701"),
    ("Madison",     "WI", "53703"),
    ("Portland",    "OR", "97201"),
    ("Austin",      "TX", "78701"),
    ("Denver",      "CO", "80201"),
    ("Nashville",   "TN", "37201"),
    ("Phoenix",     "AZ", "85001"),
    ("Columbus",    "OH", "43201"),
    ("Charlotte",   "NC", "28201"),
    ("Indianapolis","IN", "46201"),
]

CONDITION_POOL = [
    {"code": "38341003",  "display": "Hypertension"},
    {"code": "44054006",  "display": "Type 2 diabetes mellitus"},
    {"code": "195967001", "display": "Asthma"},
    {"code": "13645005",  "display": "Chronic obstructive pulmonary disease"},
    {"code": "35489007",  "display": "Depression"},
    {"code": "197480006", "display": "Anxiety disorder"},
    {"code": "414916001", "display": "Obesity"},
    {"code": "55822004",  "display": "Hyperlipidaemia"},
    {"code": "414545008", "display": "Ischaemic heart disease"},
    {"code": "396275006", "display": "Osteoarthritis"},
]


def generate_patients(n: int = 20) -> list[dict]:
    patients = []
    for i in range(1, n + 1):
        gender = random.choice(["male", "female"])
        year = random.randint(1950, 2005)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        def random_address(use: str) -> dict:
            city, state, postal = random.choice(CITIES)
            street_num = random.randint(100, 999)
            street_name = random.choice(["Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Elm St"])
            return {
                "use": use,
                "line": [f"{street_num} {street_name}"],
                "city": city,
                "state": state,
                "postalCode": postal,
                "country": "USA",
            }

        addresses = [random_address("home")]
        if random.random() < 0.4:
            addresses.append(random_address("work"))

        patients.append({
            "resourceType": "Patient",
            "id": f"pt-{i:03d}",
            "active": True,
            "name": [{"family": random.choice(LAST_NAMES), "given": [random.choice(FIRST_NAMES)]}],
            "gender": gender,
            "birthDate": f"{year}-{month:02d}-{day:02d}",
            "address": addresses,
        })
    return patients


def generate_conditions(patient_ids: list[str]) -> list[dict]:
    conditions = []
    for pid in patient_ids:
        count = random.randint(1, 3)
        selected = random.sample(CONDITION_POOL, count)
        for cond in selected:
            conditions.append({
                "resourceType": "Condition",
                "id": f"cond-{uuid.uuid4().hex[:8]}",
                "clinicalStatus": {
                    "coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                        "code": "active",
                        "display": "Active",
                    }]
                },
                "code": {
                    "coding": [{
                        "system": "http://snomed.info/sct",
                        "code": cond["code"],
                        "display": cond["display"],
                    }]
                },
                "subject": {"reference": f"Patient/{pid}"},
            })
    return conditions


def post_resource(resource: dict) -> dict:
    rtype = resource["resourceType"]
    resp = requests.post(f"{BASE_URL}/{rtype}", json=resource, headers=HEADERS)
    if resp.status_code != 201:
        raise RuntimeError(
            f"Failed to create {rtype}/{resource.get('id')}: "
            f"HTTP {resp.status_code} — {resp.text[:200]}"
        )
    return resp.json()


def seed_server(patients: list[dict], conditions: list[dict]) -> None:
    print(f"Seeding {len(patients)} patients …")
    for p in patients:
        post_resource(p)
        print(f"  ✓ Patient/{p['id']}  {p['name'][0]['given'][0]} {p['name'][0]['family']}")

    print(f"\nSeeding {len(conditions)} conditions …")
    for c in conditions:
        post_resource(c)
        display = c["code"]["coding"][0]["display"]
        print(f"  ✓ Condition/{c['id']}  {display}  → {c['subject']['reference']}")


def build_view_definition() -> dict:
    return {
        "resourceType": "ViewDefinition",
        "name": "ConditionAnalysis",
        "status": "active",
        "resource": "Condition",
        "select": [{
            "column": [
                {"name": "id",                "path": "id"},
                {"name": "patient_ref",       "path": "subject.reference"},
                {"name": "condition_code",    "path": "code.coding.first().code"},
                {"name": "condition_display", "path": "code.coding.first().display"},
                {"name": "clinical_status",   "path": "clinicalStatus.coding.first().code"},
            ]
        }],
    }


def run_view(view_def: dict) -> list[dict]:
    """POST $run with database path (no inline resource params — PostgreSQL only)."""
    payload = {
        "resourceType": "Parameters",
        "parameter": [{"name": "viewResource", "resource": view_def}],
    }
    resp = requests.post(f"{BASE_URL}/ViewDefinition/$run", json=payload, headers=HEADERS)
    if resp.status_code != 200:
        raise RuntimeError(
            f"$run failed: HTTP {resp.status_code} — {resp.text[:400]}"
        )
    return resp.json()


def analyze(rows: list[dict]) -> None:
    if not rows:
        print("\nNo condition rows returned from $run.")
        return

    counts = Counter(row.get("condition_display", "Unknown") for row in rows)
    total = sum(counts.values())
    ranked = counts.most_common()

    max_count = ranked[0][1]
    bar_width = 30

    print(f"\n{'─' * 60}")
    print(f"  CONDITION FREQUENCY ANALYSIS  ({total} total conditions across patients)")
    print(f"{'─' * 60}")
    print(f"  {'Rank':<5} {'Condition':<40} {'N':>4}  {'Bar'}")
    print(f"{'─' * 60}")
    for rank, (display, count) in enumerate(ranked, start=1):
        bar = "█" * round(count / max_count * bar_width)
        print(f"  {rank:<5} {display:<40} {count:>4}  {bar}")
    print(f"{'─' * 60}")

    top_name, top_count = ranked[0]
    print(f"\n  Most common: {top_name!r} ({top_count} patients)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed FHIR server and analyze conditions")
    parser.add_argument("--skip-seed", action="store_true",
                        help="Skip data generation and jump straight to analysis")
    args = parser.parse_args()

    if not args.skip_seed:
        patients = generate_patients(20)
        conditions = generate_conditions([p["id"] for p in patients])
        seed_server(patients, conditions)
        print("\nSeeding complete.")

    print("\nRunning SQL on FHIR ViewDefinition/$run …")
    view_def = build_view_definition()
    rows = run_view(view_def)
    print(f"  Received {len(rows)} rows from $run.")

    analyze(rows)


if __name__ == "__main__":
    main()
