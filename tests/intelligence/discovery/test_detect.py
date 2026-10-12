# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Sensitive-data suggestions: true positives, the false positives the rules are designed to avoid, evidence combination, and the guarantee that no value is ever returned."""

from __future__ import annotations

import json

import pytest

from olcf.intelligence.discovery import detect
from olcf.intelligence.model import AssetFact, AssetRef, Facts


def labels(name: str, data_type: str | None = "string", values: list | None = None, **kw) -> dict[str, float]:
    return {s["classification"]: s["confidence"] for s in detect.classify_column(name, data_type, values, **kw)}


# ------------------------------------------------------------------------------------------------ names
@pytest.mark.parametrize("name, data_type, expected", [
    ("email", "string", "email"), ("customerEmail", "string", "email"), ("CUSTOMER EMAIL", "varchar(100)", "email"), ("work_email", "string", "email"),
    ("phone_number", "string", "phone"), ("telephone", "bigint", "phone"), ("ssn", "string", "national_id"), ("Social_Security_Number", "string", "national_id"),
    ("credit_card_number", "string", "credit_card"), ("cvv", "string", "credit_card"), ("iban", "string", "iban"), ("account_number", "string", "bank_account"),
    ("client_ip", "string", "ip_address"), ("first_name", "string", "person_name"), ("surname", "string", "person_name"), ("street_address", "string", "address"),
    ("date_of_birth", "date", "date_of_birth"), ("dob", "timestamp", "date_of_birth"), ("latitude", "double", "geolocation"), ("gps_coordinates", "double", "geolocation"),
    ("password", "string", "credential"), ("api_key", "string", "credential"), ("client_secret", "binary", "credential"), ("salary", "decimal(10,2)", "financial"),
    ("ethnicity", "string", "special_category"), ("religion", "string", "special_category"),
])
def test_names_that_should_be_suggested(name, data_type, expected):
    found = labels(name, data_type)
    assert expected in found and found[expected] >= 0.5, found
    top = detect.classify_column(name, data_type)[0]
    assert top["evidence"] == "name" and top["category"] == detect.CATEGORY[top["classification"]] and top["rule"].startswith("name:")


@pytest.mark.parametrize("name, data_type", [
    ("email_count", "int"), ("email_sent_flag", "boolean"), ("password_reset_flag", "boolean"), ("password_policy", "string"), ("phone_model", "string"), ("classname", "string"),
    ("product_name", "string"), ("company_name", "string"), ("table_name", "string"), ("ip_range", "string"), ("order_id", "bigint"), ("amount", "double"), ("created_at", "timestamp"),
    ("primary_key", "string"), ("partition_key", "string"), ("card_brand", "string"), ("street_count", "int"), ("sex_ratio_note", "int"), ("long_term_debt", "double"),
    ("birth_country", "string"), ("mailbox_server", "string"), ("token_count", "int"), ("salary_band", "string"),
])
def test_names_that_merely_mention_a_sensitive_word_are_not_suggested(name, data_type):
    found = labels(name, data_type, min_confidence=0.5)
    assert found == {}, f"{name} -> {found}"


def test_wrong_type_lowers_confidence_but_a_matching_type_does_not():
    assert labels("email", "string")["email"] > labels("email", "boolean").get("email", 0)
    assert labels("latitude", "double")["geolocation"] > labels("latitude", "string").get("geolocation", 0)


def test_weak_names_stay_below_a_strict_threshold_and_tokenizer_handles_case_styles():
    assert labels("zip", "string", min_confidence=0.5).get("address") in (0.55, None) and "address" not in labels("zip", "string", min_confidence=0.9)
    assert detect.tokenize("customerEmailAddress") == detect.tokenize("customer_email_address") == detect.tokenize("CUSTOMER EMAIL ADDRESS") == ["customer", "email", "address"]
    assert detect.tokenize("HTTPServerIP") == ["http", "server", "ip"]


@pytest.mark.parametrize("t, fam", [("string", "string"), ("varchar(10)", "string"), ("bigint", "integer"), ("int", "integer"), ("double", "floating"), ("decimal(10,2)", "floating"),
                                     ("timestamp_ntz", "timestamp"), ("datetime2", "timestamp"), ("date", "date"), ("boolean", "boolean"), ("binary", "binary"), ("array<string>", "complex"),
                                     (None, "unknown"), ("", "unknown"), ("wibble", "unknown")])
def test_type_family(t, fam):
    assert detect.type_family(t) == fam


# ------------------------------------------------------------------------------------------------ values
VISA = "4111111111111111"


@pytest.mark.parametrize("fn, good, bad", [
    (detect.is_card_number, [VISA, "4111 1111 1111 1111", "5500-0000-0000-0004", "378282246310005"], ["4111111111111112", "1234567890123456", "0000000000000000", "12345", "abcd efgh ijkl mnop", ""]),
    (detect.is_iban, ["GB82WEST12345698765432", "DE89370400440532013000"], ["GB82WEST12345698765433", "XX00", "1234567890", "not an iban", ""]),
    (detect.is_us_ssn, ["123-45-6789", "078-05-1120"], ["000-12-3456", "666-12-3456", "900-12-3456", "123-00-6789", "123-45-0000", "123456789", "12-345-6789", ""]),
    (detect.is_email, ["a@b.co", "first.last+tag@sub.example.com"], ["a@b", "@example.com", "no at sign", "a@@b.com", ""]),
    (detect.is_phone, ["+44 20 7946 0958", "(555) 123-4567", "555-123-4567"], ["12345", "2024-01-15", "555-1234", "abcdefghij", ""]),
    (detect.is_ip, ["10.0.0.1", "192.168.1.255", "2001:db8::1"], ["256.1.1.1", "1.2.3", "10.0.0.1.5", "hello", ""]),
])
def test_validators(fn, good, bad):
    assert all(fn(v) for v in good), [v for v in good if not fn(v)]
    assert not any(fn(v) for v in bad), [v for v in bad if fn(v)]


def test_luhn():
    assert detect.luhn_valid(VISA) and not detect.luhn_valid("4111111111111112")


def test_values_are_detected_even_when_the_name_says_nothing():
    emails = [f"person{i}@corp.example" for i in range(10)]
    got = detect.classify_column("col_17", "string", emails)
    assert got[0]["classification"] == "email" and got[0]["evidence"] == "pattern" and got[0]["rule"] == "pattern:email"
    assert "national_id" in labels("x", "string", [f"{100 + i}-45-{6789 + i}" for i in range(10)])
    assert "credit_card" in labels("x", "string", [VISA, "5500000000000004", "340000000000009"])
    assert "iban" in labels("x", "string", ["GB82WEST12345698765432"])
    assert "ip_address" in labels("x", "string", ["10.0.0.1", "10.0.0.2", "10.0.0.3"])


def test_random_numbers_are_not_credit_cards_and_mostly_non_matching_columns_are_ignored():
    luhn_fail = [s for s in (str(2000000000000000 + i) for i in range(50)) if not detect.luhn_valid(s)]
    assert labels("order_ref", "string", luhn_fail) == {}
    mixed = ["alice@example.com", "bob@example.com"] + [f"note {i}" for i in range(20)]
    assert "email" not in labels("notes", "string", mixed)                                         # below the ratio and the minimum number of matching values
    assert labels("notes", "string", ["a@b.com"] * 2) == {}                                         # fewer than min_values


def test_name_and_value_evidence_combine_and_are_not_double_counted():
    emails = [f"u{i}@x.example" for i in range(8)]
    both = labels("email", "string", emails)["email"]
    assert both > labels("email", "string")["email"] and both > labels("c", "string", emails)["email"] and both <= 0.99
    top = detect.classify_column("email", "string", emails)[0]
    assert top["evidence"] == "pattern" and set(top["evidences"]) == {"pattern", "name"}
    again = labels("email", "string", emails + emails)["email"]
    assert again == both                                                                                # more of the same evidence adds nothing


def test_at_most_max_values_are_inspected():
    values = ["x"] * detect.MAX_VALUES + ["a@b.example"] * 1000
    assert "email" not in labels("col", "string", values[: detect.MAX_VALUES])
    assert detect.MAX_VALUES == 200


def test_results_never_contain_a_value():
    secret_values = [f"leak{i}@private.example" for i in range(10)] + [VISA, "123-45-6789"]
    out = detect.classify_column("whatever", "string", secret_values)
    blob = json.dumps(out)
    assert out and not any(v in blob for v in secret_values) and "private.example" not in blob
    assert "values" not in json.dumps(detect.classify_columns([{"name": "e", "type": "string", "values": secret_values}]))


# ------------------------------------------------------------------------------------------------ structures
def cols():
    return [{"name": "id", "type": "bigint"}, {"name": "email", "type": "string"}, {"name": "Salary", "type": "double"}, {"name": "email_count", "type": "int"}, {"name": "zip", "type": "string"}]


def test_classify_columns_and_sensitive_names():
    got = detect.classify_columns(cols())
    assert set(got) == {"email", "Salary", "zip"}
    assert detect.sensitive_column_names(cols()) == {"email", "salary", "zip"}
    assert detect.sensitive_column_names(cols(), min_confidence=0.8) == {"email", "salary"}
    assert detect.classify_columns([{"name": ""}, {"name": "id", "type": "int"}]) == {}


def test_suggest_classifications_for_every_subject_shape():
    table = {"name": "customers", "columns": cols()}
    s = detect.suggest_classifications(table)
    assert {(x.column, x.classification) for x in s} >= {("email", "email"), ("Salary", "financial")} and all(x.table == "customers" for x in s)
    assert {x.column for x in detect.suggest_classifications(cols())} == {"email", "Salary", "zip"}
    col = AssetFact(AssetRef.make("column", {"k": "c"}, "ssn"), "ssn", {"data_type": "string"})
    assert [x.classification for x in detect.suggest_classifications(col)] == ["national_id"]
    tab = AssetFact(AssetRef.make("table", {"k": "t"}, "t"), "t", {"columns": cols()})
    assert {x.column for x in detect.suggest_classifications(tab)} == {"email", "Salary", "zip"}
    assert detect.suggest_classifications(AssetFact(AssetRef.make("table", {"k": "t2"}, "t2"), "t2", {})) == []
    facts = Facts()
    facts.assets.extend([col, tab])
    assert len(detect.suggest_classifications(facts)) == 4
    with pytest.raises(TypeError):
        detect.suggest_classifications(42)
    d = s[0].to_dict()
    assert d["table"] == "customers" and d["column"] and "evidences" in d


def test_annotate_facts_attaches_suggestions_and_keeps_pattern_suggestions():
    existing = [{"classification": "credit_card", "confidence": 0.95, "evidence": "pattern", "evidences": ["pattern"], "rule": "pattern:credit-card", "category": "financial"}]
    tcols = cols()
    tcols[0] = {"name": "id", "type": "bigint", "suggested_classifications": existing}
    tab = AssetFact(AssetRef.make("table", {"k": "t"}, "t"), "t", {"columns": tcols})
    col = AssetFact(AssetRef.make("column", {"k": "c"}, "password"), "password", {"data_type": "string"})
    facts = Facts()
    facts.assets.extend([tab, col])
    n = detect.annotate_facts(facts)
    assert n >= 1
    assert col.attrs["suggested_classifications"][0]["classification"] == "credential"
    assert tab.attrs["sensitive_columns"] == ["id", "email", "Salary", "zip"]
    assert tab.attrs["columns"][0]["suggested_classifications"][0]["classification"] == "credit_card"
    clean = AssetFact(AssetRef.make("table", {"k": "t3"}, "t3"), "t3", {"columns": [{"name": "id", "type": "int"}]})
    f2 = Facts()
    f2.assets.append(clean)
    assert detect.annotate_facts(f2) == 0 and "sensitive_columns" not in clean.attrs
