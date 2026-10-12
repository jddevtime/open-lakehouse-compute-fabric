# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Sensitive-data detection: suggest classifications from column names, column types and (optionally) sample values. PURE: no database, no network, no logging of values.

What it is - and is not
-----------------------
* Everything returned is a **suggestion** with a confidence between 0 and 1, the kind of evidence (``name`` | ``pattern`` | ``type``) and the name of the rule that fired. A
  governance review queue turns suggestions into real classifications; nothing in this module classifies anything by itself, and nothing here ever enforces anything.
* Sample values, when given, are inspected in memory and **discarded**: results contain labels, ratios and rule names, never a value (this is asserted by the tests).
* Detection is heuristic. Name rules are token based (``customerEmail``, ``customer_email`` and ``CUSTOMER EMAIL`` all tokenise to ``customer, email``) so ``classname`` does not
  match ``ssn``; value rules validate their candidates (Luhn checksum and issuer prefix for card numbers, mod-97 for IBANs, real address parsing for IPs, area/group/serial
  rules for US social security numbers) so a random 16-digit order number is not reported as a card. Columns whose name merely *mentions* a sensitive word
  (``email_count``, ``password_reset_flag``, ``product_name``) are not reported. Misses are expected (fully lower-case run-together names such as ``customerssn``); false
  positives are possible, which is why the output is a suggestion.

Labels (``classification``): ``email``, ``phone``, ``national_id``, ``credit_card``, ``iban``, ``bank_account``, ``ip_address``, ``person_name``, ``address``,
``date_of_birth``, ``geolocation``, ``credential``, ``financial``, ``special_category``. Each also has a ``category`` (``pii``, ``financial``, ``credential``, ``location``,
``sensitive``) used by the review queue for grouping.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from olcf.intelligence.model import AssetFact, Facts

#: Suggestions below this confidence are not attached to assets (they are still returned by :func:`classify_column` when asked for).
DEFAULT_THRESHOLD = 0.5
#: At most this many sample values per column are inspected.
MAX_VALUES = 200
#: Evidence kinds, strongest first.
EVIDENCE_ORDER = ("pattern", "name", "type")

CATEGORY = {
    "email": "pii", "phone": "pii", "national_id": "pii", "person_name": "pii", "address": "pii", "date_of_birth": "pii", "ip_address": "pii",
    "credit_card": "financial", "iban": "financial", "bank_account": "financial", "financial": "financial",
    "credential": "credential", "geolocation": "location", "special_category": "sensitive",
}

# --------------------------------------------------------------------------------------------- types
_TYPE_FAMILIES = (
    ("boolean", ("boolean", "bool", "bit")),
    ("integer", ("tinyint", "smallint", "bigint", "byte", "short", "integer", "long", "int8", "int16", "int32", "int64", "uint", "int", "number")),
    ("floating", ("double", "float", "decimal", "numeric", "real", "money", "currency")),
    ("timestamp", ("timestamp", "datetime", "datetimeoffset", "datetime2")),
    ("date", ("date",)),
    ("binary", ("binary", "varbinary", "bytes", "blob")),
    ("string", ("string", "varchar", "nvarchar", "char", "nchar", "text", "utf8", "large_string", "uuid", "guid")),
    ("complex", ("array", "map", "struct", "json", "object", "variant")),
)


def type_family(data_type: str | None) -> str:
    """Coarse family of a column type string from Delta/Spark, Arrow, SQL or Power BI (``string``, ``integer``, ``floating``, ``boolean``, ``date``, ``timestamp``, ``binary``,
    ``complex``) or ``unknown`` when it is empty or unrecognised."""
    if not data_type:
        return "unknown"
    t = str(data_type).strip().lower()
    head = re.split(r"[\s(<\[,]", t, maxsplit=1)[0]
    if head.startswith("timestamp") or head.startswith("datetime"):
        return "timestamp"
    for fam, names in _TYPE_FAMILIES:
        if head in names:
            return fam
    for fam, names in _TYPE_FAMILIES:
        if any(head.startswith(n) for n in names if len(n) > 3):
            return fam
    return "unknown"


# --------------------------------------------------------------------------------------------- name rules
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def tokenize(name: str) -> list[str]:
    """Lower-case tokens of a column name: camelCase, snake_case, kebab-case, dotted and spaced names all split into words (``clientIP`` -> ``client``, ``ip``)."""
    spaced = _CAMEL.sub(" ", name)
    return [t.lower() for t in re.split(r"[^A-Za-z0-9]+", spaced) if t]


@dataclass(frozen=True)
class NameRule:
    """A column-name rule: ``phrases`` are ``_``-joined token sequences that must appear contiguously in the tokens; ``negative`` tokens veto the rule (the name is about a
    *property* of the data, not the data); ``types`` are the type families the rule expects (any other known family lowers the confidence to a fifth)."""

    rule: str
    classification: str
    confidence: float
    phrases: tuple[str, ...]
    negative: frozenset[str] = frozenset()
    types: frozenset[str] | None = None


_QUALIFIERS = frozenset({"count", "cnt", "flag", "status", "type", "kind", "template", "sent", "valid", "verified", "optin", "opt", "enabled", "format", "domain", "length", "len",
                         "regex", "pattern", "mask", "masked", "source", "version", "is", "has", "can", "should", "num", "number_of", "total", "avg", "sum", "ratio"})
_STR = frozenset({"string", "unknown", "complex"})
_STR_INT = frozenset({"string", "integer", "unknown"})
_STR_DATE = frozenset({"string", "date", "timestamp", "unknown"})
_FLOAT = frozenset({"floating", "string", "unknown", "integer"})

NAME_RULES: tuple[NameRule, ...] = (
    NameRule("name:email", "email", 0.9, ("email", "e_mail", "email_address", "emailaddress", "mail_address", "contact_email", "work_email"), _QUALIFIERS | {"id", "body", "subject"}, _STR),
    NameRule("name:email-weak", "email", 0.55, ("mail",), _QUALIFIERS | {"id", "box", "server", "body", "subject", "merge", "room", "order", "queue"}, _STR),
    NameRule("name:phone", "phone", 0.85, ("phone", "telephone", "phone_number", "phone_no", "msisdn", "cellphone", "mobile_phone", "mobile_number", "contact_number", "fax_number", "tel_no"),
             _QUALIFIERS | {"model", "brand", "maker", "os", "app", "id"}, _STR_INT),
    NameRule("name:phone-weak", "phone", 0.5, ("mobile", "tel", "cell"), _QUALIFIERS | {"app", "os", "device", "platform", "channel", "network", "model", "web", "id", "carrier", "operator"}, _STR_INT),
    NameRule("name:national-id", "national_id", 0.9, ("ssn", "social_security", "social_security_number", "national_id", "national_insurance", "nino", "tax_id", "tin", "taxpayer_id",
                                                       "passport", "passport_number", "drivers_license", "driver_license", "driving_licence", "license_number", "aadhaar", "aadhar", "cpf",
                                                       "curp", "personnummer", "national_identifier", "sin_number", "id_card"), _QUALIFIERS | {"issuer", "authority", "expiry", "country"}, _STR_INT),
    NameRule("name:credit-card", "credit_card", 0.9, ("credit_card", "creditcard", "card_number", "cardnumber", "card_no", "cc_number", "cc_num", "ccn", "debit_card", "pan_number", "card_pan",
                                                     "cvv", "cvc", "card_security_code"), _QUALIFIERS | {"brand", "network", "issuer", "scheme", "last4", "expiry", "holder"}, _STR_INT),
    NameRule("name:iban", "iban", 0.9, ("iban", "iban_number", "iban_code"), _QUALIFIERS, _STR),
    NameRule("name:bank-account", "bank_account", 0.75, ("bank_account", "account_number", "acct_number", "routing_number", "sort_code", "swift_code", "bic_code", "bank_account_number"),
             _QUALIFIERS | {"manager", "owner", "age", "tenure", "status", "open", "closed", "balance"}, _STR_INT),
    NameRule("name:ip", "ip_address", 0.85, ("ip_address", "ipaddress", "ip_addr", "ipv4", "ipv6", "client_ip", "remote_addr", "remote_ip", "source_ip", "src_ip", "dest_ip", "dst_ip", "user_ip",
                                             "ip"), _QUALIFIERS | {"range", "block", "whitelist", "allowlist", "denylist", "country", "geo", "owner"}, _STR),
    NameRule("name:person-name", "person_name", 0.85, ("first_name", "firstname", "last_name", "lastname", "surname", "given_name", "givenname", "family_name", "middle_name", "full_name",
                                                       "fullname", "maiden_name", "customer_name", "client_name", "employee_name", "patient_name", "person_name", "contact_name", "fname", "lname",
                                                       "legal_name", "preferred_name", "beneficiary_name", "cardholder_name", "account_holder"),
             _QUALIFIERS | {"file", "table", "product", "store", "brand", "company", "business", "department", "column", "schema", "host", "domain", "segment", "tier"}, _STR),
    NameRule("name:address", "address", 0.8, ("street", "street_address", "address_line", "address_line1", "address_line2", "addr_line", "home_address", "billing_address", "shipping_address",
                                              "mailing_address", "postal_address", "residence"), _QUALIFIERS | {"ip", "email", "mac", "web", "url", "mail"}, _STR),
    NameRule("name:address-weak", "address", 0.55, ("address", "addr", "zip", "zipcode", "zip_code", "postal_code", "postcode", "post_code"), _QUALIFIERS | {"ip", "email", "mac", "web", "url", "mail", "memory",
                                                                                                                                                   "contract", "wallet", "block", "bitcoin", "eth", "crypto", "base", "return"}, _STR_INT),
    NameRule("name:dob", "date_of_birth", 0.9, ("dob", "date_of_birth", "birth_date", "birthdate", "birthday", "birth_day", "born_on", "d_o_b", "date_birth"), _QUALIFIERS - {"is"}, _STR_DATE),
    NameRule("name:dob-weak", "date_of_birth", 0.55, ("birth_year", "year_of_birth", "birth_month", "birth"), _QUALIFIERS | {"place", "country", "city", "weight", "order", "certificate", "rate"}, frozenset({"string", "integer", "date", "timestamp", "unknown"})),
    NameRule("name:geo", "geolocation", 0.8, ("latitude", "longitude", "geo_location", "geolocation", "gps", "gps_coordinates", "coordinates", "geohash", "lat_long", "lat_lng", "lat_lon", "location_coordinates"),
             _QUALIFIERS - {"is"}, _FLOAT | {"complex"}),
    NameRule("name:geo-short", "geolocation", 0.6, ("lat", "lon", "lng", "long_deg", "lat_deg"), _QUALIFIERS | {"ms", "long_term", "term"}, _FLOAT),
    NameRule("name:credential", "credential", 0.9, ("password", "passwd", "pwd", "passphrase", "secret", "client_secret", "api_key", "apikey", "api_secret", "access_key", "secret_key", "private_key",
                                                    "auth_token", "access_token", "refresh_token", "bearer_token", "connection_string", "credential", "credentials", "sas_token", "otp", "pin_code", "salt"),
             _QUALIFIERS - {"is"} | {"reset", "policy", "strength", "expiry", "expires", "changed", "attempts", "hint", "rule", "required", "complexity"}, None),
    NameRule("name:credential-weak", "credential", 0.55, ("token", "key", "pin"), _QUALIFIERS | {"reset", "policy", "primary", "foreign", "partition", "sort", "cache", "index", "hash_key", "surrogate", "business", "natural",
                                                                                          "merge", "join", "lookup", "dedup", "row", "record", "unique", "composite", "shard", "bucket", "map", "mapping", "idx", "id", "ids", "code", "name", "value", "words"}, _STR),
    NameRule("name:financial", "financial", 0.8, ("salary", "salaries", "wage", "wages", "income", "annual_income", "compensation", "net_worth", "credit_score", "hourly_rate", "pay_rate", "base_pay",
                                                   "take_home", "payroll", "bonus_amount", "stipend", "household_income"), _QUALIFIERS | {"tax", "band", "grade"}, frozenset({"floating", "integer", "string", "unknown"})),
    NameRule("name:special-category", "special_category", 0.75, ("ethnicity", "race", "religion", "sexual_orientation", "political_opinion", "political_party", "union_membership", "disability",
                                                                 "diagnosis", "medical_condition", "health_condition", "criminal_record", "trade_union"), _QUALIFIERS, _STR),
    NameRule("name:special-category-weak", "special_category", 0.5, ("gender", "sex", "marital_status", "nationality", "citizenship"), _QUALIFIERS - {"status"} | {"category", "code"}, _STR),
)

# --------------------------------------------------------------------------------------------- value rules
_EMAIL = re.compile(r"^[A-Za-z0-9._%+'-]{1,64}@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$")
_SSN = re.compile(r"^(\d{3})-(\d{2})-(\d{4})$")
_PHONE_INTL = re.compile(r"^\+\d[\d\s().-]{6,18}\d$")
_PHONE_US = re.compile(r"^\(?\d{3}\)?[-. ]\d{3}[-. ]\d{4}$")
_IBAN = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$")
_DIGITS_CARD = re.compile(r"^[\d -]{13,23}$")
_CARD_PREFIX_LEN = (
    (re.compile(r"^4"), (13, 16, 19)),
    (re.compile(r"^(5[1-5]|2(2[2-9][1-9]|2[3-9]\d|[3-6]\d\d|7[01]\d|720))"), (16,)),
    (re.compile(r"^3[47]"), (15,)),
    (re.compile(r"^(6011|65|64[4-9]|622)"), (16, 17, 18, 19)),
    (re.compile(r"^35(2[89]|[3-8]\d)"), (16, 17, 18, 19)),
    (re.compile(r"^(30[0-5]|36|38|39)"), (14, 15, 16, 17, 18, 19)),
    (re.compile(r"^(62|81)"), (16, 17, 18, 19)),
)


def luhn_valid(digits: str) -> bool:
    """Luhn checksum of a digit string."""
    if not digits.isdigit():
        return False
    total, alt = 0, False
    for ch in reversed(digits):
        d = ord(ch) - 48
        if alt:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        alt = not alt
    return total % 10 == 0


def is_card_number(value: str) -> bool:
    """A payment card number: 13-19 digits (spaces / dashes allowed), a plausible issuer prefix for the length, a valid Luhn checksum and not one repeated digit."""
    v = value.strip()
    if not _DIGITS_CARD.match(v):
        return False
    digits = re.sub(r"[ -]", "", v)
    if not 13 <= len(digits) <= 19 or len(set(digits)) == 1:
        return False
    if not any(p.match(digits) and len(digits) in lens for p, lens in _CARD_PREFIX_LEN):
        return False
    return luhn_valid(digits)


def is_iban(value: str) -> bool:
    """An IBAN: country code, check digits, 15-34 characters and a valid ISO 7064 mod-97 check."""
    v = re.sub(r"\s", "", value.strip()).upper()
    if not _IBAN.match(v) or not 15 <= len(v) <= 34:
        return False
    rearranged = v[4:] + v[:4]
    num = "".join(str(int(c, 36)) for c in rearranged)
    return int(num) % 97 == 1


def is_us_ssn(value: str) -> bool:
    """A US social security number in ``AAA-GG-SSSS`` form with the rules the SSA publishes (area not 000/666/9xx, group not 00, serial not 0000)."""
    m = _SSN.match(value.strip())
    if not m:
        return False
    area, group, serial = m.groups()
    return area not in ("000", "666") and not area.startswith("9") and group != "00" and serial != "0000"


def is_email(value: str) -> bool:
    """A syntactically plausible e-mail address."""
    v = value.strip()
    return len(v) <= 254 and bool(_EMAIL.match(v))


def is_phone(value: str) -> bool:
    """A *formatted* phone number (leading ``+`` international form, or US ``(555) 123-4567`` / ``555-123-4567``). Bare digit strings are not reported: they are mostly ids."""
    v = value.strip()
    if _PHONE_US.match(v):
        return True
    if _PHONE_INTL.match(v):
        digits = re.sub(r"\D", "", v)
        return 8 <= len(digits) <= 15
    return False


def is_ip(value: str) -> bool:
    """An IPv4 (four dotted octets) or IPv6 address."""
    v = value.strip()
    try:
        if ":" in v:
            ipaddress.IPv6Address(v.split("%")[0])
            return True
        if v.count(".") == 3:
            ipaddress.IPv4Address(v)
            return True
    except ValueError:
        return False
    return False


@dataclass(frozen=True)
class ValueRule:
    """A value rule: ``check(value)`` decides one value; the rule fires when at least ``min_ratio`` of the (at least ``min_values``) non-empty values pass."""

    rule: str
    classification: str
    check: Any
    min_ratio: float = 0.6
    min_values: int = 3
    base: float = 0.9


VALUE_RULES: tuple[ValueRule, ...] = (
    ValueRule("pattern:email", "email", is_email),
    ValueRule("pattern:credit-card", "credit_card", is_card_number, min_values=1, base=0.95),
    ValueRule("pattern:iban", "iban", is_iban, min_values=1, base=0.95),
    ValueRule("pattern:us-ssn", "national_id", is_us_ssn, min_ratio=0.7),
    ValueRule("pattern:phone", "phone", is_phone, min_ratio=0.7, base=0.85),
    ValueRule("pattern:ip", "ip_address", is_ip, base=0.85),
)


def _stringify(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    for v in values:
        if v is None:
            continue
        s = v if isinstance(v, str) else str(v)
        s = s.strip()
        if s:
            out.append(s[:256])
        if len(out) >= MAX_VALUES:
            break
    return out


# --------------------------------------------------------------------------------------------- results
@dataclass(frozen=True)
class Suggestion:
    """A suggested classification for one column (or table). Contains no data values."""

    classification: str
    confidence: float
    evidence: str
    rule: str
    column: str | None = None
    table: str | None = None
    category: str = ""
    evidences: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """JSON form: ``{classification, category, confidence, evidence, evidences, rule[, column][, table]}``."""
        d: dict[str, Any] = {"classification": self.classification, "category": self.category or CATEGORY.get(self.classification, "pii"), "confidence": self.confidence,
                             "evidence": self.evidence, "evidences": list(self.evidences or (self.evidence,)), "rule": self.rule}
        if self.column is not None:
            d["column"] = self.column
        if self.table is not None:
            d["table"] = self.table
        return d


def _combine(a: float, b: float) -> float:
    return round(1 - (1 - a) * (1 - b), 3)


def classify_column(name: str, data_type: str | None = None, values: Sequence[Any] | None = None, *, min_confidence: float = 0.35) -> list[dict[str, Any]]:
    """Classifications suggested for one column, strongest first: ``[{classification, category, confidence, evidence, evidences, rule}]``.

    ``name`` and ``data_type`` give the *name* and *type* evidence; ``values`` (optional, at most 200 are looked at, never stored) give the *pattern* evidence. When several
    kinds of evidence agree on one label their confidences combine (``1 - (1 - a)(1 - b)``) and ``evidence`` is the strongest kind (pattern, then name, then type).
    """
    fam = type_family(data_type)
    hits: dict[str, dict[str, Any]] = {}

    def add(label: str, conf: float, kind: str, rule: str) -> None:
        cur = hits.setdefault(label, {"by_kind": {}, "rules": []})
        cur["by_kind"][kind] = max(cur["by_kind"].get(kind, 0.0), conf)   # the same kind of evidence never counts twice
        cur["rules"].append(rule)
        total = 0.0
        for v in cur["by_kind"].values():
            total = v if total == 0.0 else _combine(total, v)
        cur["confidence"] = round(total, 3)
        cur["kinds"] = list(cur["by_kind"])

    tokens = tokenize(name)
    joined_phrases = set()
    if tokens:
        for i in range(len(tokens)):
            for j in range(i + 1, min(len(tokens), i + 5) + 1):
                joined_phrases.add("_".join(tokens[i:j]))
    for rule in NAME_RULES:
        if not any(p in joined_phrases for p in rule.phrases):
            continue
        if rule.negative & set(tokens):
            continue
        conf = rule.confidence
        if rule.types is not None and fam not in rule.types:
            conf *= 0.2
        add(rule.classification, conf, "name", rule.rule)
    if fam == "binary" and not hits:
        pass  # binary columns are opaque; nothing to say from the type alone
    for vr in VALUE_RULES:
        strs = _stringify(values or ())
        if len(strs) < vr.min_values:
            continue
        passed = sum(1 for s in strs if vr.check(s))
        ratio = passed / len(strs)
        if ratio >= vr.min_ratio:
            add(vr.classification, min(0.99, vr.base * (0.85 + 0.15 * ratio)), "pattern", vr.rule)
    if fam in ("date", "timestamp") and "date_of_birth" in hits and "name" in hits["date_of_birth"]["kinds"]:
        add("date_of_birth", 0.2, "type", "type:date-for-birth-name")
    if fam in ("floating",) and "geolocation" in hits and "name" in hits["geolocation"]["kinds"]:
        add("geolocation", 0.2, "type", "type:float-for-coordinate-name")
    out: list[dict[str, Any]] = []
    for label, h in hits.items():
        if h["confidence"] < min_confidence:
            continue
        kinds = sorted(h["kinds"], key=EVIDENCE_ORDER.index)
        out.append(Suggestion(label, min(0.99, h["confidence"]), kinds[0], "+".join(dict.fromkeys(h["rules"])), category=CATEGORY.get(label, "pii"), evidences=tuple(kinds)).to_dict())
    out.sort(key=lambda s: (-s["confidence"], s["classification"]))
    return out


def classify_columns(columns: Iterable[dict[str, Any]], *, min_confidence: float = DEFAULT_THRESHOLD) -> dict[str, list[dict[str, Any]]]:
    """``{column name: suggestions}`` for the columns that have at least one suggestion at ``min_confidence`` (``columns`` are ``{name, type}`` dicts; ``values`` per column optional)."""
    result: dict[str, list[dict[str, Any]]] = {}
    for c in columns:
        name = str(c.get("name") or "")
        if not name:
            continue
        s = classify_column(name, c.get("type") or c.get("data_type"), c.get("values"), min_confidence=min_confidence)
        if s:
            result[name] = s
    return result


def suggest_classifications(subject: Any, *, min_confidence: float = DEFAULT_THRESHOLD) -> list[Suggestion]:
    """Suggestions for a table schema or for discovered facts.

    ``subject`` may be a table schema (``{"name": ..., "columns": [{"name", "type"}]}`` or just the list of column dicts), an :class:`~olcf.intelligence.model.AssetFact`
    (a table / view / model table with ``attrs["columns"]``, or a column) or :class:`~olcf.intelligence.model.Facts` (every asset in it). Returns one
    :class:`Suggestion` per (column, label) at or above ``min_confidence``.
    """
    out: list[Suggestion] = []
    if isinstance(subject, Facts):
        for a in subject.assets:
            out.extend(suggest_classifications(a, min_confidence=min_confidence))
        return out
    if isinstance(subject, AssetFact):
        if subject.ref.type == "column":
            t = subject.attrs.get("data_type") or subject.attrs.get("type")
            return [_with(s, subject.name, None) for s in classify_column(subject.name, t, None, min_confidence=min_confidence)]
        cols = subject.attrs.get("columns")
        return suggest_classifications({"name": subject.name, "columns": cols or []}, min_confidence=min_confidence) if cols else []
    if isinstance(subject, dict):
        table = subject.get("name")
        for col, suggestions in classify_columns(subject.get("columns") or [], min_confidence=min_confidence).items():
            out.extend(_with(s, col, table) for s in suggestions)
        return out
    if isinstance(subject, (list, tuple)):
        return suggest_classifications({"columns": list(subject)}, min_confidence=min_confidence)
    raise TypeError(f"cannot suggest classifications for {type(subject).__name__}")


def _with(s: dict[str, Any], column: str | None, table: str | None) -> Suggestion:
    return Suggestion(classification=s["classification"], confidence=s["confidence"], evidence=s["evidence"], rule=s["rule"], column=column, table=table, category=s["category"],
                      evidences=tuple(s["evidences"]))


def annotate_facts(facts: Facts, *, min_confidence: float = DEFAULT_THRESHOLD) -> int:
    """Attach ``attrs["suggested_classifications"]`` to the column assets in ``facts`` (and to the ``attrs["columns"]`` entries and a ``sensitive_columns`` summary of their
    table). Mutates ``facts`` in place; returns the number of columns that received a suggestion. Existing suggestions from value profiling (pattern evidence) are kept."""
    count = 0
    by_table: dict[str, list[str]] = {}
    for a in facts.assets:
        if a.ref.type == "column":
            sug = classify_column(a.name, a.attrs.get("data_type") or a.attrs.get("type"), None, min_confidence=min_confidence)
            sug = _merge_existing(a.attrs.get("suggested_classifications"), sug)
            if sug:
                a.attrs["suggested_classifications"] = sug
                count += 1
    for a in facts.assets:
        cols = a.attrs.get("columns")
        if a.ref.type in ("table", "view", "model_table") and isinstance(cols, list):
            sens: list[str] = []
            for c in cols:
                if not isinstance(c, dict) or not c.get("name"):
                    continue
                sug = classify_column(str(c["name"]), c.get("type"), None, min_confidence=min_confidence)
                sug = _merge_existing(c.get("suggested_classifications"), sug)
                if sug:
                    c["suggested_classifications"] = sug
                    sens.append(str(c["name"]))
            if sens:
                a.attrs["sensitive_columns"] = sens
                by_table[a.name] = sens
    if by_table and count == 0:
        count = sum(len(v) for v in by_table.values())
    return count


def _merge_existing(existing: Any, fresh: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not existing:
        return fresh
    merged = {s["classification"]: s for s in fresh}
    for s in existing:
        if isinstance(s, dict) and s.get("classification") and s["classification"] not in merged:
            merged[s["classification"]] = s
    return sorted(merged.values(), key=lambda s: (-float(s.get("confidence", 0)), s["classification"]))


def sensitive_column_names(columns: Iterable[dict[str, Any]], *, min_confidence: float = DEFAULT_THRESHOLD) -> set[str]:
    """Names (lower-case) of the columns that look sensitive by name and type - the set a sampling policy with ``exclude_classified`` leaves out."""
    return {name.lower() for name in classify_columns(columns, min_confidence=min_confidence)}
