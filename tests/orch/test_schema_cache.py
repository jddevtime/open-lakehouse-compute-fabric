# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The validator caches of ``pipelines/model.py`` must never hand one activity type the schema of another.

They were keyed by ``id(object)``.  An id is only unique among *live* objects: after an activity instance or registry is garbage collected, a new object of a different
type can get the same id and would then be validated with the old object's schema.  In the full ``tests/orch`` run that showed up as an ``http`` activity rejected with
"unknown property 'method' (allowed: fail, failTimes, gate, ...)" - the property list of a test probe that had lived at the same address.
"""

from __future__ import annotations

import gc
from typing import Any

from olcf.pipelines import model
from olcf.pipelines.activities.base import Activity, ActivityRegistry, obj


def make_type(type_name: str, prop: str) -> type[Activity]:
    class T(Activity):
        type = type_name
        mode = "blocking"
        properties_schema = obj({prop: {"type": "string"}})

        def execute(self, actx: Any) -> Any:  # pragma: no cover - never run
            return None

    T.__name__ = f"T_{type_name}"
    return T


def test_props_validator_follows_the_object_not_its_address() -> None:
    a_cls, b_cls = make_type("cache_a", "alpha"), make_type("cache_b", "beta")
    for i in range(400):  # same-sized objects freed and re-allocated: CPython hands out the same address again and again
        cls, want = (a_cls, "alpha") if i % 2 == 0 else (b_cls, "beta")
        inst = cls()
        v = model._props_validator(inst)
        assert set(v.schema["properties"]) == {want}, f"iteration {i}: validator of another type returned for {cls.__name__}"
        del inst, v
        if i % 50 == 0:
            gc.collect()


def test_caches_do_not_keep_dead_registries_or_activities_alive() -> None:
    before = len(model._SCHEMA_CACHE), len(model._PROPS_CACHE)
    for i in range(30):
        reg = ActivityRegistry()
        reg.register(make_type(f"reg_type_{i}", f"prop{i}"))
        model._validators(reg)
        for t in reg.all():
            model._props_validator(t)
        del reg, t
    gc.collect()
    assert (len(model._SCHEMA_CACHE), len(model._PROPS_CACHE)) == before, "validators of collected registries must not accumulate"
