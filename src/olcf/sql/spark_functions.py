# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The Spark SQL functions a statement sent to the shared Spark kernel may call (review C01).

Policy
------
This is an **allowlist**.  A function that is not in :data:`SPARK_ALLOWED_FUNCTIONS` is refused by :mod:`olcf.sql.guard` whoever sends the statement: an unknown registered UDF, a
function added by a future Spark release, a function of another distribution, a function resolved through a catalog (``mydb.my_udf(x)`` - qualified calls are refused outright)
and anything that merely *looks* like a built-in.  The previous denylist only knew the dangerous names somebody had thought of.

What belongs here: the ordinary **pure** built-ins of Spark 3.5 / 4.x - scalar, aggregate, window, generator, string, math, date / time, collection, map, JSON / CSV / XML, hash,
conditional, bitwise, type-conversion and variant functions - that compute a value from their arguments and the rows they are applied to.

What never belongs here (they stay in ``guard.SPARK_DENIED_FUNCTIONS`` / ``SPARK_DENIED_FUNCTION_PREFIXES`` and keep their explicit refusal message): anything that reaches the host
or beyond the lakehouse tables the caller was authorised for - JVM reflection (``reflect``, ``java_method``, ``try_reflect``), function indirection (``call_function``,
``call_udf``, ``identifier``), descriptor files read from the host (the protobuf functions), network / secret / service functions (``http_*``, ``secret``, ``remote_*``,
``ai_*``), file readers (``read_*``) and user-defined function wrappers (``python_udf``, ``scala_udf``, ``java_udf``).  ``input_file_name`` stays allowed as it always was.

Adding a function: **review and test each function before adding it.**  Read the Spark implementation (does it touch the file system, the class path, the network, the
environment, a secret or the session configuration?), add it to the matching group below in lowercase, and add a positive case to ``tests/data/test_sql_host_guard.py``.  Do not
add a name only to make one query pass, and never add a name that also appears in the denylist (a test asserts the two sets are disjoint).  Custom or UDF functions are not
added here at all: run a notebook or a job when a statement needs one.

The names are Spark's (lower case).  The guard compares the spelling the author typed, normalised by sqlglot, and for the typed nodes sqlglot builds (``Sum``, ``Cast``,
``TimestampTrunc`` ...) also the Spark spelling those nodes render to.
"""

from __future__ import annotations

_AGGREGATE = (
    "any", "any_value", "approx_count_distinct", "approx_percentile", "approx_top_k", "array_agg", "avg", "bit_and", "bit_or", "bit_xor", "bitmap_construct_agg", "bitmap_or_agg",
    "bool_and", "bool_or", "collect_list", "collect_set", "corr", "count", "count_if", "count_min_sketch", "covar_pop", "covar_samp", "every", "first", "first_value",
    "grouping", "grouping_id", "histogram_numeric", "hll_sketch_agg", "hll_union_agg", "kurtosis", "last", "last_value", "listagg", "max", "max_by", "mean", "median", "min",
    "min_by", "mode", "percentile", "percentile_approx", "percentile_cont", "percentile_disc", "product", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2",
    "regr_slope", "regr_sxx", "regr_sxy", "regr_syy", "schema_of_variant_agg", "skewness", "some", "std", "stddev", "stddev_pop", "stddev_samp", "string_agg", "sum", "try_avg",
    "try_sum", "var_pop", "var_samp", "variance",
)
_WINDOW = ("cume_dist", "dense_rank", "lag", "lead", "nth_value", "ntile", "percent_rank", "rank", "row_number", "session_window", "window", "window_time")
_GENERATOR = (
    "explode", "explode_outer", "inline", "inline_outer", "json_tuple", "parse_url", "posexplode", "posexplode_outer", "range", "sequence", "stack", "variant_explode",
    "variant_explode_outer",
)
_STRING = (
    "ascii", "base64", "bit_length", "btrim", "char", "char_length", "character_length", "chr", "collation", "concat", "concat_ws", "contains", "decode", "elt", "encode",
    "endswith", "find_in_set", "format_number", "format_string", "initcap", "instr", "is_valid_utf8", "lcase", "left", "len", "length", "levenshtein", "like", "ilike", "locate",
    "lower", "lpad", "ltrim", "luhn_check", "make_valid_utf8", "mask", "ngrams", "context_ngrams", "octet_length", "overlay", "position", "printf", "randstr", "regexp", "regexp_count",
    "regexp_extract", "regexp_extract_all", "regexp_instr", "regexp_like", "regexp_replace", "regexp_substr", "repeat", "replace", "right", "rlike", "rpad", "rtrim", "sentences",
    "soundex", "space", "split", "split_part", "startswith", "str_to_map", "substr", "substring", "substring_index", "to_binary", "to_char", "to_number", "to_varchar", "translate",
    "trim", "try_to_binary", "try_to_number", "try_validate_utf8", "ucase", "unbase64", "upper", "url_decode", "url_encode", "validate_utf8",
)
_MATH = (
    "abs", "acos", "acosh", "asin", "asinh", "atan", "atan2", "atanh", "bin", "bround", "cbrt", "ceil", "ceiling", "conv", "cos", "cosh", "cot", "csc", "degrees", "div", "e",
    "exp", "expm1", "factorial", "floor", "greatest", "hex", "hypot", "least", "ln", "log", "log10", "log1p", "log2", "mod", "negative", "pi", "pmod", "positive", "pow", "power",
    "radians", "rand", "randn", "random", "rint", "round", "sec", "sign", "signum", "sin", "sinh", "sqrt", "tan", "tanh", "try_add", "try_divide", "try_mod", "try_multiply",
    "try_subtract", "unhex", "uniform", "width_bucket",
)
_DATETIME = (
    "add_months", "convert_timezone", "curdate", "current_date", "current_time", "current_timestamp", "current_timezone", "date", "date_add", "date_diff", "date_format",
    "date_from_unix_date", "date_part", "date_sub", "date_trunc", "dateadd", "datediff", "datepart", "day", "dayofmonth", "dayofweek", "dayofyear", "extract", "from_unixtime",
    "from_utc_timestamp", "hour", "last_day", "localtimestamp", "make_date", "make_dt_interval", "make_interval", "make_time", "make_timestamp", "make_timestamp_ltz",
    "make_timestamp_ntz", "make_ym_interval", "minute", "month", "monthname", "months_between", "next_day", "now", "quarter", "second", "time_diff", "time_trunc", "timestamp",
    "timestamp_micros", "timestamp_millis", "timestamp_seconds", "timestampadd", "timestampdiff", "to_date", "to_time", "to_timestamp", "to_timestamp_ltz", "to_timestamp_ntz",
    "to_unix_timestamp", "to_utc_timestamp", "trunc", "try_make_interval", "try_make_timestamp", "try_make_timestamp_ltz", "try_make_timestamp_ntz", "try_to_date", "try_to_time",
    "try_to_timestamp", "unix_date", "unix_micros", "unix_millis", "unix_seconds", "unix_timestamp", "weekday", "weekofyear", "year", "dayname",
)
_COLLECTION = (
    "array", "array_append", "array_compact", "array_contains", "array_distinct", "array_except", "array_insert", "array_intersect", "array_join", "array_max", "array_min",
    "array_position", "array_prepend", "array_remove", "array_repeat", "array_size", "array_sort", "array_union", "arrays_overlap", "arrays_zip", "cardinality", "element_at",
    "exists", "filter", "flatten", "forall", "get", "reduce", "reverse", "shuffle", "size", "slice", "sort_array", "transform", "aggregate", "zip_with",
)
_MAP = (
    "map", "map_concat", "map_contains_key", "map_entries", "map_filter", "map_from_arrays", "map_from_entries", "map_keys", "map_values", "map_zip_with", "named_struct", "struct",
    "transform_keys", "transform_values",
)
_SEMI_STRUCTURED = (
    "from_csv", "from_json", "from_xml", "get_json_object", "is_variant_null", "json_array_length", "json_object_keys", "parse_json", "schema_of_csv", "schema_of_json",
    "schema_of_variant", "schema_of_xml", "to_csv", "to_json", "to_variant_object", "to_xml", "try_parse_json", "try_variant_get", "variant_get",
)
_HASH_CRYPTO = (
    "aes_decrypt", "aes_encrypt", "crc32", "hash", "hll_sketch_estimate", "hll_union", "md5", "sha", "sha1", "sha2", "try_aes_decrypt", "xxhash64",
)
_CONDITIONAL = (
    "assert_true", "case", "coalesce", "decode", "equal_null", "if", "ifnull", "isnan", "isnotnull", "isnull", "nanvl", "nullif", "nullifzero", "nvl", "nvl2", "raise_error",
    "try_element_at", "zeroifnull",
)
_BITWISE = ("bit_count", "bit_get", "getbit", "shiftleft", "shiftright", "shiftrightunsigned", "bitmap_bit_position", "bitmap_bucket_number", "bitmap_count")
_CONVERSION = (
    "bigint", "binary", "boolean", "cast", "decimal", "double", "float", "int", "smallint", "string", "tinyint", "try_cast", "typeof",
)
_ENVIRONMENT_FREE_INFO = (
    # values the session already knows; none reads a file, the class path, the network or a secret
    "current_catalog", "current_database", "current_schema", "current_user", "input_file_name", "monotonically_increasing_id", "session_user", "spark_partition_id", "user", "uuid",
    "version",
)

SPARK_ALLOWED_FUNCTIONS: frozenset[str] = frozenset(
    (*_AGGREGATE, *_WINDOW, *_GENERATOR, *_STRING, *_MATH, *_DATETIME, *_COLLECTION, *_MAP, *_SEMI_STRUCTURED, *_HASH_CRYPTO, *_CONDITIONAL, *_BITWISE, *_CONVERSION,
     *_ENVIRONMENT_FREE_INFO)
)
