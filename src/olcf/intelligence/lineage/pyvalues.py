# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Abstract values of the PySpark interpreter (:mod:`olcf.intelligence.lineage.pyspark`): partially known strings, column expressions and the function tables.

The interpreter never executes user code. It evaluates the Python AST over an *abstract* value domain:

* ordinary Python values that are fully known (``str``, ``int``, ``list`` ...);
* :data:`UNK`, the value of anything that cannot be determined statically;
* :class:`PStr`, a string with unknown parts (``f"{schema}.orders"`` where ``schema`` is unknown), remembering which parts came from notebook *parameters* (defaults
  that a pipeline may override), so that edges derived from them are only ``inferred``;
* :class:`ColV`, a Spark / pandas column expression: the base-table columns it already depends on (``bound``), the names still to be resolved against the DataFrame it is
  used on (``free``) and its text;
* handle objects (:class:`Reader`, :class:`Writer`, :class:`Grouped`, ...) for the builder-style APIs.

The function tables at the bottom describe which positional arguments of the ``pyspark.sql.functions`` API are column names (``c``), literals or columns (``l``) and so on,
because ``F.sum("amount")`` references a column while ``F.lit("amount")`` does not.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from olcf.intelligence.lineage.relation import Dataset, Origin, Relation


# ================================================================================================================================== unknown / strings
class _Unknown:
    """The value of anything the interpreter cannot determine. Falsy-looking but never compared: callers test ``is UNK``."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNK"

    def __bool__(self) -> bool:
        return False


UNK = _Unknown()
HOLE = "<?>"          # rendering of an unknown fragment of a partially known string


@dataclass(frozen=True)
class PStr:
    """A string made of known fragments and unknown holes (``None``); ``taint`` lists the literal texts that came from notebook parameters; ``path`` marks ``pathlib`` paths."""

    frags: tuple[str | None, ...]
    taint: tuple[str, ...] = ()
    path: bool = False

    @property
    def known(self) -> bool:
        """True when no fragment is unknown."""
        return all(f is not None for f in self.frags)

    def render(self) -> str:
        """The text with unknown fragments shown as ``<?>``."""
        return "".join(HOLE if f is None else f for f in self.frags)

    def text(self) -> str | None:
        """The full text when known, else ``None``."""
        return "".join(f for f in self.frags if f is not None) if self.known else None


def mk_str(frags: list[str | None] | tuple[str | None, ...], taint: tuple[str, ...] = (), path: bool = False) -> Any:
    """Normalise string fragments: a fully known string without taint is a plain ``str``, a string with nothing known is :data:`UNK`."""
    merged: list[str | None] = []
    for f in frags:
        if f == "" :
            continue
        if isinstance(f, str) and merged and isinstance(merged[-1], str):
            merged[-1] += f
        elif f is None and merged and merged[-1] is None:
            continue
        else:
            merged.append(f)
    if not merged:
        return PStr(("",), taint, path) if (taint or path) else ""
    if all(f is None for f in merged) and not path:
        return UNK
    if all(isinstance(f, str) for f in merged) and not taint and not path:
        return "".join(f for f in merged if isinstance(f, str))
    return PStr(tuple(merged), tuple(dict.fromkeys(taint)), path)


def str_parts(v: Any) -> tuple[list[str | None], tuple[str, ...], bool] | None:
    """Fragments, taint and path flag of a string-like value; ``None`` when ``v`` is not a string."""
    if isinstance(v, str):
        return [v], (), False
    if isinstance(v, PStr):
        return list(v.frags), v.taint, v.path
    return None


def concat_str(a: Any, b: Any, *, sep: str = "", path: bool = False) -> Any:
    """Concatenate two string-like values (either may be partially known); non-strings count as an unknown fragment."""
    pa, pb = str_parts(a), str_parts(b)
    fa: list[str | None] = pa[0] if pa else [None]
    fb: list[str | None] = pb[0] if pb else [None]
    taint = (pa[1] if pa else ()) + (pb[1] if pb else ())
    is_path = path or (pa[2] if pa else False) or (pb[2] if pb else False)
    return mk_str([*fa, sep, *fb] if sep else [*fa, *fb], taint, is_path)


# ================================================================================================================================== column expressions
@dataclass(frozen=True)
class ColV:
    """A column expression: ``bound`` origins are final, ``free`` names are resolved against the DataFrame the expression is used on.

    ``ref`` is True for a plain column reference (no transformation step is recorded for it), ``name`` the output name when known, ``star`` for ``F.col("*")``.
    ``literal`` is True for a constant (``F.lit(1)``): such columns have no origins by design, which is a complete statement, not a gap.
    """

    bound: frozenset[Origin] = frozenset()
    free: tuple[str, ...] = ()
    text: str = ""
    name: str | None = None
    ref: bool = False
    star: bool = False
    literal: bool = False

    def renamed(self, name: str) -> ColV:
        """The same expression under a new output name (``.alias(name)``)."""
        return ColV(self.bound, self.free, self.text, name, self.ref, self.star, self.literal)


def combine(text: str, parts: list[Any], name: str | None = None) -> ColV:
    """A column computed from ``parts`` (the :class:`ColV` ones contribute their origins and free names)."""
    bound: set[Origin] = set()
    free: list[str] = []
    for p in parts:
        if isinstance(p, ColV):
            bound |= p.bound
            free.extend(f for f in p.free if f not in free)
    lit = all(isinstance(p, ColV) and p.literal or not isinstance(p, ColV) for p in parts) and not bound and not free
    return ColV(frozenset(bound), tuple(free), text, name, False, False, lit)


def render_value(v: Any) -> str:
    """Short text for a value inside an expression text (strings quoted, columns by their text)."""
    if isinstance(v, ColV):
        return v.text
    if isinstance(v, PStr):
        return repr(v.render())
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(render_value(x) for x in v) + "]"
    if v is UNK:
        return "?"
    return repr(v)


# ================================================================================================================================== handles
@dataclass(frozen=True)
class DF:
    """A DataFrame (Spark, pandas, DuckDB relation, Arrow ...): its abstract :class:`Relation` and the library it belongs to."""

    rel: Relation
    kind: str = "spark"


@dataclass(frozen=True)
class Reader:
    """``spark.read`` / ``spark.readStream`` after some ``.format()`` / ``.option()`` / ``.schema()`` calls."""

    fmt: str | None = None
    options: tuple[tuple[str, Any], ...] = ()
    schema: tuple[str, ...] | None = None
    stream: bool = False


@dataclass(frozen=True)
class Writer:
    """``df.write`` / ``df.writeStream`` after ``.format()`` / ``.mode()`` / ``.option()`` / ``.partitionBy()`` calls."""

    df: DF
    fmt: str | None = None
    mode: str | None = None
    options: tuple[tuple[str, Any], ...] = ()
    stream: bool = False
    v2_table: Any = None


@dataclass(frozen=True)
class Grouped:
    """Result of ``df.groupBy(...)``: the input DataFrame and the grouping columns (as resolved infos), ``pivot`` when ``.pivot()`` was called."""

    df: DF
    keys: tuple[Any, ...] = ()
    pivot: bool = False


@dataclass(frozen=True)
class DeltaTbl:
    """A Delta table handle (``DeltaTable.forName/forPath``, ``deltalake.DeltaTable(path)``) with an optional alias.

    ``conf`` is the confidence of the reference: ``inferred`` when the table was identified from a path whose root is computed at run time or from a notebook parameter.
    """

    ds: Dataset
    alias: str | None = None
    py: bool = False
    conf: str = "derived"


@dataclass
class MergeB:
    """A Delta ``MERGE`` builder: the clauses collected until ``.execute()``."""

    target: Dataset
    source: DF
    target_alias: str | None
    source_alias: str | None
    condition: Any
    clauses: list[tuple[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class WinV:
    """A window specification: its partition columns (``part``), order columns (``order``) and frame text."""

    part: tuple[Any, ...] = ()
    order: tuple[Any, ...] = ()
    frame: str = ""

    @property
    def cols(self) -> tuple[Any, ...]:
        """All columns the window depends on."""
        return (*self.part, *self.order)

    def text(self) -> str:
        """SQL text of the window (``PARTITION BY a ORDER BY b ROWS ...``)."""
        bits = []
        if self.part:
            bits.append("PARTITION BY " + ", ".join(render_value(c) for c in self.part))
        if self.order:
            bits.append("ORDER BY " + ", ".join(render_value(c) for c in self.order))
        if self.frame:
            bits.append(self.frame)
        return " ".join(bits)


@dataclass(frozen=True)
class SchemaV:
    """A DDL string / ``StructType`` reduced to its column names."""

    names: tuple[str, ...]


@dataclass(frozen=True)
class ModuleRef:
    """An imported module or attribute path (``pyspark.sql.functions``); calls on it are dispatched by their dotted name."""

    path: str


@dataclass(frozen=True)
class ConnV:
    """A DuckDB connection (``duckdb.connect()``), identified by ``cid`` so that registered views stay with their connection."""

    cid: int


@dataclass(frozen=True)
class RddV:
    """An RDD derived from a DataFrame: only the datasets feeding it are known."""

    datasets: frozenset[str]
    names: tuple[str, ...] | None = None


@dataclass(frozen=True)
class NaV:
    """``df.na`` / ``df.stat``: methods return the DataFrame unchanged."""

    df: DF


@dataclass(frozen=True)
class SparkV:
    """The Spark session (``spark``, ``sqlContext``)."""


@dataclass(frozen=True)
class UdfV:
    """A user-defined function usable on columns; calling it with columns yields a column that depends on all of them."""

    name: str


@dataclass(frozen=True)
class FuncV:
    """A user function or lambda defined in the analysed code (``node`` is the AST, ``env`` the defining scope); ``defaults`` are evaluated at definition time."""

    name: str
    node: Any = field(compare=False)
    env: Any = field(compare=False)
    defaults: tuple[Any, ...] = field(default=(), compare=False)
    kw_defaults: tuple[tuple[str, Any], ...] = field(default=(), compare=False)
    origin: Any = field(default=None, compare=False)
    uid: int = 0


SPARK = SparkV()


# ================================================================================================================================== function tables
#: Spark SQL functions (``pyspark.sql.functions``). Unknown names are *not* treated as column functions.
KNOWN_FUNCS = frozenset("""
abs acos acosh add_months aggregate any_value approx_count_distinct approx_percentile array array_agg array_append array_compact array_contains array_distinct array_except array_insert
array_intersect array_join array_max array_min array_position array_prepend array_remove array_repeat array_size array_sort array_union arrays_overlap arrays_zip asc asc_nulls_first
asc_nulls_last ascii asin asinh atan atan2 atanh avg base64 bin bit_and bit_length bit_or bit_xor bitmap_bit_position bitwise_not bitwiseNOT bool_and bool_or bround btrim
cardinality cbrt ceil ceiling char char_length character_length coalesce col collect_list collect_set column concat concat_ws contains conv corr cos cosh cot count count_distinct countDistinct
count_if covar_pop covar_samp crc32 create_map csc cume_dist curdate current_date current_timestamp current_timezone date_add date_diff date_format date_from_unix_date date_sub date_trunc
dateadd datediff datepart day dayofmonth dayofweek dayofyear days decode degrees dense_rank desc desc_nulls_first desc_nulls_last element_at elt encode endswith equal_null every exists exp
explode explode_outer expm1 expr extract factorial filter find_in_set first first_value flatten floor forall format_number format_string from_csv from_json from_unixtime from_utc_timestamp
get get_json_object getbit greatest grouping grouping_id hash hex hour hours hypot ifnull ilike initcap inline inline_outer input_file_name instr isnan isnotnull isnull
json_array_length json_object_keys json_tuple kurtosis lag last last_day last_value lcase lead least left length levenshtein like lit ln localtimestamp locate log log10 log1p log2 lower lpad
ltrim make_date make_dt_interval make_interval make_timestamp make_timestamp_ltz make_timestamp_ntz map_concat map_contains_key map_entries map_filter map_from_arrays map_from_entries
map_keys map_values map_zip_with mask max max_by md5 mean median min min_by minute mode monotonically_increasing_id month months months_between named_struct nanvl negative next_day now
nth_value ntile nullif nvl nvl2 octet_length overlay parse_json percent_rank percentile percentile_approx pi pmod posexplode posexplode_outer position positive pow power printf product
quarter radians raise_error rand randn rank regexp regexp_count regexp_extract regexp_extract_all regexp_instr regexp_like regexp_replace regexp_substr regr_avgx regr_avgy regr_count
regr_intercept regr_r2 regr_slope regr_sxx regr_sxy regr_syy repeat replace reverse right rint rlike round row_number rpad rtrim schema_of_csv schema_of_json sec second sentences
sequence sha sha1 sha2 shiftleft shiftright shiftrightunsigned shuffle sign signum sin sinh size skewness slice some sort_array soundex spark_partition_id split split_part sqrt stack
startswith std stddev stddev_pop stddev_samp str_to_map struct substr substring substring_index sum sum_distinct sumDistinct tan tanh timestamp_micros timestamp_millis timestamp_seconds
to_binary to_char to_csv to_date to_json to_number to_timestamp to_timestamp_ltz to_timestamp_ntz to_unix_timestamp to_utc_timestamp to_varchar toDegrees toRadians transform transform_keys
transform_values translate trim trunc try_add try_avg try_divide try_element_at try_multiply try_subtract try_sum try_to_number try_to_timestamp typeof ucase unbase64 unhex unix_date
unix_micros unix_millis unix_seconds unix_timestamp upper url_decode url_encode uuid var_pop var_samp variance weekday weekofyear when width_bucket window xxhash64 year years zip_with
""".split())
#: Functions that take no column (their result depends on no input column).
NOARG_FUNCS = frozenset("""current_date current_timestamp curdate localtimestamp now monotonically_increasing_id rand randn row_number rank dense_rank percent_rank cume_dist uuid
spark_partition_id input_file_name current_timezone pi""".split())
#: Positional argument kinds per function when they differ from the default ``c`` followed by ``l``...:
#: ``c`` column name or column, ``l`` literal (a column object is still honoured), ``x`` expression string, ``*`` repeat the last kind.
FUNC_SPEC: dict[str, str] = {
    "coalesce": "c*", "greatest": "c*", "least": "c*", "concat": "c*", "array": "c*", "struct": "c*", "hash": "c*", "xxhash64": "c*", "create_map": "c*", "map_concat": "c*",
    "arrays_zip": "c*", "countDistinct": "c*", "count_distinct": "c*", "count": "c*", "bit_and": "c", "concat_ws": "lc*", "format_string": "lc*", "printf": "lc*", "elt": "lc*",
    "lit": "l", "col": "c", "column": "c", "expr": "x", "date_trunc": "lc", "locate": "lcl", "when": "cl", "datediff": "cc", "date_diff": "cc", "months_between": "ccl",
    "atan2": "cc", "hypot": "cc", "pow": "cc", "power": "cc", "nanvl": "cc", "pmod": "cc", "array_union": "cc", "array_intersect": "cc", "array_except": "cc", "arrays_overlap": "cc",
    "map_from_arrays": "cc", "corr": "cc", "covar_pop": "cc", "covar_samp": "cc", "max_by": "cc", "min_by": "cc", "nvl": "cc", "ifnull": "cc", "nullif": "cc", "nvl2": "ccc",
    "equal_null": "cc", "levenshtein": "cc", "map_zip_with": "cc", "zip_with": "ccf", "transform": "cf", "filter": "cf", "exists": "cf", "forall": "cf", "aggregate": "clff",
    "map_filter": "cf", "transform_keys": "cf", "transform_values": "cf", "isnan": "c", "isnull": "c", "next_day": "cl", "stack": "l", "named_struct": "l*", "coalesce_": "c*",
    "overlay": "cclc", "regr_slope": "cc", "regr_intercept": "cc", "regr_r2": "cc", "date_add": "cl", "date_sub": "cl", "dateadd": "lcc", "add_months": "cl", "make_date": "ccc",
    "array_repeat": "cl", "sequence": "ccl", "contains": "cc", "startswith": "cc", "endswith": "cc", "split_part": "cll", "left": "cc", "right": "cc", "sha2": "cl",
    "approx_percentile": "ccl", "percentile_approx": "cll", "percentile": "cl", "lag": "cll", "lead": "cll", "nth_value": "cl", "ntile": "l", "window": "cll", "like": "cl", "ilike": "cl",
    "rlike": "cl", "regexp_like": "cl", "regexp": "cl", "mask": "c", "to_number": "cl", "try_to_number": "cl", "greatest_": "c*", "when_": "cl",
}
#: Methods of a column expression that are plain functions of the receiver and their arguments.
COLUMN_METHODS = frozenset("""
alias name astype cast otherwise when isNull isNotNull isin between like rlike ilike startswith endswith contains substr getItem getField over asc desc asc_nulls_first asc_nulls_last
desc_nulls_first desc_nulls_last eqNullSafe bitwiseAND bitwiseOR bitwiseXOR withField dropFields isNaN
""".split())
#: Spark DataFrame methods that keep the columns (and the lineage) of the receiver.
DF_PASSTHROUGH = frozenset("""
cache persist unpersist checkpoint localCheckpoint repartition coalesce repartitionByRange hint limit sample sampleBy sortWithinPartitions orderBy sort filter where distinct
dropDuplicates drop_duplicates dropDuplicatesWithinWatermark withWatermark offset tail
""".split())
#: pandas DataFrame methods that keep the columns of the receiver.
PANDAS_PASSTHROUGH = frozenset("""
copy sort_values dropna fillna replace reset_index sort_index head tail drop_duplicates sample nlargest nsmallest filter query astype infer_objects convert_dtypes
""".split())
#: DataFrame methods that return plain Python values (nothing flows on).
DF_ACTIONS = frozenset("""show display printSchema explain count collect first take takeAsList foreach foreachPartition toLocalIterator isEmpty describe summary
""".split())
SPARK_FILE_READERS = frozenset("csv parquet json orc text avro xml".split())
SPARK_FILE_WRITERS = frozenset("csv parquet json orc text avro xml".split())
