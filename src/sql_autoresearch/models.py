from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class IterationStatus(Enum):
    KEPT = "KEPT"
    DISCARDED_SLOWER = "DISCARDED_SLOWER"
    FAILED_MISMATCH = "FAILED_MISMATCH"
    FAILED_TIE_REORDER = "FAILED_TIE_REORDER"
    FAILED_SAFETY = "FAILED_SAFETY"
    FAILED_SCHEMA = "FAILED_SCHEMA"
    CANDIDATE_ERROR = "CANDIDATE_ERROR"
    CANDIDATE_TOO_LARGE = "CANDIDATE_TOO_LARGE"


class QueryOutcome(Enum):
    ERROR = "ERROR"
    UNSUPPORTED_SAFETY = "UNSUPPORTED_SAFETY"
    UNSUPPORTED_TYPES = "UNSUPPORTED_TYPES"
    UNSUPPORTED_TOO_LARGE = "UNSUPPORTED_TOO_LARGE"
    UNSUPPORTED_PROMPT = "UNSUPPORTED_PROMPT"
    NO_VALID_CANDIDATE = "NO_VALID_CANDIDATE"
    OPTIMIZED = "OPTIMIZED"
    UNCHANGED = "UNCHANGED"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    VERIFICATION_TIE = "VERIFICATION_TIE"
    NO_VERIFIED_CANDIDATE = "NO_VERIFIED_CANDIDATE"


class CompareResult(Enum):
    EQUAL = "EQUAL"
    MISMATCH = "MISMATCH"
    TIE_REORDER = "TIE_REORDER"


@dataclass
class RelationRef:
    schema: str | None
    name: str
    inh: bool  # True = include descendants, False = ONLY


@dataclass
class ResolvedRelation:
    schema: str
    name: str
    oid: int


@dataclass
class AstCheckResult:
    clean_sql: str
    function_names: set[str] = field(default_factory=set)
    operator_names: set[str] = field(default_factory=set)
    relations: list[RelationRef] = field(default_factory=list)
    has_order_by: bool = False
    cast_type_names: list[tuple[str, ...]] = field(default_factory=list)


@dataclass
class ColumnInfo:
    name: str
    type_name: str
    type_oid: int
    not_null: bool


@dataclass
class IndexInfo:
    name: str
    definition: str
    is_unique: bool
    is_primary: bool


@dataclass
class TableStats:
    n_distinct: dict[str, float] = field(default_factory=dict)
    null_frac: dict[str, float] = field(default_factory=dict)
    correlation: dict[str, float] = field(default_factory=dict)


@dataclass
class TableInfo:
    schema: str
    name: str
    oid: int
    columns: list[ColumnInfo] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    stats: TableStats = field(default_factory=TableStats)
    row_estimate: float = 0.0


@dataclass
class ColumnDesc:
    name: str
    type_oid: int


@dataclass
class IterationResult:
    iteration: int
    status: IterationStatus
    candidate_sql: str | None = None
    explanation: str | None = None
    candidate_timing_ms: float | None = None
    current_best_timing_ms: float | None = None


@dataclass
class QueryResult:
    original_sql: str
    final_sql: str
    outcome: QueryOutcome
    iterations: list[IterationResult] = field(default_factory=list)
    improvement_ratio: float | None = None
    original_timing_ms: float | None = None
    error: str | None = None
    final_timing_ms: float | None = None


class UnsafeQueryError(Exception):
    pass


class UnsupportedQueryError(Exception):
    pass


class PromptBudgetError(Exception):
    pass


# Supported type OIDs (PostgreSQL)
SUPPORTED_TYPE_OIDS: dict[int, str] = {
    # Scalar types
    16: "bool",
    17: "bytea",
    20: "int8",
    21: "int2",
    23: "int4",
    25: "text",
    114: "json",
    700: "float4",
    701: "float8",
    1042: "bpchar",
    1043: "varchar",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    1184: "timestamptz",
    1186: "interval",
    1266: "timetz",
    1700: "numeric",
    2950: "uuid",
    3802: "jsonb",
    # Array types
    1000: "_bool",
    1001: "_bytea",
    1005: "_int2",
    1007: "_int4",
    1009: "_text",
    1015: "_varchar",
    1016: "_int8",
    1115: "_timestamp",
    1182: "_date",
    1185: "_timestamptz",
    2951: "_uuid",
}

# Built-in type OIDs allowed for explicit casts
BUILTIN_CAST_TYPE_NAMES: set[str] = {
    "int2", "smallint",
    "int4", "integer", "int",
    "int8", "bigint",
    "float4", "real",
    "float8", "double precision",
    "numeric", "decimal",
    "bool", "boolean",
    "text",
    "varchar", "character varying",
    "char", "character", "bpchar",
    "bytea",
    "timestamp", "timestamp without time zone",
    "timestamptz", "timestamp with time zone",
    "date",
    "time", "time without time zone",
    "timetz", "time with time zone",
    "interval",
    "uuid",
    "json",
    "jsonb",
}

# Aggregate allowlist
ALLOWED_AGGREGATES: set[str] = {
    "count", "sum", "avg", "min", "max",
    "bool_and", "bool_or", "every",
    "bit_and", "bit_or",
    "variance", "var_pop", "var_samp",
    "stddev", "stddev_pop", "stddev_samp",
    "corr", "covar_pop", "covar_samp",
    "regr_avgx", "regr_avgy", "regr_count", "regr_intercept",
    "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy",
}

# Order-dependent aggregates (always rejected)
ORDER_DEPENDENT_AGGREGATES: set[str] = {
    "array_agg", "string_agg", "json_agg", "jsonb_agg",
    "json_object_agg", "jsonb_object_agg",
    "xmlagg",
}

# Pure window functions
ALLOWED_WINDOW_FUNCTIONS: set[str] = {
    "rank", "dense_rank", "percent_rank", "cume_dist",
}

REJECTED_WINDOW_FUNCTIONS: set[str] = {
    "row_number", "ntile", "lag", "lead",
    "first_value", "last_value", "nth_value",
}

MAX_ROWS = 10_000
MAX_BYTES = 10 * 1024 * 1024  # 10MB
