"""Unit tests for Phase 1 core infrastructure modules."""

import time
from unittest.mock import MagicMock, patch


from quicksight_mcp.config import Settings
from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.core.types import (
    AGG_MAP,
    VISUAL_TYPES,
    extract_visual_id,
    is_date_column,
    parse_visual,
)
from quicksight_mcp.safety.exceptions import (
    ChangeVerificationError,
    ConcurrentModificationError,
    DestructiveChangeError,
    QSAuthError,
    QSError,
    QSNotFoundError,
    QSRateLimitError,
    QSValidationError,
)


# =========================================================================
# TTLCache tests
# =========================================================================


class TestTTLCache:
    """Tests for the TTL-based cache."""

    def test_set_and_get(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", {"data": [1, 2, 3]})
        assert cache.get("key1") == {"data": [1, 2, 3]}

    def test_get_missing_returns_none(self):
        cache = TTLCache()
        assert cache.get("nonexistent") is None

    def test_expired_entry_returns_none(self):
        cache = TTLCache(ttl=0)  # Immediate expiry
        cache.set("key1", "value")
        # TTL=0 means expired immediately on next get
        time.sleep(0.01)
        assert cache.get("key1") is None

    def test_invalidate_removes_entry(self):
        cache = TTLCache()
        cache.set("key1", "value")
        cache.invalidate("key1")
        assert cache.get("key1") is None

    def test_clear_removes_all(self):
        cache = TTLCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert cache.size == 0
        assert cache.get("a") is None

    def test_has_returns_true_for_valid_entry(self):
        cache = TTLCache(ttl=60)
        cache.set("key1", "value")
        assert cache.has("key1") is True

    def test_has_returns_false_for_missing(self):
        cache = TTLCache()
        assert cache.has("key1") is False

    def test_max_entries_evicts_oldest(self):
        cache = TTLCache(ttl=60, max_entries=3)
        cache.set("a", 1)
        time.sleep(0.01)
        cache.set("b", 2)
        time.sleep(0.01)
        cache.set("c", 3)
        # Adding 4th should evict "a"
        cache.set("d", 4)
        assert cache.get("a") is None
        assert cache.get("d") == 4
        assert cache.size == 3

    def test_size_property(self):
        cache = TTLCache()
        assert cache.size == 0
        cache.set("a", 1)
        assert cache.size == 1


# =========================================================================
# Types tests
# =========================================================================


class TestTypes:
    """Tests for shared type helpers."""

    def test_is_date_column_positive(self):
        assert is_date_column("CREATED_AT") is True
        assert is_date_column("purchase_date") is True
        assert is_date_column("EVENT_TIMESTAMP") is True
        assert is_date_column("SOME_DT") is True

    def test_is_date_column_negative(self):
        assert is_date_column("MARKET_NAME") is False
        assert is_date_column("FLIP_TOKEN") is False
        assert is_date_column("DATA") is False

    def test_parse_visual_kpi(self):
        visual = {
            "KPIVisual": {
                "VisualId": "kpi-001",
                "Title": {
                    "FormatText": {"PlainText": "Total Revenue"},
                },
            }
        }
        result = parse_visual(visual)
        assert result["type"] == "KPI"
        assert result["visual_id"] == "kpi-001"
        assert result["title"] == "Total Revenue"

    def test_parse_visual_unknown(self):
        result = parse_visual({"SomethingNew": {}})
        assert result["type"] == "Unknown"

    def test_extract_visual_id(self):
        assert extract_visual_id({"KPIVisual": {"VisualId": "v1"}}) == "v1"
        assert extract_visual_id({"UnknownType": {}}) is None

    def test_agg_map_has_all_standard_aggs(self):
        assert "SUM" in AGG_MAP
        assert "COUNT" in AGG_MAP
        assert "DISTINCT_COUNT" in AGG_MAP
        assert AGG_MAP["AVG"] == "AVERAGE"

    def test_visual_types_has_common_types(self):
        assert "KPIVisual" in VISUAL_TYPES
        assert "TableVisual" in VISUAL_TYPES
        assert "BarChartVisual" in VISUAL_TYPES


# =========================================================================
# Settings tests
# =========================================================================


class TestSettings:
    """Tests for the Settings dataclass."""

    def test_default_values(self):
        s = Settings()
        assert s.aws_region == "us-east-1"
        assert s.cache_ttl_seconds == 300
        assert s.verify_by_default is True
        assert s.character_limit == 25_000

    def test_override_values(self):
        s = Settings(cache_ttl_seconds=600, verify_by_default=False)
        assert s.cache_ttl_seconds == 600
        assert s.verify_by_default is False


# =========================================================================
# Exception hierarchy tests
# =========================================================================


class TestExceptions:
    """Tests for the structured exception hierarchy."""

    def test_qs_error_base(self):
        e = QSError("something broke", resource_id="ds-123")
        assert e.error_type == "unknown"
        assert e.resource_id == "ds-123"
        d = e.to_dict()
        assert d["error_type"] == "unknown"
        assert d["resource_id"] == "ds-123"
        assert "something broke" in d["error"]

    def test_auth_error(self):
        e = QSAuthError()
        assert e.error_type == "auth_expired"
        assert len(e.suggestions) > 0

    def test_not_found_error(self):
        e = QSNotFoundError("Dataset", "ds-abc")
        assert e.error_type == "not_found"
        assert "ds-abc" in str(e)
        assert e.resource_id == "ds-abc"

    def test_validation_error(self):
        e = QSValidationError("SQL must contain SELECT")
        assert e.error_type == "validation"

    def test_rate_limit_error(self):
        e = QSRateLimitError()
        assert e.error_type == "rate_limited"

    def test_concurrent_modification_error(self):
        e = ConcurrentModificationError("a-123", "time1", "time2")
        assert e.error_type == "concurrent_modification"
        assert e.analysis_id == "a-123"
        assert e.expected_time == "time1"
        assert e.actual_time == "time2"
        d = e.to_dict()
        assert d["metadata"]["expected_time"] == "time1"

    def test_change_verification_error(self):
        e = ChangeVerificationError("add_calc_field", "a-123", "Field not found")
        assert e.error_type == "verification_failed"
        assert e.operation == "add_calc_field"

    def test_destructive_change_error(self):
        e = DestructiveChangeError(
            "a-123",
            "Would delete all sheets",
            {"sheets": 3, "visuals": 10},
            {"sheets": 0, "visuals": 0},
        )
        assert e.error_type == "destructive_blocked"
        assert e.current_counts["sheets"] == 3
        assert e.new_counts["sheets"] == 0
        d = e.to_dict()
        assert d["metadata"]["current_counts"]["sheets"] == 3

    def test_all_exceptions_inherit_from_qs_error(self):
        """Every custom exception should be catchable as QSError."""
        exceptions = [
            QSAuthError(),
            QSNotFoundError("X", "y"),
            QSValidationError("bad"),
            QSRateLimitError(),
            ConcurrentModificationError("a", "t1", "t2"),
            ChangeVerificationError("op", "r", "d"),
            DestructiveChangeError("a", "d", {}, {}),
        ]
        for e in exceptions:
            assert isinstance(e, QSError)


# =========================================================================
# AwsClient tests
# =========================================================================


class TestAwsClient:
    """Tests for AwsClient credential refresh cascade."""

    @patch("quicksight_mcp.core.aws_client.boto3")
    def test_init_creates_session(self, mock_boto3):
        mock_session = MagicMock()
        mock_session.client.return_value = MagicMock()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456"}
        mock_session.client.side_effect = lambda svc, **kw: (
            mock_sts if svc == "sts" else MagicMock()
        )
        mock_boto3.Session.return_value = mock_session

        from quicksight_mcp.core.aws_client import AwsClient

        settings = Settings(aws_region="us-west-2")
        client = AwsClient(settings)
        assert client.account_id == "123456"

    @patch("quicksight_mcp.core.aws_client.boto3")
    def test_call_retries_on_expired(self, mock_boto3):
        mock_session = MagicMock()
        mock_qs = MagicMock()
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123"}

        # First call to list_data_sets raises ExpiredToken,
        # second succeeds after refresh
        mock_qs.list_data_sets.side_effect = [
            Exception("ExpiredToken: token expired"),
            {"DataSetSummaries": [{"Name": "test"}]},
        ]
        mock_session.client.side_effect = lambda svc, **kw: (
            mock_sts if svc == "sts" else mock_qs
        )
        mock_boto3.Session.return_value = mock_session

        from quicksight_mcp.core.aws_client import AwsClient

        settings = Settings()
        client = AwsClient(settings)

        result = client.call(
            "list_data_sets", AwsAccountId="123"
        )
        assert result["DataSetSummaries"][0]["Name"] == "test"
