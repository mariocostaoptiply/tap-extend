"""TapExtend -- Singer tap for the Extend Commerce (Lxir) REST API."""

from __future__ import annotations

import logging
from typing import Any, List

from hotglue_singer_sdk import Tap
from hotglue_singer_sdk import typing as th

from tap_extend.streams import (
    CustomerOrdersStream,
    ProductAvailabilityStream,
    ProductSupplierAgreementsStream,
    ProductsCreatedStream,
    ProductsStream,
    PurchaseOrdersStream,
    ReportsOrderHeadersStream,
    ReportsOrderRowsStream,
    SupplierAgreementsStream,
    SuppliersStream,
)


class _SuppressEmptyUnmappedPropertiesFilter(logging.Filter):
    """Suppress SDK noise when it logs an empty tuple of unmapped properties."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "Properties () were present" not in record.getMessage()


def _install_logging_filters() -> None:
    tap_logger = logging.getLogger("tap-extend")
    if not any(
        isinstance(existing_filter, _SuppressEmptyUnmappedPropertiesFilter)
        for existing_filter in tap_logger.filters
    ):
        tap_logger.addFilter(_SuppressEmptyUnmappedPropertiesFilter())


_install_logging_filters()


class TapExtend(Tap):
    """Singer tap for Extend Commerce (Lxir) REST API.

    Streams:
      - suppliers                     FULL_TABLE  GET /Supplier
      - supplier_agreements           FULL_TABLE  GET /SupplierAgreement (active=true)
      - product_supplier_agreements   INCREMENTAL GET /ProductSupplierAgreements (child of supplier_agreements)
      - products                      INCREMENTAL GET /Products (first run unfiltered, then modifiedDateFrom/modifiedDateTo)
      - products_created              INCREMENTAL GET /Products (unfiltered scan, local createDate window + detail)
      - product_availability          INCREMENTAL GET /ProductAvailability (modifiedDateFrom)
      - customer_orders               INCREMENTAL after bookmark exists via GET /CustomerOrders + detail (modifiedDateFrom/modifiedDateTo)
      - purchase_orders               INCREMENTAL GET /PurchaseOrders (createDateFrom)
      - reports_order_headers         INCREMENTAL historical-only GET /reports/{client}/OrderHeaders (changeDate day-by-day)
      - reports_order_rows            INCREMENTAL historical-only GET /reports/{client}/OrderRows    (changeDate day-by-day)
    """

    name = "tap-extend"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            default="https://s05.extend.se/RESTAPI",
            description="Base URL for the Extend Commerce REST API",
        ),
        th.Property(
            "client",
            th.StringType,
            required=True,
            description="Client shortname as configured in Extend Commerce (e.g. YOURCLIENT)",
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="Username for Extend API authentication",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Password for Extend API authentication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="Earliest record date to sync (ISO 8601)",
        ),
        th.Property(
            "warehouse_codes",
            th.StringType,
            required=False,
            description=(
                "Comma-separated warehouse codes to include. "
                "If omitted, all warehouses are synced."
            ),
        ),
    ).to_dict()

    @property
    def warehouse_codes(self) -> list[str] | None:
        """Return warehouse_codes as a list, coercing from string if needed."""
        wc = self.config.get("warehouse_codes")
        if isinstance(wc, str):
            return [c.strip() for c in wc.split(",") if c.strip()] or None
        return wc or None

    def load_state(self, state: dict[str, Any]) -> None:
        """Normalize bookmarks and preserve report-only top-level overrides."""
        normalized_state = self._normalize_legacy_bookmarks(state)
        self._loaded_state = normalized_state
        super().load_state(normalized_state)

        # hotglue_singer_sdk.Tap.load_state only copies values under
        # "bookmarks". Keep these report-only controls at the root of the
        # in-memory tap state so report streams can use one-off backfill bounds
        # without exposing them as config.
        for key in ("reports_start_date", "reports_end_date"):
            value = normalized_state.get(key)
            if value not in (None, ""):
                self.state[key] = value

    def _normalize_legacy_bookmarks(self, state: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(state, dict):
            return {}

        bookmarks = state.get("bookmarks")
        if not isinstance(bookmarks, dict):
            return state

        normalized_state = dict(state)
        normalized_bookmarks: dict[str, dict[str, Any]] = {}

        for stream_name, stream_state in bookmarks.items():
            if isinstance(stream_state, dict):
                if stream_name == "product_supplier_agreements":
                    stream_state = self._normalize_product_supplier_agreements_bookmark(
                        stream_state
                    )
                normalized_bookmarks[stream_name] = stream_state
                continue

            stream = self.streams.get(stream_name)
            replication_key = getattr(stream, "replication_key", None) if stream else None

            if replication_key and stream_state not in (None, ""):
                self.logger.warning(
                    "Coercing legacy scalar bookmark for stream '%s' into Singer state.",
                    stream_name,
                )
                normalized_bookmarks[stream_name] = {
                    "replication_key": replication_key,
                    "replication_key_value": stream_state,
                }
                continue

            self.logger.warning(
                "Ignoring malformed bookmark for stream '%s': expected an object, got %s.",
                stream_name,
                type(stream_state).__name__,
            )
            normalized_bookmarks[stream_name] = {}

        normalized_state["bookmarks"] = normalized_bookmarks
        return normalized_state

    def _normalize_product_supplier_agreements_bookmark(
        self,
        stream_state: dict[str, Any],
    ) -> dict[str, Any]:
        """Promote legacy PSA progress-marker state to a normal Singer bookmark."""
        if stream_state.get("replication_key") == "changeDate" and stream_state.get(
            "replication_key_value"
        ) not in (None, ""):
            return stream_state

        bookmark_value = stream_state.get("replication_key_signpost")
        source = "replication_key_signpost"
        progress_markers = stream_state.get("progress_markers")
        if bookmark_value in (None, "") and isinstance(progress_markers, dict):
            if progress_markers.get("replication_key") == "changeDate":
                bookmark_value = progress_markers.get("replication_key_value")
                source = "progress_markers.replication_key_value"

        if bookmark_value in (None, ""):
            return stream_state

        self.logger.warning(
            "Normalizing product_supplier_agreements bookmark from %s=%s.",
            source,
            bookmark_value,
        )
        normalized = dict(stream_state)
        normalized["replication_key"] = "changeDate"
        normalized["replication_key_value"] = bookmark_value
        normalized.pop("replication_key_signpost", None)
        normalized.pop("starting_replication_value", None)
        normalized.pop("progress_markers", None)
        return normalized

    def discover_streams(self) -> List:
        """Return stream instances."""
        return [
            SuppliersStream(tap=self),
            SupplierAgreementsStream(tap=self),
            ProductSupplierAgreementsStream(tap=self),
            ProductsStream(tap=self),
            ProductsCreatedStream(tap=self),
            ProductAvailabilityStream(tap=self),
            CustomerOrdersStream(tap=self),
            PurchaseOrdersStream(tap=self),
            ReportsOrderHeadersStream(tap=self),
            ReportsOrderRowsStream(tap=self),
        ]


if __name__ == "__main__":
    TapExtend.cli()
