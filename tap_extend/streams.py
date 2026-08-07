"""Stream definitions for tap-extend.

Streams:
  - SuppliersStream:                  GET /Supplier                  (FULL_TABLE)
  - SupplierAgreementsStream:         GET /SupplierAgreement         (FULL_TABLE, active=true)
  - ProductSupplierAgreementsStream:  GET /ProductSupplierAgreements (INCREMENTAL child of supplier_agreements)
  - ProductsStream:                   GET /Products                  (INCREMENTAL, first run unfiltered then modifiedDateFrom/modifiedDateTo)
  - ProductsCreatedStream:            GET /Products + detail         (INCREMENTAL, unfiltered scan + local createDate window)
  - ProductAvailabilityStream:        GET /ProductAvailability       (INCREMENTAL, modifiedDateFrom)
  - CustomerOrdersStream:             first run via reports, then GET /CustomerOrders + detail
  - PurchaseOrdersStream:             GET /PurchaseOrders            (INCREMENTAL, createDateFrom)
  - ReportsOrderHeadersStream:        GET /reports/{client}/OrderHeaders  (INCREMENTAL, changeDate day-by-day)
  - ReportsOrderRowsStream:           GET /reports/{client}/OrderRows     (INCREMENTAL, changeDate day-by-day)

All streams share ExtendStream base class for auth/HTTP/state handling.

Pagination:
  - Products, CustomerOrders: pageCount + pageOffset
  - Supplier, SupplierAgreement, ProductSupplierAgreements, PurchaseOrders, ProductAvailability: pageNumber (1-based)
  - ReportsOrderHeaders, ReportsOrderRows: pageNumber (1-based) per day — iterates one day at a time,
    paginating all pages within each day before advancing to the next.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
from email.utils import parsedate_to_datetime
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import backoff
import requests

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream

try:
    from hotglue_singer_sdk.helpers._state import (
        finalize_state_progress_markers as finalize_sdk_state_progress_markers,
    )
except ImportError:  # pragma: no cover - test stubs only
    def finalize_sdk_state_progress_markers(stream_or_partition_state: dict) -> None:
        """Fallback no-op when tests stub only the minimal SDK surface."""
        return None

try:
    from hotglue_singer_sdk.exceptions import InvalidCredentialsError
except ImportError:
    class InvalidCredentialsError(Exception):
        """Raised when API credentials are invalid."""


logger = logging.getLogger(__name__)

_SIGNPOST_BOOKMARK_STREAMS = {
    "product_supplier_agreements": "changeDate",
    "products": "modifiedDate",
    "products_created": "createDate",
    "customer_orders": "changeDate",
}

REQUESTS_PER_SECOND = 4.0
MIN_REQUESTS_PER_SECOND = 0.1
MAX_REQUESTS_PER_SECOND = 15.0
SECONDS_PER_RATE_LIMIT_WINDOW = 60.0
RETRYABLE_CLIENT_ERROR_WAIT_SECONDS = 10.0
RETRYABLE_CLIENT_ERROR_MAX_ATTEMPTS = 0  # 0 means retry indefinitely.
_LOGGED_UNMAPPED_REPORT_FIELDS: set[tuple[str, tuple[str, ...]]] = set()


class _RetryableError(Exception):
    """Raised for errors that should trigger backoff retry (429, 5xx)."""


def _remaining_requests_label(response: requests.Response) -> str:
    headers = getattr(response, "headers", {}) or {}
    return headers.get("x-ratelimit-remaining", "unknown")


def _strip_timezone_offset(value: Any) -> Any:
    """Strip trailing timezone offsets while preserving local datetime text."""
    if not isinstance(value, str) or not value:
        return value

    text = value.strip()
    if text.endswith("Z") and "T" in text:
        return text[:-1]

    for sign in ("+", "-"):
        marker_index = text.rfind(sign)
        if marker_index <= len("YYYY-MM-DDT"):
            continue
        suffix = text[marker_index:]
        if len(suffix) == 6 and suffix[3] == ":" and suffix[1:3].isdigit() and suffix[4:6].isdigit():
            return text[:marker_index]

    return text


# ---------------------------------------------------------------------------
# Shared base class
# ---------------------------------------------------------------------------


class ExtendStream(Stream):
    """Base class for all Extend Commerce streams."""

    _session: Optional[requests.Session] = None
    _next_request_at = 0.0  # Backwards-compatible fallback for tests/older call sites.
    _rate_limit_next_request_at: dict[str, float] = {}
    _rate_limit_requests_per_second: dict[str, float] = {}
    _rate_limit_ignored_higher_logged: set[str] = set()

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            credentials = base64.b64encode(
                f"{self.config['username']}:{self.config['password']}".encode("utf-8")
            ).decode("utf-8")
            self._session.headers.update({
                "ExtendBasicAuthorization": f"Basic {credentials}",
                "Accept": "application/json",
            })
            # Extend uses header-based auth only. Disable cookies to prevent
            # accumulation over long syncs (hundreds of pages) which can cause
            # the server to reject requests with 400.
            self._session.cookies.set_policy(
                __import__("http.cookiejar", fromlist=["DefaultCookiePolicy"])
                .DefaultCookiePolicy(allowed_domains=[])
            )
        return self._session

    @property
    def base_url(self) -> str:
        api_url = self.config.get("api_url", "https://s05.extend.se/RESTAPI").rstrip("/")
        return f"{api_url}/v1_0/{self.config['client']}"

    @property
    def requests_per_second(self) -> float:
        configured = self.config.get("max_requests_per_second", MAX_REQUESTS_PER_SECOND)
        try:
            configured_rate = float(configured)
        except (TypeError, ValueError):
            configured_rate = MAX_REQUESTS_PER_SECOND
        return max(min(configured_rate, MAX_REQUESTS_PER_SECOND), MIN_REQUESTS_PER_SECOND)

    @property
    def sync_upper_bound(self) -> str:
        """Return one stable upper-bound timestamp for the current tap run."""
        tap = getattr(self, "_tap", None)
        attr = "_extend_sync_upper_bound"
        if tap is not None:
            upper_bound = getattr(tap, attr, None)
            if upper_bound is None:
                upper_bound = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                setattr(tap, attr, upper_bound)
            return upper_bound

        if not hasattr(self, "_extend_sync_upper_bound"):
            self._extend_sync_upper_bound = (
                datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            )
        return self._extend_sync_upper_bound

    @property
    def sync_upper_bound_date(self) -> str:
        """Return the YYYY-MM-DD date for the current tap run upper bound."""
        return self.sync_upper_bound[:10]

    @staticmethod
    def _next_day(date_str: str) -> str:
        """Return the next day for a YYYY-MM-DD string."""
        return (
            datetime.strptime(date_str[:10], "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

    @staticmethod
    def _format_extend_datetime(value: Any) -> str:
        """Format a datetime-like value for Extend query params without timezone."""
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

        text_value = str(value)
        if text_value.endswith("Z"):
            text_value = text_value[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(text_value)
        except ValueError:
            return str(value)

        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)

        return parsed.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

    @property
    def request_timeout(self) -> float:
        """Per-request timeout in seconds. Default 120, configurable via request_timeout_seconds."""
        raw = self.config.get("request_timeout_seconds", 120)
        try:
            return max(float(raw), 10.0)
        except (TypeError, ValueError):
            return 120.0

    @property
    def retryable_client_error_wait_seconds(self) -> float:
        """Wait between transient Extend 400 retries, such as SQL deadlocks."""
        return RETRYABLE_CLIENT_ERROR_WAIT_SECONDS

    @property
    def retryable_client_error_max_attempts(self) -> int:
        """Max attempts for transient Extend 400s. 0 means retry indefinitely."""
        return RETRYABLE_CLIENT_ERROR_MAX_ATTEMPTS

    def _rate_limit_bucket(self, url: str) -> str:
        """Return the shared rate-limit bucket for an Extend URL.

        Extend exposes different limits for endpoint families, and those limits may vary
        per customer. Detail/list routes for the same resource are treated as one bucket;
        reports are treated as one shared bucket because OrderHeaders and OrderRows
        responses expose the same reset window.
        """
        path = url.split("?", 1)[0].rstrip("/")
        marker = "/RESTAPI/"
        if marker in path:
            path = path.split(marker, 1)[1]
        parts = [part for part in path.split("/") if part]

        if parts and parts[0].lower() == "reports":
            client = parts[1] if len(parts) > 1 else self.config.get("client", "")
            return f"reports:{client}"

        if len(parts) >= 3 and parts[0].lower() == "v1_0":
            client = parts[1]
            resource = parts[2]
            singular_resources = {
                "Product": "Products",
                "SupplierAgreement": "SupplierAgreements",
                "PurchaseOrder": "PurchaseOrders",
            }
            resource = singular_resources.get(resource, resource)
            return f"v1_0:{client}:{resource}"

        return path

    def _bucket_requests_per_second(self, bucket: str) -> float:
        return ExtendStream._rate_limit_requests_per_second.get(bucket, self.requests_per_second)

    def _apply_client_throttle(self, url: Optional[str] = None) -> None:
        bucket = self._rate_limit_bucket(url) if url else "default"
        min_interval = 1.0 / self._bucket_requests_per_second(bucket)
        now = time.monotonic()
        next_request_at = ExtendStream._rate_limit_next_request_at.get(
            bucket,
            ExtendStream._next_request_at if bucket == "default" else 0.0,
        )
        sleep_for = next_request_at - now
        if sleep_for > 0:
            time.sleep(sleep_for)
            now = time.monotonic()
        ExtendStream._rate_limit_next_request_at[bucket] = max(next_request_at, now) + min_interval
        if bucket == "default":
            ExtendStream._next_request_at = ExtendStream._rate_limit_next_request_at[bucket]

    def _remaining_requests_label(self, response: requests.Response) -> str:
        return _remaining_requests_label(response)

    def _update_rate_limit_from_response(self, response: requests.Response) -> None:
        request_url = getattr(response, "url", None) or getattr(
            getattr(response, "request", None), "url", ""
        )
        if not request_url:
            return

        bucket = self._rate_limit_bucket(request_url)
        limit_value = response.headers.get("x-ratelimit-limit")
        if limit_value:
            try:
                header_requests_per_second = float(limit_value) / SECONDS_PER_RATE_LIMIT_WINDOW
            except (TypeError, ValueError):
                header_requests_per_second = None
            if header_requests_per_second and header_requests_per_second > 0:
                effective_rate = min(
                    max(header_requests_per_second, MIN_REQUESTS_PER_SECOND),
                    self.requests_per_second,
                )
                previous_rate = ExtendStream._rate_limit_requests_per_second.get(bucket)
                if previous_rate is not None and effective_rate > previous_rate:
                    if bucket not in ExtendStream._rate_limit_ignored_higher_logged:
                        logger.info(
                            "Rate limit bucket %s keeping %.3f requests/sec; ignoring higher "
                            "x-ratelimit-limit=%s (%.3f requests/sec).",
                            bucket,
                            previous_rate,
                            limit_value,
                            effective_rate,
                        )
                        ExtendStream._rate_limit_ignored_higher_logged.add(bucket)
                    effective_rate = previous_rate

                ExtendStream._rate_limit_requests_per_second[bucket] = effective_rate
                ExtendStream._rate_limit_next_request_at[bucket] = max(
                    ExtendStream._rate_limit_next_request_at.get(bucket, 0.0),
                    time.monotonic() + (1.0 / effective_rate),
                )
                if previous_rate != effective_rate:
                    logger.info(
                        "Rate limit bucket %s using %.3f requests/sec from x-ratelimit-limit=%s "
                        "(%s requests/min).",
                        bucket,
                        effective_rate,
                        limit_value,
                        limit_value,
                    )

        remaining_value = response.headers.get("x-ratelimit-remaining")
        try:
            remaining_is_exhausted = remaining_value is not None and float(remaining_value) <= 0
        except (TypeError, ValueError):
            remaining_is_exhausted = False
        if remaining_is_exhausted:
            delay_seconds = self._delay_from_reset_header(response)
            if delay_seconds:
                logger.info(
                    "Rate limit bucket %s exhausted; deferring next request %.1fs until reset.",
                    bucket,
                    delay_seconds,
                )
                self._defer_next_request(delay_seconds, bucket=bucket)

    def _delay_from_retry_after(self, retry_after: Optional[str]) -> Optional[float]:
        if not retry_after:
            return None
        try:
            return max(float(retry_after), 0.0)
        except (TypeError, ValueError):
            try:
                retry_at = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return None
            return max(retry_at.timestamp() - time.time(), 0.0)

    def _delay_from_reset_header(self, response: requests.Response) -> Optional[float]:
        reset_value = response.headers.get("x-ratelimit-reset")
        if not reset_value:
            return None
        try:
            reset_epoch = float(reset_value)
        except (TypeError, ValueError):
            return None
        return max(reset_epoch - time.time(), 0.0) + 1.0

    def _defer_next_request(self, delay_seconds: Optional[float], bucket: Optional[str] = None) -> None:
        if not delay_seconds or delay_seconds <= 0:
            return
        defer_until = time.monotonic() + delay_seconds
        if bucket:
            ExtendStream._rate_limit_next_request_at[bucket] = max(
                ExtendStream._rate_limit_next_request_at.get(bucket, 0.0),
                defer_until,
            )
            return

        ExtendStream._next_request_at = max(
            ExtendStream._next_request_at,
            defer_until,
        )
        ExtendStream._rate_limit_next_request_at["default"] = max(
            ExtendStream._rate_limit_next_request_at.get("default", 0.0),
            ExtendStream._next_request_at,
        )

    def _is_retryable_client_error(self, response: requests.Response) -> bool:
        """Return True for known transient 4xx responses misclassified by Extend."""
        if response.status_code != 400:
            return False

        message = (response.text or "").lower()
        if "deadlock" in message and "rerun the transaction" in message:
            return True
        if "timeout" in message and ("expired" in message or "execution" in message):
            return True
        return False

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout, _RetryableError),
        max_tries=8,
        factor=2,
        jitter=backoff.full_jitter,
    )
    def _request(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """GET with retry/backoff.  Only retries on 429, 5xx, connection errors, and transient 400s."""
        retryable_client_error_attempt = 0

        while True:
            self._apply_client_throttle(url)
            response = self.session.get(url, params=params, timeout=self.request_timeout)
            self._update_rate_limit_from_response(response)

            if response.status_code == 401:
                raise InvalidCredentialsError(
                    f"Authentication failed (401): {response.text[:300]}"
                )
            if response.status_code == 429:
                retry_after = self._delay_from_retry_after(response.headers.get("Retry-After"))
                reset_delay = self._delay_from_reset_header(response)
                delay_seconds = max(
                    retry_after if retry_after is not None else 0.0,
                    reset_delay if reset_delay is not None else 0.0,
                    30.0,
                )
                logger.warning("Rate limited (429). Sleeping %.1fs.", delay_seconds)
                self._defer_next_request(delay_seconds, bucket=self._rate_limit_bucket(url))
                time.sleep(delay_seconds)
                raise _RetryableError("Rate limited (429)")
            if response.status_code >= 500:
                raise _RetryableError(
                    f"Server error ({response.status_code}): {response.text[:300]}"
                )
            if not self._is_retryable_client_error(response):
                break

            retryable_client_error_attempt += 1
            max_attempts = self.retryable_client_error_max_attempts
            if max_attempts and retryable_client_error_attempt >= max_attempts:
                logger.error(
                    "Transient Extend client error (%s) persisted after %d attempts: %s",
                    response.status_code,
                    retryable_client_error_attempt,
                    response.text[:300],
                )
                break

            wait_seconds = self.retryable_client_error_wait_seconds
            max_attempts_label = f"/{max_attempts}" if max_attempts else ""
            request_url = getattr(response, "url", None) or getattr(
                getattr(response, "request", None), "url", url
            )
            logger.warning(
                "Retrying transient Extend client error for %s (%s) in %.1fs "
                "(attempt %d%s): %s",
                request_url,
                response.status_code,
                wait_seconds,
                retryable_client_error_attempt,
                max_attempts_label,
                response.text[:300],
            )
            time.sleep(wait_seconds)

        # 4xx (except 429 handled above) are client errors — log body for diagnosis, then fail
        if response.status_code >= 400:
            logger.error(
                "%s %s -> %d: %s",
                response.request.method,
                response.url,
                response.status_code,
                response.text[:500],
            )
        response.raise_for_status()
        return response

    def _write_state_message(self) -> None:
        """Emit state without transient signpost/progress marker noise."""
        try:
            live_state = self.tap_state
            if not isinstance(live_state, dict):
                super()._write_state_message()
                return

            sanitized_state = copy.deepcopy(live_state)
            bookmarks = sanitized_state.get("bookmarks", {})
            if isinstance(bookmarks, dict):
                for stream_name, stream_state in list(bookmarks.items()):
                    if not isinstance(stream_state, dict):
                        continue
                    if stream_name in _SIGNPOST_BOOKMARK_STREAMS:
                        bookmarks[stream_name] = _sanitize_signpost_bookmark_state(
                            stream_state,
                            _SIGNPOST_BOOKMARK_STREAMS[stream_name],
                        )
                    else:
                        stream_state.pop("partitions", None)

            original_state = self._tap_state
            self._tap_state = sanitized_state
            try:
                super()._write_state_message()
            finally:
                self._tap_state = original_state
        except Exception as exc:
            self.logger.warning("Error writing state message: %s", exc)


# ---------------------------------------------------------------------------
# SuppliersStream  —  GET /Supplier
# ---------------------------------------------------------------------------


class SuppliersStream(ExtendStream):
    """Extend Commerce Suppliers (company-level).

    Schema from SupplierListItem definition.
    FULL_TABLE — no date filter available on this endpoint.
    Pagination: pageNumber (1-based).
    Response wrapper key: SupplierList.
    """

    name = "suppliers"
    primary_keys = ["supplierNumber"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property("supplierNumber", th.StringType),
        th.Property("name", th.StringType),
        th.Property("shortName", th.StringType),
        th.Property("organizationNumber", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("address3", th.StringType),
        th.Property("postalCode", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("countryId", th.StringType),
        th.Property("companyPhone", th.StringType),
        th.Property("rating", th.StringType),
        th.Property("manualRating", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/Supplier", params={"pageNumber": page}
            ).json()
            items = data.get("SupplierList", [])
            pagination = data.get("paginationInfo", {})

            for s in items:
                yield {
                    "supplierNumber": s.get("supplierNumber"),
                    "name": s.get("name"),
                    "shortName": s.get("shortName"),
                    "organizationNumber": s.get("organizationNumber"),
                    "address1": s.get("address1"),
                    "address2": s.get("address2"),
                    "address3": s.get("address3"),
                    "postalCode": s.get("postalCode"),
                    "city": s.get("city"),
                    "state": s.get("state"),
                    "countryId": s.get("countryId"),
                    "companyPhone": s.get("companyPhone"),
                    "rating": s.get("rating"),
                    "manualRating": s.get("manualRating"),
                }

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# SupplierAgreementsStream  —  GET /SupplierAgreement
# ---------------------------------------------------------------------------


class SupplierAgreementsStream(ExtendStream):
    """Extend Commerce Supplier Agreements (contract/brand level).

    These are the entities used as Optiply Suppliers — they carry lead times,
    currency and payment terms needed for replenishment. Each agreement maps
    to one purchasing contract (e.g. "Gandalf - Corsair", "Fifine").

    NOTE: supplierNumber (FK to parent Supplier) is NOT returned by the list
    endpoint — only by GET /SupplierAgreement/{id}. Omitted here to avoid
    525 extra detail calls. Add if the ETL needs the company-level grouping.

    Filters active=true per Xavier's requirements (2026-03-06 email).

    Schema from SupplierAgreementListItem definition plus newer fields returned
    by the updated API contract.
    FULL_TABLE parent stream for ProductSupplierAgreementsStream.
    Pagination: pageNumber (1-based).
    Response wrapper key: SupplierAgreementList.
    """

    name = "supplier_agreements"
    primary_keys = ["supplierAgreementNumber"]
    replication_method = "FULL_TABLE"

    schema = th.PropertiesList(
        th.Property("supplierAgreementNumber", th.IntegerType),
        th.Property("supplierAgreementId", th.StringType),
        th.Property("name", th.StringType),
        # supplierNumber (FK to Supplier) only on detail endpoint — not in list response
        th.Property("active", th.BooleanType),
        th.Property("currencyId", th.StringType),
        th.Property("manufacturingLeadTime", th.NumberType),
        th.Property("transportLeadTime", th.NumberType),
        th.Property("customsLeadTime", th.NumberType),
        th.Property("paymentTerms", th.StringType),
        th.Property("deliveryMethod", th.StringType),
        th.Property("forwarderCustomerNumber", th.StringType),
        th.Property("penalty", th.StringType),
        th.Property("incoterms", th.StringType),
        th.Property("transportConditionDescription", th.StringType),
        th.Property("purchaseNotificationSystem", th.StringType),
        th.Property("useRowBasedLeadTime", th.BooleanType),
        th.Property("validFrom", th.DateTimeType),
        th.Property("validTo", th.DateTimeType),
        th.Property("ordererAddress1", th.StringType),
        th.Property("ordererPostalCode", th.StringType),
        th.Property("ordererCity", th.StringType),
        th.Property("ordererCountryId", th.StringType),
        th.Property("makeAutomaticPurchase", th.BooleanType),
        th.Property("capacity", th.IntegerType),
        th.Property("authorizationNumber", th.StringType),
        th.Property("latitude", th.StringType),
        th.Property("longitude", th.StringType),
        th.Property("internalContact", th.StringType),
        th.Property("externalContact", th.StringType),
        th.Property("purchaseNotificationAddress", th.StringType),
        th.Property("changeDate", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        supplier_agreement_number = record.get("supplierAgreementNumber")
        if supplier_agreement_number is None:
            return context or {}
        return {"supplierAgreementNumber": supplier_agreement_number}

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        params_base: dict[str, Any] = {"active": "true"}

        page = 1
        while True:
            page_started_at = time.monotonic()
            logger.info("Requesting SupplierAgreement page %d: active=%s", page, params_base["active"])
            response = self._request(
                f"{self.base_url}/SupplierAgreement",
                params={**params_base, "pageNumber": page},
            )
            data = response.json()
            items = data.get("SupplierAgreementList", [])
            pagination = data.get("paginationInfo", {})
            current_page = int(pagination.get("currentPage") or page)
            total_pages = int(pagination.get("totalPages") or 0)
            logger.info(
                "SupplierAgreement page %d/%d returned %d records in %.1fs remaining_requests=%s",
                current_page,
                total_pages,
                len(items),
                time.monotonic() - page_started_at,
                self._remaining_requests_label(response),
            )

            for a in items:
                yield {
                    "supplierAgreementNumber": a.get("supplierAgreementNumber"),
                    "supplierAgreementId": a.get("supplierAgreementId"),
                    "name": a.get("name"),
                    "active": a.get("active"),
                    "currencyId": a.get("currencyId"),
                    "manufacturingLeadTime": a.get("manufacturingLeadTime"),
                    "transportLeadTime": a.get("transportLeadTime"),
                    "customsLeadTime": a.get("customsLeadTime"),
                    "paymentTerms": a.get("paymentTerms"),
                    "deliveryMethod": a.get("deliveryMethod"),
                    "forwarderCustomerNumber": a.get("forwarderCustomerNumber"),
                    "penalty": a.get("penalty"),
                    "incoterms": a.get("incoterms"),
                    "transportConditionDescription": a.get("transportConditionDescription"),
                    "purchaseNotificationSystem": a.get("purchaseNotificationSystem"),
                    "useRowBasedLeadTime": a.get("useRowBasedLeadTime"),
                    "validFrom": a.get("validFrom"),
                    "validTo": a.get("validTo"),
                    "ordererAddress1": a.get("ordererAddress1"),
                    "ordererPostalCode": a.get("ordererPostalCode"),
                    "ordererCity": a.get("ordererCity"),
                    "ordererCountryId": a.get("ordererCountryId"),
                    "makeAutomaticPurchase": a.get("makeAutomaticPurchase"),
                    "capacity": a.get("capacity"),
                    "authorizationNumber": a.get("authorizationNumber"),
                    "latitude": str(a.get("latitude")) if a.get("latitude") is not None else None,
                    "longitude": str(a.get("longitude")) if a.get("longitude") is not None else None,
                    "internalContact": a.get("internalContact"),
                    "externalContact": a.get("externalContact"),
                    "purchaseNotificationAddress": a.get("purchaseNotificationAddress"),
                    "changeDate": a.get("changeDate"),
                }

            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# ProductSupplierAgreementsStream  —  GET /ProductSupplierAgreements
# ---------------------------------------------------------------------------


class ProductSupplierAgreementsStream(ExtendStream):
    """Extend Commerce product↔SupplierAgreement links.

    One record per (product, supplier agreement) pair. Used as the primary
    source for Optiply SupplierProducts — replaces the per-product detail
    call that was previously embedded in ProductsStream.

    INCREMENTAL child stream: loops ProductSupplierAgreements once per
    supplierAgreementNumber emitted by SupplierAgreementsStream.

    First run: no changeDateFrom/changeDateTo filters, so the endpoint returns
    the full supplier agreement mapping set.

    Subsequent runs: use a stable tap-run changeDateTo plus the saved bookmark
    as changeDateFrom.
    Pagination: pageNumber (1-based).
    Response wrapper key: productSupplierAgreementList.
    """

    name = "product_supplier_agreements"
    parent_stream_type = SupplierAgreementsStream
    primary_keys = ["productNumber", "supplierAgreementNumber"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        th.Property("productNumber", th.StringType),
        th.Property("supplierAgreementNumber", th.IntegerType),
        th.Property("supplierAgreementName", th.StringType),
        th.Property("supplierName", th.StringType),
        th.Property("supplierAgreementCurrencyId", th.StringType),
        th.Property("supplierProductNumber", th.StringType),
        th.Property("supplierProductName", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("vatPercent", th.NumberType),
        th.Property("manufacturingLeadTimeHour", th.NumberType),
        th.Property("supplierAgreementProductionLeadtimeHours", th.IntegerType),
        th.Property("supplierAgreementTransportLeadtimeHours", th.IntegerType),
        th.Property("inactive", th.BooleanType),
        th.Property("statisticalNumber", th.StringType),
        th.Property("country", th.StringType),
        th.Property("productUnitId", th.StringType),
        th.Property("useOtherPurchaseUnit", th.BooleanType),
        th.Property("purchaseProductUnit", th.StringType),
        th.Property("quantityPerPurchaseProductUnit", th.NumberType),
        th.Property("changeDate", th.DateTimeType),
    ).to_dict()

    def _map(self, a: dict) -> dict:
        return {
            "productNumber": a.get("productNumber"),
            "supplierAgreementNumber": a.get("supplierAgreementNumber"),
            "supplierAgreementName": a.get("supplierAgreementName"),
            "supplierName": a.get("supplierName"),
            "supplierAgreementCurrencyId": a.get("supplierAgreementCurrencyId"),
            "supplierProductNumber": a.get("supplierProductNumber"),
            "supplierProductName": a.get("supplierProductName"),
            "price": a.get("price"),
            "vatPercent": a.get("vatPercent"),
            "manufacturingLeadTimeHour": a.get("manufacturingLeadTimeHour"),
            "supplierAgreementProductionLeadtimeHours": a.get("supplierAgreementProductionLeadtimeHours"),
            "supplierAgreementTransportLeadtimeHours": a.get("supplierAgreementTransportLeadtimeHours"),
            "inactive": a.get("inactive"),
            "statisticalNumber": a.get("statisticalNumber"),
            "country": a.get("country"),
            "productUnitId": a.get("productUnitId"),
            "useOtherPurchaseUnit": a.get("useOtherPurchaseUnit"),
            "purchaseProductUnit": a.get("purchaseProductUnit"),
            "quantityPerPurchaseProductUnit": a.get("quantityPerPurchaseProductUnit"),
            "changeDate": a.get("changeDate"),
        }

    def _get_state_partition_context(self, context: Optional[dict]) -> Optional[dict]:
        """Keep one global bookmark across all supplierAgreementNumber child syncs."""
        return None

    def get_replication_key_signpost(self, context: Optional[dict]) -> Optional[str]:
        """Freeze bookmark advancement to the tap-run upper bound."""
        return self._format_extend_datetime(self.sync_upper_bound)

    def finalize_state_progress_markers(self, state: Optional[dict] = None) -> None:
        """Persist the tap-run upper bound even when no child request emits records."""
        if state in (None, {}):
            for context in self.partitions or [{}]:
                finalize_sdk_state_progress_markers(self.get_context_state(context or None))
            target_state = self.stream_state
        else:
            finalize_sdk_state_progress_markers(state)
            target_state = state
        target_state["replication_key"] = self.replication_key
        target_state["replication_key_value"] = self.get_replication_key_signpost(None)
        target_state.pop("replication_key_signpost", None)
        target_state.pop("starting_replication_value", None)
        target_state.pop("progress_markers", None)

    def _get_start_replication_value(self, state: dict[str, Any]) -> Optional[str]:
        """Return the best usable ProductSupplierAgreements bookmark.

        Older completed jobs can persist SDK progress-marker state without promoting it
        to the top-level replication_key/replication_key_value pair. Prefer the
        completed-run signpost in that shape so the next run remains incremental.
        """
        if state.get("replication_key") == self.replication_key:
            value = state.get("replication_key_value")
            return str(value) if value not in (None, "") else None

        signpost = state.get("replication_key_signpost")
        if signpost not in (None, ""):
            current_run_signpost = self.get_replication_key_signpost(None)
            if str(signpost) == current_run_signpost:
                return None
            logger.warning(
                "Using ProductSupplierAgreements replication_key_signpost=%s as legacy bookmark; "
                "state was not finalized to replication_key_value.",
                signpost,
            )
            return str(signpost)

        progress_markers = state.get("progress_markers")
        if (
            isinstance(progress_markers, dict)
            and progress_markers.get("replication_key") == self.replication_key
        ):
            value = progress_markers.get("replication_key_value")
            if value not in (None, ""):
                logger.warning(
                    "Using ProductSupplierAgreements progress marker %s=%s as legacy bookmark; "
                    "state was not finalized.",
                    self.replication_key,
                    value,
                )
                return str(value)

        return None

    def _increment_stream_state(
        self, latest_record: dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        """Skip SDK bookmark comparisons for legacy rows missing changeDate."""
        if latest_record.get(self.replication_key) in (None, ""):
            logger.debug(
                "Skipping state increment for ProductSupplierAgreements row without %s "
                "(supplierAgreementNumber=%s, productNumber=%s)",
                self.replication_key,
                latest_record.get("supplierAgreementNumber"),
                latest_record.get("productNumber"),
            )
            return
        super()._increment_stream_state(latest_record, context=context)

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        supplier_agreement_number = (context or {}).get("supplierAgreementNumber")
        if supplier_agreement_number in (None, ""):
            raise ValueError(
                "ProductSupplierAgreementsStream requires supplierAgreementNumber context "
                "from SupplierAgreementsStream."
            )

        url = f"{self.base_url}/ProductSupplierAgreements"
        total_emitted = 0
        current_state = self.get_context_state(context)
        start_replication = self._get_start_replication_value(current_state)
        params_base: dict[str, Any] = {
            "supplierAgreementNumber": supplier_agreement_number,
        }
        if start_replication:
            params_base["changeDateFrom"] = self._format_extend_datetime(start_replication)
            params_base["changeDateTo"] = self.get_replication_key_signpost(context)

        try:
            page = 1
            while True:
                page_started_at = time.monotonic()
                logger.info(
                    "Requesting ProductSupplierAgreements supplierAgreementNumber=%s page=%d "
                    "changeDateFrom=%s changeDateTo=%s",
                    supplier_agreement_number,
                    page,
                    params_base.get("changeDateFrom"),
                    params_base.get("changeDateTo"),
                )
                response = self._request(url, params={**params_base, "pageNumber": page})
                data = response.json()
                items = data.get("productSupplierAgreementList", [])
                pagination = data.get("paginationInfo", {})
                current_page = int(pagination.get("currentPage") or page)
                total_pages = int(pagination.get("totalPages") or 0)
                logger.info(
                    "ProductSupplierAgreements supplierAgreementNumber=%s page %d/%d returned "
                    "%d records in %.1fs remaining_requests=%s",
                    supplier_agreement_number,
                    current_page,
                    total_pages,
                    len(items),
                    time.monotonic() - page_started_at,
                    self._remaining_requests_label(response),
                )

                for a in items:
                    total_emitted += 1
                    yield self._map(a)

                if page >= total_pages:
                    break
                page += 1
        except requests.exceptions.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 400:
                raise
            logger.warning(
                "ProductSupplierAgreements: supplierAgreementNumber=%s returned 400, "
                "skipping child sync (%s)",
                supplier_agreement_number,
                exc,
            )

        logger.info(
            "ProductSupplierAgreements done for supplierAgreementNumber=%s: emitted %d records",
            supplier_agreement_number,
            total_emitted,
        )


# ---------------------------------------------------------------------------
# ProductsStream  —  GET /Products
# ---------------------------------------------------------------------------


class ProductsStream(ExtendStream):
    """Extend Commerce Products with per-warehouse stock.

    List endpoint (GET /Products) returns one row per product-per-warehouse.
    This stream deduplicates by productNumber, aggregating warehouse stock
    into a JSON array.

    Supplier-product links are handled by ProductSupplierAgreementsStream
    (GET /ProductSupplierAgreements) — no per-product detail calls needed here.

    Incremental via modifiedDateFrom/modifiedDateTo server-side filters.
    Pagination: pageCount + pageOffset (NOT pageNumber).

    First run: no modifiedDate filters, so the endpoint returns the full
    product set.

    Subsequent runs: use a stable tap-run modifiedDateTo plus the saved
    bookmark as modifiedDateFrom.

    The endpoint does not expose a record-level modifiedDate field, so the
    incremental bookmark is signpost-based rather than derived from each row.

    Schema from ProductListItem definition.
    """

    name = "products"
    primary_keys = ["productNumber"]
    replication_key = "modifiedDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # ProductListItem fields
        th.Property("productNumber", th.StringType),
        th.Property("productName", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("productUnit", th.StringType),
        th.Property("cost", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("countryOfOrigin", th.StringType),
        th.Property("supplyMode", th.StringType),
        th.Property("manufacturer", th.StringType),
        th.Property("manufacturerProductNumber", th.StringType),
        th.Property("gtinNumberList", th.StringType),
        th.Property("enabled", th.BooleanType),
        th.Property("statisticalCategory1", th.StringType),
        th.Property("statisticalCategory2", th.StringType),
        th.Property("statisticalCategory3", th.StringType),
        th.Property("companyGroup", th.StringType),
        th.Property("financialCategory", th.StringType),
        # Product detail fields from GET /Products/{productNumber}.
        th.Property("annulled", th.BooleanType),
        th.Property("productHandlings", th.StringType),
        th.Property("assortmentCategory", th.StringType),
        th.Property("productVisibility", th.StringType),
        th.Property("detailChangedDate", th.DateTimeType),
        th.Property("modifiedDate", th.DateTimeType),
        # Aggregated per-warehouse stock as JSON: [{warehouse, availableBalance}]
        th.Property("warehouse_stock", th.StringType),
    ).to_dict()

    def get_replication_key_signpost(self, context: Optional[dict]) -> Optional[str]:
        """Freeze bookmark advancement to the tap-run upper bound."""
        return self._format_extend_datetime(self.sync_upper_bound)

    def finalize_state_progress_markers(self, state: Optional[dict] = None) -> None:
        """Persist the tap-run upper bound even when records lack modifiedDate."""
        if state in (None, {}):
            for context in self.partitions or [{}]:
                finalize_sdk_state_progress_markers(self.get_context_state(context or None))
            target_state = self.stream_state
        else:
            finalize_sdk_state_progress_markers(state)
            target_state = state
        target_state["replication_key"] = self.replication_key
        target_state["replication_key_value"] = self.get_replication_key_signpost(None)

    def _increment_stream_state(
        self, latest_record: dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        """Products bookmarks are signpost-based, not row-derived."""
        return


    def _get_product_detail_fields(self, product_number: str) -> dict[str, Any]:
        """Fetch and flatten product-detail fields needed for visibility/status rules."""
        try:
            detail = self._request(f"{self.base_url}/Products/{product_number}").json()
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            logger.warning(
                "Products: detail lookup failed for productNumber=%s status=%s; emitting list fields only",
                product_number,
                status_code,
            )
            return {}

        if not isinstance(detail, dict):
            return {}

        product_data = detail.get("productData") or {}
        if not isinstance(product_data, dict):
            product_data = {}

        product_services = product_data.get("productServices") or {}
        if not isinstance(product_services, dict):
            product_services = {}

        groups = product_data.get("productGroupsAndCategories") or {}
        if not isinstance(groups, dict):
            groups = {}

        product_dates = product_data.get("productDates") or {}
        if not isinstance(product_dates, dict):
            product_dates = {}

        product_handlings = product_services.get("productHandlings")
        if product_handlings is not None and not isinstance(product_handlings, str):
            product_handlings = json.dumps(product_handlings)

        return {
            "annulled": product_data.get("annulled"),
            "productHandlings": product_handlings,
            "assortmentCategory": groups.get("assortmentCategory"),
            "productVisibility": product_data.get("productVisibility"),
            "detailChangedDate": product_dates.get("changedDate"),
        }

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        seen: dict[str, dict] = {}
        stock_map: dict[str, list] = {}

        current_state = self.get_context_state(context)
        start_replication = None
        if current_state.get("replication_key") == self.replication_key:
            start_replication = current_state.get("replication_key_value")

        page_offset = 0
        page_count = 100
        total_rows = 0

        while True:
            params: dict[str, Any] = {"pageCount": page_count, "pageOffset": page_offset}
            if start_replication:
                params["modifiedDateFrom"] = self._format_extend_datetime(start_replication)
                params["modifiedDateTo"] = self.get_replication_key_signpost(context)

            try:
                product_list = self._request(
                    f"{self.base_url}/Products", params=params
                ).json()
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400 and page_offset > 0:
                    logger.warning(
                        "Products: 400 at pageOffset=%d after %d rows / %d unique products. "
                        "Yielding partial results. Response: %s",
                        page_offset, total_rows, len(seen),
                        exc.response.text[:500],
                    )
                    break
                raise

            if not isinstance(product_list, list) or not product_list:
                break

            total_rows += len(product_list)

            for p in product_list:
                pn = str(p.get("productNumber") or "")
                if not pn:
                    continue

                stock_map.setdefault(pn, []).append({
                    "warehouse": p.get("warehouse") or "",
                    "availableBalance": p.get("availableBalance") or 0,
                })

                if pn not in seen:
                    groups = p.get("productGroupsAndCategories") or {}
                    record = {
                        "productNumber": pn,
                        "productName": p.get("productName"),
                        "createDate": p.get("createDate"),
                        "productUnit": p.get("productUnit"),
                        "cost": p.get("cost"),
                        "currency": p.get("currency"),
                        "countryOfOrigin": p.get("countryOfOrigin"),
                        "supplyMode": p.get("supplyMode"),
                        "manufacturer": p.get("manufacturer"),
                        "manufacturerProductNumber": p.get("manufacturerProductNumber"),
                        "gtinNumberList": p.get("gtinNumberList"),
                        "enabled": p.get("enabled"),
                        "statisticalCategory1": p.get("statisticalCategory1"),
                        "statisticalCategory2": p.get("statisticalCategory2"),
                        "statisticalCategory3": p.get("statisticalCategory3"),
                        "companyGroup": groups.get("companyGroup"),
                        "financialCategory": groups.get("financialCategory"),
                        "modifiedDate": p.get("modifiedDate"),
                    }
                    record.update(self._get_product_detail_fields(pn))
                    seen[pn] = record

            if len(product_list) < page_count:
                break
            page_offset += 1

            if page_offset % 50 == 0:
                logger.info(
                    "Products: page %d — %d rows fetched, %d unique products so far",
                    page_offset, total_rows, len(seen),
                )

        logger.info("Products: done — %d rows, %d unique products", total_rows, len(seen))

        for pn, record in seen.items():
            record["warehouse_stock"] = json.dumps(stock_map.get(pn, []))
            yield record


class ProductsCreatedStream(ProductsStream):
    """Recover newly created products missed by Extend's modified-date filter.

    Every run scans the unfiltered Products list and groups warehouse rows by
    productNumber before deciding what to retain. It keeps products when any
    row has a valid createDate between the previous successful run (with a
    24-hour overlap) and the current tap-run upper bound. Each retained product
    gets exactly one detail request; products without a valid createDate are
    skipped.

    This is a temporary workaround for Extend not reliably populating
    changedDate when a product is created.
    """

    name = "products_created"
    replication_key = "createDate"
    replication_method = "INCREMENTAL"
    overlap = timedelta(hours=24)

    @staticmethod
    def _parse_create_date(value: Any) -> Optional[datetime]:
        if value in (None, ""):
            return None

        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError):
            return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _creation_window(self, context: Optional[dict]) -> tuple[datetime, datetime]:
        upper_bound = self._parse_create_date(self.sync_upper_bound)
        if upper_bound is None:  # pragma: no cover - sync_upper_bound is generated internally
            raise ValueError(f"Invalid products_created upper bound: {self.sync_upper_bound}")

        current_state = self.get_context_state(context)
        bookmark = None
        if current_state.get("replication_key") == self.replication_key:
            bookmark = self._parse_create_date(current_state.get("replication_key_value"))

        if bookmark is not None and bookmark > upper_bound:
            logger.warning(
                "ProductsCreated: bookmark %s is after run upper bound %s; using upper bound.",
                bookmark.isoformat(),
                upper_bound.isoformat(),
            )
            bookmark = upper_bound

        return (bookmark or upper_bound) - self.overlap, upper_bound

    @staticmethod
    def _map_list_record(product: dict[str, Any], product_number: str) -> dict[str, Any]:
        groups = product.get("productGroupsAndCategories") or {}
        if not isinstance(groups, dict):
            groups = {}

        return {
            "productNumber": product_number,
            "productName": product.get("productName"),
            "createDate": product.get("createDate"),
            "productUnit": product.get("productUnit"),
            "cost": product.get("cost"),
            "currency": product.get("currency"),
            "countryOfOrigin": product.get("countryOfOrigin"),
            "supplyMode": product.get("supplyMode"),
            "manufacturer": product.get("manufacturer"),
            "manufacturerProductNumber": product.get("manufacturerProductNumber"),
            "gtinNumberList": product.get("gtinNumberList"),
            "enabled": product.get("enabled"),
            "statisticalCategory1": product.get("statisticalCategory1"),
            "statisticalCategory2": product.get("statisticalCategory2"),
            "statisticalCategory3": product.get("statisticalCategory3"),
            "companyGroup": groups.get("companyGroup"),
            "financialCategory": groups.get("financialCategory"),
            "modifiedDate": product.get("modifiedDate"),
        }

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        window_start, window_end = self._creation_window(context)
        products: dict[str, dict[str, Any]] = {}
        page_offset = 0
        page_count = 100
        total_rows = 0
        invalid_create_date_rows = 0
        in_window_products = 0

        while True:
            params: dict[str, Any] = {"pageCount": page_count, "pageOffset": page_offset}
            try:
                product_list = self._request(
                    f"{self.base_url}/Products", params=params
                ).json()
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400 and page_offset > 0:
                    logger.warning(
                        "ProductsCreated: 400 at pageOffset=%d after %d rows / %d unique products. "
                        "Yielding partial results. Response: %s",
                        page_offset,
                        total_rows,
                        len(products),
                        exc.response.text[:500],
                    )
                    break
                raise

            if not isinstance(product_list, list) or not product_list:
                break

            total_rows += len(product_list)
            for product in product_list:
                product_number = str(product.get("productNumber") or "")
                if not product_number:
                    continue

                grouped_product = products.setdefault(
                    product_number,
                    {
                        "record": self._map_list_record(product, product_number),
                        "warehouse_stock": [],
                        "has_valid_create_date": False,
                        "is_in_window": False,
                    },
                )
                grouped_product["warehouse_stock"].append({
                    "warehouse": product.get("warehouse") or "",
                    "availableBalance": product.get("availableBalance") or 0,
                })

                created_at = self._parse_create_date(product.get("createDate"))
                if created_at is None:
                    invalid_create_date_rows += 1
                    continue

                grouped_product["has_valid_create_date"] = True
                if window_start <= created_at <= window_end:
                    if not grouped_product["is_in_window"]:
                        in_window_products += 1
                        grouped_product["record"] = self._map_list_record(
                            product, product_number
                        )
                    grouped_product["is_in_window"] = True

            if len(product_list) < page_count:
                break
            page_offset += 1

            if page_offset % 50 == 0:
                logger.info(
                    "ProductsCreated: page %d — %d rows scanned, %d unique products, "
                    "%d in-window products",
                    page_offset,
                    total_rows,
                    len(products),
                    in_window_products,
                )

        invalid_only_products = sum(
            1 for product in products.values() if not product["has_valid_create_date"]
        )
        valid_out_of_window_products = len(products) - in_window_products - invalid_only_products
        retained_products = in_window_products
        logger.info(
            "ProductsCreated: scan done — %d rows / %d unique products; retaining %d "
            "in-window products, skipping %d with no valid createDate and %d with "
            "valid out-of-window dates; %d invalid createDate rows; window=%s..%s",
            total_rows,
            len(products),
            retained_products,
            invalid_only_products,
            valid_out_of_window_products,
            invalid_create_date_rows,
            window_start.isoformat(),
            window_end.isoformat(),
        )

        enriched_products = 0
        for product_number, grouped_product in products.items():
            if not grouped_product["is_in_window"]:
                continue

            record = grouped_product["record"]
            record.update(self._get_product_detail_fields(product_number))
            record["warehouse_stock"] = json.dumps(
                grouped_product["warehouse_stock"]
            )
            enriched_products += 1
            if enriched_products % 250 == 0:
                logger.info(
                    "ProductsCreated: enriched %d / %d retained products",
                    enriched_products,
                    retained_products,
                )
            yield record


# ---------------------------------------------------------------------------
# ProductAvailabilityStream  —  GET /ProductAvailability
# ---------------------------------------------------------------------------


class ProductAvailabilityStream(ExtendStream):
    """Extend Commerce Product Availability (stock per product/warehouse).

    Separate endpoint from Products — provides availability including
    incoming stock and next receiving dates. One row per product/warehouse.

    Per Xavier's requirements (2026-03-06 email): stocks come from this
    endpoint, not from the Products list response.

    Incremental via modifiedDateFrom.
    Pagination: pageNumber (1-based).
    """

    name = "product_availability"
    primary_keys = ["productNumber", "warehouse"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        th.Property("productNumber", th.StringType),
        th.Property("warehouse", th.StringType),
        th.Property("warehouseName", th.StringType),
        th.Property("physicalBalance", th.NumberType),
        th.Property("availableBalanceNow", th.NumberType),
        th.Property("blockedBalance", th.NumberType),
        th.Property("orderedQuantity", th.NumberType),
        th.Property("nextReceivingDate", th.DateTimeType),
        th.Property("quantityOnNextReceiving", th.NumberType),
        th.Property("totalExpectedReceivingFromPurchase", th.NumberType),
        th.Property("totalExpectedReceivingFromWarehouseTransfer", th.NumberType),
        th.Property("totalExpectedReceivingFromReturn", th.NumberType),
        th.Property("changeDate", th.DateTimeType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        warehouse_codes = self._tap.warehouse_codes

        start_replication = self.get_starting_replication_key_value(context)
        params_base: dict[str, Any] = {}
        if start_replication:
            params_base["modifiedDateFrom"] = str(start_replication)
        elif self.config.get("start_date"):
            params_base["modifiedDateFrom"] = str(self.config["start_date"])

        page = 1
        while True:
            data = self._request(
                f"{self.base_url}/ProductAvailability",
                params={**params_base, "pageNumber": page},
            ).json()

            items = data.get("productAvailabilityList", [])
            pagination = data.get("paginationInfo", {})

            if not items:
                break

            for item in items:
                wh = item.get("warehouse") or ""
                if warehouse_codes and wh not in warehouse_codes:
                    continue

                yield {
                    "productNumber": str(item.get("productNumber") or ""),
                    "warehouse": wh,
                    "warehouseName": item.get("warehouseName"),
                    "physicalBalance": item.get("physicalBalance"),
                    "availableBalanceNow": item.get("availableBalanceNow"),
                    "blockedBalance": item.get("blockedBalance"),
                    "orderedQuantity": item.get("orderedQuantity"),
                    "nextReceivingDate": item.get("nextReceivingDate"),
                    "quantityOnNextReceiving": item.get("quantityOnNextReceiving"),
                    "totalExpectedReceivingFromPurchase": item.get("totalExpectedReceivingFromPurchase"),
                    "totalExpectedReceivingFromWarehouseTransfer": item.get("totalExpectedReceivingFromWarehouseTransfer"),
                    "totalExpectedReceivingFromReturn": item.get("totalExpectedReceivingFromReturn"),
                    "changeDate": item.get("changeDate"),
                }

            total_pages = pagination.get("totalPages", 0)
            if page >= total_pages:
                break
            page += 1


# ---------------------------------------------------------------------------
# CustomerOrdersStream  —  GET /CustomerOrders  +  GET /CustomerOrders/{id}
# ---------------------------------------------------------------------------


class CustomerOrdersStream(ExtendStream):
    """Extend Commerce CustomerOrders with full detail (header + rows).

    First run (no customer_orders bookmark): defers historical extraction to
    the reports_order_headers / reports_order_rows streams and seeds a bookmark
    for future incremental CustomerOrders syncs.

    Subsequent runs: uses CustomerOrders list with modifiedDateFrom/
    modifiedDateTo plus one detail call per order to fetch order rows.

    Rows are serialised as JSON strings using a report-row-compatible shape.

    Pagination: pageCount + pageOffset (NOT pageNumber).
    Replication key: changeDate (from CustomerOrderListItem).

    Schema from CustomerOrderListItem + CustomerOrderRow definitions.
    """

    name = "customer_orders"
    primary_keys = ["orderNumber"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # CustomerOrderListItem fields
        th.Property("orderNumber", th.StringType),
        th.Property("orderNumberExternal", th.StringType),
        th.Property("orderType", th.StringType),
        th.Property("orderStatus", th.StringType),
        th.Property("orderDate", th.DateTimeType),
        th.Property("askedDeliveryDate", th.DateTimeType),
        th.Property("slaDate", th.DateTimeType),
        th.Property("customerNumber", th.StringType),
        th.Property("customerName", th.StringType),
        th.Property("totalPrice", th.NumberType),
        th.Property("changeDate", th.DateTimeType),
        # Detail: CustomerOrderRow list as JSON
        # Key fields per row: position, orderRowStatus,
        #   product.productNumber, salesData.quantity, salesData.unitPrice,
        #   salesData.vatPercent, salesData.currency, warehouse
        th.Property("order_rows", th.StringType),
    ).to_dict()

    def _has_customer_orders_bookmark(self, context: Optional[dict]) -> bool:
        state = self.get_context_state(context)
        return (
            state.get("replication_key") == self.replication_key
            and state.get("replication_key_value") not in (None, "")
        )

    def get_replication_key_signpost(self, context: Optional[dict]) -> Optional[str]:
        """Freeze bookmark advancement to the tap-run upper bound."""
        return self._format_extend_datetime(self.sync_upper_bound)

    def finalize_state_progress_markers(self, state: Optional[dict] = None) -> None:
        """Persist the tap-run upper bound even when CustomerOrders is skipped."""
        if state in (None, {}):
            for context in self.partitions or [{}]:
                finalize_sdk_state_progress_markers(self.get_context_state(context or None))
            target_state = self.stream_state
        else:
            finalize_sdk_state_progress_markers(state)
            target_state = state
        self._advance_bookmark_to_signpost(target_state)

    def _advance_bookmark_to_signpost(self, state: Optional[dict] = None) -> None:
        """Set CustomerOrders bookmark to the stable tap-run upper bound."""
        target_state = state if state is not None else self.stream_state
        target_state["replication_key"] = self.replication_key
        target_state["replication_key_value"] = self.get_replication_key_signpost(None)
        target_state.pop("replication_key_signpost", None)
        target_state.pop("starting_replication_value", None)
        target_state.pop("progress_markers", None)

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        if not self._has_customer_orders_bookmark(context):
            logger.info(
                "CustomerOrders: no bookmark present; skipping endpoint sync so reports streams "
                "can serve as the historical source. Seeding bookmark for next run."
            )
            self._advance_bookmark_to_signpost(self.get_context_state(context))
            return

        start_replication = self.get_starting_replication_key_value(context)
        modified_date_from = self._format_extend_datetime(start_replication)

        page_offset = 0
        page_count = 100
        total_orders = 0

        while True:
            params: dict[str, Any] = {
                "pageCount": page_count,
                "pageOffset": page_offset,
                "modifiedDateFrom": modified_date_from,
            }

            logger.info(
                "Requesting CustomerOrders pageOffset=%d modifiedDateFrom=%s",
                page_offset,
                params["modifiedDateFrom"],
            )
            try:
                order_list = self._request(
                    f"{self.base_url}/CustomerOrders",
                    params=params,
                ).json()
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400 and page_offset > 0:
                    logger.warning(
                        "CustomerOrders: 400 at pageOffset=%d after %d orders. "
                        "Yielding partial results. Response: %s",
                        page_offset, total_orders,
                        exc.response.text[:500],
                    )
                    return
                raise

            if not isinstance(order_list, list) or not order_list:
                break

            total_orders += len(order_list)

            page_size = len(order_list)
            page_detail_started_at = time.monotonic()
            customer_orders_bucket = self._rate_limit_bucket(f"{self.base_url}/CustomerOrders")
            for index, o in enumerate(order_list, start=1):
                order_number = str(o.get("orderNumber") or "")
                if not order_number:
                    continue

                if index == 1 or index % 10 == 0 or index == page_size:
                    elapsed_seconds = max(time.monotonic() - page_detail_started_at, 0.001)
                    started_per_second = index / elapsed_seconds
                    logger.info(
                        "CustomerOrders pageOffset=%d fetching order details %d/%d "
                        "(total listed=%d) orderNumber=%s avg_start_rate=%.2f/s "
                        "throttle_limit=%.2f/s bucket=%s",
                        page_offset,
                        index,
                        page_size,
                        total_orders,
                        order_number,
                        started_per_second,
                        self._bucket_requests_per_second(customer_orders_bucket),
                        customer_orders_bucket,
                    )

                yield {
                    "orderNumber": order_number,
                    "orderNumberExternal": o.get("orderNumberExternal"),
                    "orderType": o.get("orderType"),
                    "orderStatus": o.get("orderStatus"),
                    "orderDate": o.get("orderDate"),
                    "askedDeliveryDate": o.get("askedDeliveryDate"),
                    "slaDate": o.get("slaDate"),
                    "customerNumber": str(o.get("customerNumber") or ""),
                    "customerName": o.get("customerName"),
                    "totalPrice": o.get("totalPrice"),
                    "changeDate": o.get("changeDate"),
                    "order_rows": json.dumps(self._fetch_order_rows(order_number)),
                }

            if len(order_list) < page_count:
                break
            page_offset += 1

            if page_offset % 50 == 0:
                logger.info(
                    "CustomerOrders: page %d — %d orders fetched so far",
                    page_offset, total_orders,
                )

        logger.info("CustomerOrders: done — %d orders", total_orders)
        self._advance_bookmark_to_signpost(self.get_context_state(context))

    def _fetch_order_rows(self, order_number: str) -> list:
        """Fetch orderRows from GET /CustomerOrders/{id}.

        Response: {orderHeader: {...}, orderRows: [...]}.
        Key fields per CustomerOrderRow:
          position, orderRowStatus, supplyMode, warehouse,
          product.productNumber, product.productName,
          salesData.quantity, salesData.unitPrice, salesData.vatPercent,
          salesData.currency, expectedDeliveryDate, changeDate.
        """
        try:
            detail = self._request(f"{self.base_url}/CustomerOrders/{order_number}").json()
            rows = detail.get("orderRows", [])
            if not isinstance(rows, list):
                return []
            return [
                self._normalize_customer_order_row_from_detail(order_number, row)
                for row in rows
            ]
        except Exception:
            logger.warning("Failed to fetch rows for order %s", order_number, exc_info=True)
            return []

    @staticmethod
    def _normalize_customer_order_row_from_detail(
        order_number: str,
        row: dict[str, Any],
    ) -> dict[str, Any]:
        product = row.get("product") or {}
        sales_data = row.get("salesData") or {}
        return {
            "orderRowId": row.get("orderRowId"),
            "position": row.get("position"),
            "subPosition": row.get("subPosition"),
            "supplyMode": row.get("supplyMode"),
            "productNumber": product.get("productNumber"),
            "productName": product.get("productName"),
            "orderQuantity": sales_data.get("quantity"),
            "price": sales_data.get("unitPrice"),
            "vatPercent": sales_data.get("vatPercent"),
            "currencyId": sales_data.get("currency"),
            "expectedDeliveryDate": row.get("expectedDeliveryDate"),
            "shipDate": row.get("shipDate"),
            "orderRowStatus": row.get("orderRowStatus"),
            "shipmentNumber": row.get("shipmentNumber"),
            "warehouseShortName": row.get("warehouse"),
            "orderNumber": order_number,
            "salesUnit": sales_data.get("unit"),
            "salesUnitQuantity": sales_data.get("quantity"),
            "productSalesUnitPrice": sales_data.get("unitPrice"),
            "allocationStatus": row.get("allocationStatus"),
            "changeDate": row.get("changeDate"),
        }


# ---------------------------------------------------------------------------
# PurchaseOrdersStream  —  GET /PurchaseOrders  +  GET /PurchaseOrders/{id}
# ---------------------------------------------------------------------------


class PurchaseOrdersStream(ExtendStream):
    """Extend Commerce PurchaseOrders with full detail.

    List endpoint returns PurchaseOrderListItem summaries.
    Detail endpoint is called per PO for header (incl. supplierAgreementNumber),
    rows, and shipments.

    Incremental via changeDateFrom/changeDateTo.
    Pagination: pageNumber (1-based).
    Replication key: changeDate (from PurchaseOrderListItem).

    Schema from PurchaseOrderListItem + PurchaseOrderSupplier +
    PurchaseOrderRow definitions.
    """

    name = "purchase_orders"
    primary_keys = ["purchaseNumber"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    def get_change_date_to_for_query(self) -> str:
        """Return PurchaseOrders changeDateTo query upper bound.

        Extend reported that PurchaseOrders change-date filtering currently
        ignores the time of day. Until Extend fixes that behavior, query
        through the current tap-run timestamp plus one day so same-day changes
        are included.
        """
        use_tomorrow = self.config.get("purchase_orders_change_date_to_tomorrow", True)
        if not use_tomorrow:
            return self._format_extend_datetime(self.sync_upper_bound)

        query_upper_bound = datetime.fromisoformat(self.sync_upper_bound) + timedelta(days=1)
        return self._format_extend_datetime(query_upper_bound)

    schema = th.PropertiesList(
        # PurchaseOrderListItem fields
        th.Property("purchaseNumber", th.StringType),
        th.Property("status", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("warehouse", th.StringType),
        th.Property("isOpen", th.BooleanType),
        th.Property("isReceived", th.BooleanType),
        th.Property("externalOrderNumber", th.StringType),
        th.Property("supplierOrderNumber", th.StringType),
        th.Property("shippedDate", th.DateTimeType),
        th.Property("changeDate", th.DateTimeType),
        # PurchaseOrderSupplier fields (from detail header)
        th.Property("supplierNumber", th.StringType),
        th.Property("supplierName", th.StringType),
        th.Property("supplierAgreementNumber", th.IntegerType),  # FK to SupplierAgreement
        th.Property("supplierAgreement", th.StringType),
        th.Property("paymentTerms", th.StringType),
        th.Property("deliveryMethod", th.StringType),
        th.Property("deliveryMethodName", th.StringType),
        th.Property("transportCondition", th.StringType),
        th.Property("forwarder", th.StringType),
        # Header fields
        th.Property("reference", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("requestedDeliveryDate", th.DateTimeType),
        # Delivery address (flattened)
        th.Property("deliveryAddress_name1", th.StringType),
        th.Property("deliveryAddress_name2", th.StringType),
        th.Property("deliveryAddress_address1", th.StringType),
        th.Property("deliveryAddress_postalCode", th.StringType),
        th.Property("deliveryAddress_city", th.StringType),
        th.Property("deliveryAddress_countryCode", th.StringType),
        # Buyer contact (flattened)
        th.Property("buyerContactName", th.StringType),
        th.Property("buyerContactEmail", th.StringType),
        # PurchaseOrderRow list as JSON
        # Key fields per row: position, rowStatus, productNumber, productName,
        #   supplierProductNumber, purchaseDataProductUnit.quantity,
        #   purchaseDataProductUnit.unitPrice, purchaseDataProductUnit.vatPercent,
        #   purchaseDataProductUnit.currency, expectedDeliveryDate
        th.Property("rows", th.StringType),
        # PurchaseOrderShipment list as JSON (populated when isReceived=True)
        th.Property("shipments", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        warehouse_codes = self._tap.warehouse_codes
        start_replication = self.get_starting_replication_key_value(context)

        params_base: dict[str, Any] = {}
        if start_replication:
            params_base["changeDateFrom"] = self._format_extend_datetime(start_replication)
        elif self.config.get("start_date"):
            params_base["changeDateFrom"] = self._format_extend_datetime(self.config["start_date"])
        else:
            params_base["changeDateFrom"] = self._format_extend_datetime(self.sync_upper_bound)
        params_base["changeDateTo"] = self.get_change_date_to_for_query()

        page = 1
        total_emitted = 0
        total_detail_fallbacks = 0
        while True:
            page_started_at = time.monotonic()
            logger.info(
                "Requesting PurchaseOrders page %d: changeDateFrom=%s changeDateTo=%s",
                page,
                params_base["changeDateFrom"],
                params_base["changeDateTo"],
            )
            response = self._request(
                f"{self.base_url}/PurchaseOrders",
                params={**params_base, "pageNumber": page},
            )
            data = response.json()

            po_list = data.get("purchaseOrderList", [])
            pagination = data.get("paginationInfo", {})
            current_page = int(pagination.get("currentPage") or page)
            total_pages = int(pagination.get("totalPages") or 0)
            logger.info(
                "PurchaseOrders page %d/%d returned %d list records in %.1fs remaining_requests=%s",
                current_page,
                total_pages,
                len(po_list),
                time.monotonic() - page_started_at,
                self._remaining_requests_label(response),
            )

            if not po_list:
                break

            for po in po_list:
                purchase_number = po.get("purchaseNumber")
                if not purchase_number:
                    continue
                if warehouse_codes and po.get("warehouse") not in warehouse_codes:
                    continue

                logger.info("Requesting PurchaseOrder details: %s", purchase_number)
                detail = self._fetch_detail(purchase_number)
                if detail:
                    total_emitted += 1
                    yield self._map_detail(detail, po)
                else:
                    total_emitted += 1
                    total_detail_fallbacks += 1
                    yield self._map_summary(po)

            if page >= total_pages:
                break
            page += 1

        logger.info(
            "PurchaseOrders done: emitted %d records (%d summary-only fallbacks)",
            total_emitted,
            total_detail_fallbacks,
        )

    def _fetch_detail(self, purchase_number: str) -> Optional[dict]:
        try:
            return self._request(f"{self.base_url}/PurchaseOrders/{purchase_number}").json()
        except requests.exceptions.HTTPError as exc:
            response = exc.response
            message = (response.text or "").lower() if response is not None else ""
            if response is not None and response.status_code == 400 and (
                "there is no row at position 0" in message
            ):
                logger.info(
                    "Purchase order detail unavailable for %s; falling back to summary only.",
                    purchase_number,
                )
                return None
            logger.warning("Failed to fetch detail for PO %s", purchase_number, exc_info=True)
            return None
        except Exception:
            logger.warning("Failed to fetch detail for PO %s", purchase_number, exc_info=True)
            return None

    def _map_detail(self, detail: dict, summary: dict) -> dict:
        header = detail.get("header", {}) or {}
        rows = detail.get("rows", [])
        shipments = detail.get("shipments", [])
        supplier = header.get("supplier", {}) or {}
        delivery_addr = header.get("deliveryAddress", {}) or {}
        buyer = header.get("buyerContact", {}) or {}

        for row in rows:
            if not isinstance(row, dict):
                continue
            row["expectedDeliveryDate"] = _strip_timezone_offset(row.get("expectedDeliveryDate"))
            row["statusChangeDate"] = _strip_timezone_offset(row.get("statusChangeDate"))

        return {
            "purchaseNumber": header.get("purchaseNumber") or summary.get("purchaseNumber"),
            "status": header.get("status") or summary.get("status"),
            "createDate": _strip_timezone_offset(header.get("createDate") or summary.get("createDate")),
            "warehouse": header.get("warehouse") or summary.get("warehouse"),
            "isOpen": summary.get("isOpen", True),
            "isReceived": summary.get("isReceived", False),
            "externalOrderNumber": header.get("externalOrderNumber") or summary.get("externalOrderNumber", ""),
            "supplierOrderNumber": header.get("supplierOrderNumber") or summary.get("supplierOrderNumber", ""),
            "shippedDate": header.get("shippedDate") or summary.get("shippedDate"),
            "changeDate": _strip_timezone_offset(header.get("changeDate") or summary.get("changeDate")),
            # PurchaseOrderSupplier
            "supplierNumber": supplier.get("supplierNumber") or summary.get("supplierNumber"),
            "supplierName": supplier.get("supplierName") or summary.get("supplierName"),
            "supplierAgreementNumber": supplier.get("supplierAgreementNumber"),
            "supplierAgreement": supplier.get("supplierAgreement"),
            "paymentTerms": supplier.get("paymentTerms"),
            "deliveryMethod": supplier.get("deliveryMethod"),
            "deliveryMethodName": supplier.get("deliveryMethodName"),
            "transportCondition": supplier.get("transportCondition"),
            "forwarder": supplier.get("forwarder"),
            # Header
            "reference": header.get("reference", ""),
            "notes": header.get("notes", ""),
            "requestedDeliveryDate": header.get("requestedDeliveryDate"),
            # Delivery address
            "deliveryAddress_name1": delivery_addr.get("name1"),
            "deliveryAddress_name2": delivery_addr.get("name2"),
            "deliveryAddress_address1": delivery_addr.get("address1"),
            "deliveryAddress_postalCode": delivery_addr.get("postalCode"),
            "deliveryAddress_city": delivery_addr.get("city"),
            "deliveryAddress_countryCode": delivery_addr.get("countryCode"),
            # Buyer contact
            "buyerContactName": buyer.get("name"),
            "buyerContactEmail": buyer.get("email"),
            # Nested
            "rows": json.dumps(rows),
            "shipments": json.dumps(shipments),
        }

    def _map_summary(self, summary: dict) -> dict:
        """Fallback when detail fetch fails — summary fields only."""
        return {
            "purchaseNumber": summary.get("purchaseNumber"),
            "status": summary.get("status"),
            "createDate": _strip_timezone_offset(summary.get("createDate")),
            "warehouse": summary.get("warehouse"),
            "isOpen": summary.get("isOpen", True),
            "isReceived": summary.get("isReceived", False),
            "externalOrderNumber": summary.get("externalOrderNumber", ""),
            "supplierOrderNumber": summary.get("supplierOrderNumber", ""),
            "shippedDate": summary.get("shippedDate"),
            "changeDate": _strip_timezone_offset(summary.get("changeDate")),
            "supplierNumber": summary.get("supplierNumber"),
            "supplierName": summary.get("supplierName"),
            "supplierAgreementNumber": None,
            "supplierAgreement": None,
            "paymentTerms": None,
            "deliveryMethod": None,
            "deliveryMethodName": None,
            "transportCondition": None,
            "forwarder": None,
            "reference": None,
            "notes": None,
            "requestedDeliveryDate": None,
            "deliveryAddress_name1": None,
            "deliveryAddress_name2": None,
            "deliveryAddress_address1": None,
            "deliveryAddress_postalCode": None,
            "deliveryAddress_city": None,
            "deliveryAddress_countryCode": None,
            "buyerContactName": None,
            "buyerContactEmail": None,
            "rows": "[]",
            "shipments": "[]",
        }


# ---------------------------------------------------------------------------
# Shared helper: day-by-day paginator for Reports endpoints
# ---------------------------------------------------------------------------


def _iter_report_days(
    stream: "ExtendStream",
    url: str,
    list_key: str,
    start_date: str,
    end_date: Optional[str] = None,
) -> Iterable[dict]:
    """Iterate a Reports endpoint one day at a time, paginating each day.

    Reports endpoints use inclusive day windows:
    changeDate=YYYY-MM-DDT00:00:00 and toChangeDate=YYYY-MM-DDT23:59:59.
    Pagination uses pageNumber (1-based); stop when paginationInfo.currentPage
    equals paginationInfo.totalPages.

    Args:
        stream:     ExtendStream instance (for _request and logging).
        url:        Full endpoint URL.
        list_key:   Key in the JSON response that holds the list of records.
        start_date: ISO date string "YYYY-MM-DD" to start from.
        end_date:   ISO date string "YYYY-MM-DD" upper bound (inclusive).
                    Defaults to today (UTC).
    """
    current = datetime.strptime(start_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_date:
        stop = datetime.strptime(end_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        stop = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    total_days = (stop - current).days + 1
    day_num = 0

    while current <= stop:
        date_str = current.strftime("%Y-%m-%d")
        day_num += 1
        page = 1
        day_records = 0

        while True:
            change_date_from = f"{date_str}T00:00:00"
            change_date_to = f"{date_str}T23:59:59"
            params = {
                "pageNumber": page,
                "changeDate": change_date_from,
                "toChangeDate": change_date_to,
            }
            try:
                page_started_at = time.monotonic()
                logger.info(
                    "Requesting Reports %s day %d/%d (%s) page %d: %s to %s",
                    list_key,
                    day_num,
                    total_days,
                    date_str,
                    page,
                    change_date_from,
                    change_date_to,
                )
                response = stream._request(url, params=params)
                data = response.json()
            except requests.exceptions.HTTPError as exc:
                response = exc.response
                if response is None or response.status_code != 400 or page == 1:
                    raise

                probe = stream._request(url, params={
                    "pageNumber": 1,
                    "changeDate": change_date_from,
                    "toChangeDate": change_date_to,
                }).json()
                total_pages = int(probe.get("paginationInfo", {}).get("totalPages") or 1)
                if page > total_pages:
                    logger.warning(
                        "Reports endpoint rejected page %s for %s; current totalPages is %s. "
                        "Treating this as end-of-day pagination drift.",
                        page,
                        date_str,
                        total_pages,
                    )
                    break
                raise

            items = data.get(list_key, [])
            pagination = data.get("paginationInfo", {})
            day_records += len(items)
            current_page = int(pagination.get("currentPage") or page)
            total_pages = int(pagination.get("totalPages") or 1)
            logger.info(
                "Reports %s day %d/%d (%s) page %d/%d returned %d records in %.1fs remaining_requests=%s",
                list_key,
                day_num,
                total_days,
                date_str,
                current_page,
                total_pages,
                len(items),
                time.monotonic() - page_started_at,
                _remaining_requests_label(response),
            )

            for item in items:
                if not item.get("changeDate"):
                    item["changeDate"] = date_str + "T00:00:00+00:00"
                yield item

            if not total_pages or current_page >= total_pages:
                break
            page += 1

        logger.info(
            "Reports %s day %d/%d (%s): %d records across %d pages",
            list_key, day_num, total_days, date_str, day_records, page,
        )
        current += timedelta(days=1)


def _log_unmapped_report_fields(
    stream_name: str,
    item: dict,
    field_defs: list[tuple[str, Any]],
) -> None:
    """Log report API fields missing from the tap schema/mapping once."""
    mapped_fields = {field_name for field_name, _field_type in field_defs}
    unmapped_fields = tuple(sorted(set(item) - mapped_fields))
    if not unmapped_fields:
        return

    log_key = (stream_name, unmapped_fields)
    if log_key in _LOGGED_UNMAPPED_REPORT_FIELDS:
        return

    _LOGGED_UNMAPPED_REPORT_FIELDS.add(log_key)
    logger.warning(
        "Reports API returned fields not mapped in '%s': %s",
        stream_name,
        ", ".join(unmapped_fields),
    )


def _report_state_date_range(stream: "ExtendStream") -> tuple[Optional[str], Optional[str]]:
    """Return report-only date range overrides from Singer state, if present.

    Supported state locations, in precedence order:
      1. top-level reports_start_date / reports_end_date
      2. bookmarks.<stream_name>.reports_start_date / reports_end_date
         (kept as a developer/backward-compatible escape hatch)

    Dates are normalized to YYYY-MM-DD because Reports endpoints are
    day-window based.
    """
    tap_state = getattr(stream, "tap_state", None)
    if tap_state is None:
        tap_state = getattr(getattr(stream, "_tap", None), "state", {}) or {}
    if not isinstance(tap_state, dict):
        return None, None

    candidates = [tap_state]

    bookmarks = tap_state.get("bookmarks", {})
    if isinstance(bookmarks, dict):
        stream_bookmark = bookmarks.get(stream.name)
        if isinstance(stream_bookmark, dict):
            candidates.append(stream_bookmark)

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue

        start_date = candidate.get("reports_start_date")
        end_date = candidate.get("reports_end_date")
        if start_date or end_date:
            return (
                str(start_date)[:10] if start_date else None,
                str(end_date)[:10] if end_date else None,
            )

    return None, None


def _stream_has_bookmark(
    stream: "ExtendStream",
    stream_name: str,
    replication_key: Optional[str] = None,
) -> bool:
    """Return True when the given stream has a persisted replication bookmark."""
    tap = getattr(stream, "_tap", None)
    tap_state = getattr(tap, "_loaded_state", None)
    if tap_state is None:
        tap_state = getattr(stream, "tap_state", None)
    if tap_state is None:
        tap_state = getattr(tap, "state", {}) or {}
    if not isinstance(tap_state, dict):
        return False

    bookmarks = tap_state.get("bookmarks", {})
    if not isinstance(bookmarks, dict):
        return False

    stream_state = bookmarks.get(stream_name)
    if not isinstance(stream_state, dict):
        return False

    if replication_key and stream_state.get("replication_key") != replication_key:
        return False

    return stream_state.get("replication_key_value") not in (None, "")


def _sanitize_signpost_bookmark_state(
    stream_state: dict[str, Any],
    replication_key: str,
) -> dict[str, Any]:
    """Return a clean persisted bookmark for signpost-based streams."""
    normalized = dict(stream_state)

    bookmark_value = normalized.get("replication_key_value")
    progress_markers = normalized.get("progress_markers")
    if normalized.get("replication_key_signpost") not in (None, ""):
        bookmark_value = normalized.get("replication_key_signpost")
    elif isinstance(progress_markers, dict) and progress_markers.get("replication_key") == replication_key:
        progress_value = progress_markers.get("replication_key_value")
        if progress_value not in (None, ""):
            bookmark_value = progress_value

    if bookmark_value not in (None, ""):
        normalized["replication_key"] = replication_key
        normalized["replication_key_value"] = bookmark_value

    normalized.pop("replication_key_signpost", None)
    normalized.pop("starting_replication_value", None)
    normalized.pop("progress_markers", None)
    normalized.pop("partitions", None)
    return normalized


# ---------------------------------------------------------------------------
# ReportsOrderHeadersStream  —  GET /reports/{client}/OrderHeaders
# ---------------------------------------------------------------------------


_REPORTS_ORDER_HEADER_FIELDS = [
    ("orderNumber", th.StringType),
    ("orderNumberExternal", th.StringType),
    ("orderNumberEndCustomer", th.StringType),
    ("orderDate", th.DateTimeType),
    ("changeDate", th.DateTimeType),
    ("askedDeliveryDate", th.DateTimeType),
    ("slaDate", th.DateTimeType),
    ("orderType", th.StringType),
    ("orderStatus", th.StringType),
    ("orderMethod", th.StringType),
    ("customerNumber", th.StringType),
    ("orderReference", th.StringType),
    ("acceptPartialDelivery", th.BooleanType),
    ("invoiceEmail", th.StringType),
    ("orderConfirmationEmail", th.StringType),
    ("deliveryAdviceEmail", th.StringType),
    ("phoneNumber2", th.StringType),
    ("phoneNumber", th.StringType),
    ("shippingMark", th.StringType),
    ("notes", th.StringType),
    ("orderPriority", th.IntegerType),
    ("orderReference2", th.StringType),
    ("deliveryDayLeadTimeOverride", th.BooleanType),
    ("requestedForwarder", th.StringType),
    ("requestedTransportMode", th.StringType),
    ("palletRegistrationNumber", th.StringType),
    ("transportCondition", th.StringType),
    ("handlingMark", th.StringType),
    ("orderPaymentStatus", th.StringType),
    ("freightFree", th.BooleanType),
    ("latitude", th.StringType),
    ("longitude", th.StringType),
    ("invoiceOnlyAtFullOrderDelivery", th.BooleanType),
    ("consolidateDelivery", th.BooleanType),
    ("deliveryDescription1", th.StringType),
    ("deliveryDescription2", th.StringType),
    ("deliveryDescription3", th.StringType),
    ("deliveryDescription4", th.StringType),
    ("consignmentInventoryHandling", th.StringType),
    ("arcNumber", th.StringType),
    ("departureId", th.StringType),
    ("routeId", th.StringType),
    ("orderReviewReasonNotes", th.StringType),
    ("doorCode", th.StringType),
    ("salesChannel", th.StringType),
    ("createdBy", th.StringType),
    ("lastmodifiedBy", th.StringType),
    ("paymentType", th.StringType),
    ("termsOfPayment", th.StringType),
    ("isProductSample", th.BooleanType),
    ("customerGLN", th.StringType),
    ("clientSalesChannel", th.StringType),
    ("salesMan", th.StringType),
    ("customerFinancialCategory", th.StringType),
    ("isAgreedOrderForCompanyGroup", th.BooleanType),
    ("isAgreedOrderFixedPriceAgreement", th.BooleanType),
    ("isAgreedOrderCustomerOwned", th.BooleanType),
    ("anonymized", th.BooleanType),
    ("requirePrepayment", th.BooleanType),
    ("orderDeliveryUpdateEmailType", th.StringType),
    ("deliveryName1", th.StringType),
    ("deliveryName2", th.StringType),
    ("deliveryAddress1", th.StringType),
    ("deliveryAddress2", th.StringType),
    ("deliveryAddress3", th.StringType),
    ("deliveryPostalCode", th.StringType),
    ("deliveryCity", th.StringType),
    ("deliveryState", th.StringType),
    ("deliveryCountryId", th.StringType),
    ("invoiceName", th.StringType),
    ("invoiceAddress1", th.StringType),
    ("invoiceAddress2", th.StringType),
    ("invoiceAddress3", th.StringType),
    ("invoicePostalCode", th.StringType),
    ("invoiceCity", th.StringType),
    ("invoiceState", th.StringType),
    ("invoiceCountryId", th.StringType),
]


class ReportsOrderHeadersStream(ExtendStream):
    """Extend Commerce order headers from the Reports API.

    Iterates day by day from start_date to today, requesting the full-day
    window (00:00:00 through 23:59:59) and paginating all pages within each day
    before advancing.

    Historical source only for sell orders. Once customer_orders has its own
    bookmark, this stream stops emitting so incremental sell-order extraction
    comes exclusively from CustomerOrders + CustomerOrders/{orderNumber}.

    Pagination: pageNumber (1-based), stop when currentPage == totalPages.
    Replication key: changeDate.
    """

    name = "reports_order_headers"
    primary_keys = ["orderNumber"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        *(
            th.Property(field_name, field_type)
            for field_name, field_type in _REPORTS_ORDER_HEADER_FIELDS
        )
    ).to_dict()

    @property
    def _reports_url(self) -> str:
        api_url = self.config.get("api_url", "https://s05.extend.se/RESTAPI").rstrip("/")
        return f"{api_url}/reports/{self.config['client']}/OrderHeaders"

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        if _stream_has_bookmark(self, "customer_orders", replication_key="changeDate"):
            logger.info(
                "Skipping reports_order_headers because customer_orders bookmark exists; "
                "CustomerOrders is now the active sell-order incremental source."
            )
            return

        reports_start_date, reports_end_date = _report_state_date_range(self)
        start_replication = self.get_starting_replication_key_value(context)
        if reports_start_date:
            start_date = reports_start_date
        elif start_replication:
            start_date = str(start_replication)[:10]
        elif self.config.get("start_date"):
            start_date = str(self.config["start_date"])[:10]
        else:
            start_date = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")

        for item in _iter_report_days(
            self,
            self._reports_url,
            "orderHeaderList",
            start_date,
            reports_end_date or self.sync_upper_bound_date,
        ):
            _log_unmapped_report_fields(self.name, item, _REPORTS_ORDER_HEADER_FIELDS)
            record = {
                field_name: item.get(field_name)
                for field_name, _field_type in _REPORTS_ORDER_HEADER_FIELDS
            }
            record["customerNumber"] = str(record.get("customerNumber") or "")
            if record.get("orderPaymentStatus") is not None:
                record["orderPaymentStatus"] = str(record["orderPaymentStatus"])
            yield record


# ---------------------------------------------------------------------------
# ReportsOrderRowsStream  —  GET /reports/{client}/OrderRows
# ---------------------------------------------------------------------------


_REPORTS_ORDER_ROW_FIELDS = [
    ("orderRowId", th.StringType),
    ("position", th.IntegerType),
    ("subPosition", th.IntegerType),
    ("supplyMode", th.StringType),
    ("productNumber", th.StringType),
    ("productName", th.StringType),
    ("productUnitName", th.StringType),
    ("orderQuantity", th.NumberType),
    ("price", th.NumberType),
    ("vatPercent", th.NumberType),
    ("currencyId", th.StringType),
    ("currencyExchangeRate", th.NumberType),
    ("expectedDeliveryDate", th.DateTimeType),
    ("shipDate", th.DateTimeType),
    ("backOrderHandling", th.StringType),
    ("notes", th.StringType),
    ("orderRowStatus", th.StringType),
    ("shipmentNumber", th.StringType),
    ("warehouseShortName", th.StringType),
    ("orderNumber", th.StringType),
    ("productNotes", th.StringType),
    ("handlingMark", th.StringType),
    ("shippingMark", th.StringType),
    ("batchNumber", th.StringType),
    ("salesUnit", th.StringType),
    ("salesUnitQuantity", th.NumberType),
    ("agreedOrderPickTime", th.DateTimeType),
    ("pickingDelayReasonId", th.StringType),
    ("listPrice", th.NumberType),
    ("ordinalPrice", th.NumberType),
    ("promotion", th.StringType),
    ("isResultOfPromotion", th.BooleanType),
    ("deliveredQuantity", th.NumberType),
    ("waitReservation", th.BooleanType),
    ("requestedBatchNo", th.StringType),
    ("productSalesUnitPrice", th.NumberType),
    ("productVisibility", th.StringType),
    ("numberOfOrdersCreatedFromSubscription", th.IntegerType),
    ("maxNumberOfOrdersToCreateFromSubscription", th.IntegerType),
    ("explicitCost", th.NumberType),
    ("explicitCostCurrency", th.StringType),
    ("originalExpectedDeliveryDate", th.DateTimeType),
    ("project", th.StringType),
    ("orderReasonCode", th.StringType),
    ("structuredCost", th.NumberType),
    ("exciseDutyCost", th.NumberType),
    ("customerBonusCost", th.NumberType),
    ("cost", th.NumberType),
    ("parentOrderRowPosition", th.IntegerType),
    ("agreeedOrderRowId", th.StringType),
    ("orderDate", th.DateTimeType),
    ("orderPriority", th.IntegerType),
    ("getBalanceFromAgreedOrder", th.BooleanType),
    ("gtin", th.StringType),
    ("allocationStatus", th.StringType),
    ("releaseToWarehouseWhenAllocated", th.BooleanType),
    ("pickDate", th.DateTimeType),
    ("changeDate", th.DateTimeType),
]


class ReportsOrderRowsStream(ExtendStream):
    """Extend Commerce order rows from the Reports API.

    Iterates day by day from start_date to today, requesting the full-day
    window (00:00:00 through 23:59:59) and paginating all pages within each day
    before advancing.

    Historical source only for sell orders. Once customer_orders has its own
    bookmark, this stream stops emitting so incremental sell-order extraction
    comes exclusively from CustomerOrders + CustomerOrders/{orderNumber}.

    Pagination: pageNumber (1-based), stop when currentPage == totalPages.
    Replication key: changeDate.
    """

    name = "reports_order_rows"
    primary_keys = ["orderRowId"]
    replication_key = "changeDate"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        *(
            th.Property(field_name, field_type)
            for field_name, field_type in _REPORTS_ORDER_ROW_FIELDS
        )
    ).to_dict()

    @property
    def _reports_url(self) -> str:
        api_url = self.config.get("api_url", "https://s05.extend.se/RESTAPI").rstrip("/")
        return f"{api_url}/reports/{self.config['client']}/OrderRows"

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        if _stream_has_bookmark(self, "customer_orders", replication_key="changeDate"):
            logger.info(
                "Skipping reports_order_rows because customer_orders bookmark exists; "
                "CustomerOrders is now the active sell-order incremental source."
            )
            return

        reports_start_date, reports_end_date = _report_state_date_range(self)
        start_replication = self.get_starting_replication_key_value(context)
        if reports_start_date:
            start_date = reports_start_date
        elif start_replication:
            start_date = str(start_replication)[:10]
        elif self.config.get("start_date"):
            start_date = str(self.config["start_date"])[:10]
        else:
            start_date = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")

        for item in _iter_report_days(
            self,
            self._reports_url,
            "orderRowList",
            start_date,
            reports_end_date or self.sync_upper_bound_date,
        ):
            _log_unmapped_report_fields(self.name, item, _REPORTS_ORDER_ROW_FIELDS)
            yield {
                field_name: item.get(field_name)
                for field_name, _field_type in _REPORTS_ORDER_ROW_FIELDS
            }
