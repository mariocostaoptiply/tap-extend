import importlib
import json
import sys
import types

import pytest


def install_hotglue_sdk_stubs():
    """Provide the tiny SDK surface needed to import stream definitions."""
    sdk = types.ModuleType("hotglue_singer_sdk")
    typing = types.ModuleType("hotglue_singer_sdk.typing")
    streams = types.ModuleType("hotglue_singer_sdk.streams")

    class Property:
        def __init__(self, name, type_, **kwargs):
            self.name = name
            self.type_ = type_
            self.kwargs = kwargs

    class PropertiesList:
        def __init__(self, *properties):
            self.properties = properties

        def to_dict(self):
            return {
                "type": "object",
                "properties": {prop.name: {} for prop in self.properties},
            }

    class Stream:
        def __init__(self, tap=None):
            self._tap = tap
            self.config = getattr(tap, "config", {}) if tap is not None else {}
            self.logger = getattr(tap, "logger", None)
            self.tap_state = getattr(tap, "state", {}) if tap is not None else {}
            self.stream_state = self.tap_state.setdefault("bookmarks", {}).setdefault(
                getattr(self, "name", self.__class__.__name__),
                {},
            )
            self.partitions = None

        def get_starting_replication_key_value(self, context):
            return self.stream_state.get("replication_key_value")

        def get_context_state(self, context):
            return self.stream_state

    for name in [
        "StringType",
        "IntegerType",
        "NumberType",
        "BooleanType",
        "DateTimeType",
    ]:
        setattr(typing, name, type(name, (), {}))

    setattr(typing, "Property", Property)
    setattr(typing, "PropertiesList", PropertiesList)
    setattr(streams, "Stream", Stream)
    setattr(sdk, "typing", typing)

    sys.modules["hotglue_singer_sdk"] = sdk
    sys.modules["hotglue_singer_sdk.typing"] = typing
    sys.modules["hotglue_singer_sdk.streams"] = streams


install_hotglue_sdk_stubs()
stream_module = importlib.import_module("tap_extend.streams")


@pytest.fixture(autouse=True)
def reset_rate_limit_state():
    stream_module.ExtendStream._next_request_at = 0.0
    stream_module.ExtendStream._rate_limit_next_request_at = {}
    stream_module.ExtendStream._rate_limit_requests_per_second = {}
    stream_module.ExtendStream._rate_limit_ignored_higher_logged = set()


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeRequest:
    method = "GET"
    url = "https://example.test/resource"


class FakeHTTPResponse:
    def __init__(self, status_code, text="", headers=None, url=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.request = FakeRequest()
        if url is not None:
            self.request = types.SimpleNamespace(method="GET", url=url)
        self.url = self.request.url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise stream_module.requests.exceptions.HTTPError(response=self)


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.responses.pop(0)


class FakeReportStream:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def _request(self, url, params=None):
        self.requests.append({"url": url, "params": dict(params or {})})
        return FakeResponse(self.responses.pop(0))


DEADLOCK_BODY = (
    '{"Message": "Error getting order row list: Transaction (Process ID 232) '
    "was deadlocked on lock resources with another process and has been chosen "
    'as the deadlock victim. Rerun the transaction."}'
)


def install_fake_clock(monkeypatch):
    clock = {"monotonic": 0.0, "epoch": 1_777_454_000.0, "sleeps": []}

    def sleep(seconds):
        clock["sleeps"].append(seconds)
        clock["monotonic"] += seconds
        clock["epoch"] += seconds

    monkeypatch.setattr(stream_module.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(stream_module.time, "time", lambda: clock["epoch"])
    monkeypatch.setattr(stream_module.time, "sleep", sleep)
    return clock


def test_request_learns_report_rate_limit_from_headers(monkeypatch):
    class Tap:
        config = {"request_timeout_seconds": 10}

    clock = install_fake_clock(monkeypatch)
    url = "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows"
    stream = stream_module.ExtendStream(tap=Tap())
    stream._session = FakeSession([
        FakeHTTPResponse(200, "{}", {"x-ratelimit-limit": "60", "x-ratelimit-remaining": "59"}, url),
        FakeHTTPResponse(200, "{}", {"x-ratelimit-limit": "60", "x-ratelimit-remaining": "58"}, url),
    ])

    stream._request(url)
    stream._request(url)

    assert stream_module.ExtendStream._rate_limit_requests_per_second["reports:TESTCLIENT"] == 1.0
    assert clock["sleeps"] == [1.0]


def test_request_keeps_conservative_rate_when_later_headers_are_higher(monkeypatch):
    class Tap:
        config = {"request_timeout_seconds": 10}

    clock = install_fake_clock(monkeypatch)
    url = "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/CustomerOrders"
    stream = stream_module.ExtendStream(tap=Tap())
    stream._session = FakeSession([
        FakeHTTPResponse(200, "{}", {"x-ratelimit-limit": "300", "x-ratelimit-remaining": "299"}, url),
        FakeHTTPResponse(200, "{}", {"x-ratelimit-limit": "3000", "x-ratelimit-remaining": "2999"}, url),
    ])

    stream._request(url)
    stream._request(url)

    bucket = "v1_0:TESTCLIENT:CustomerOrders"
    assert stream_module.ExtendStream._rate_limit_requests_per_second[bucket] == 5.0
    assert clock["sleeps"] == [0.2]


def test_request_caps_unusually_high_rate_limit_headers(monkeypatch):
    class Tap:
        config = {"request_timeout_seconds": 10}

    clock = install_fake_clock(monkeypatch)
    url = "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/ProductSupplierAgreements"
    stream = stream_module.ExtendStream(tap=Tap())
    stream._session = FakeSession([
        FakeHTTPResponse(200, "{}", {"x-ratelimit-limit": "1000000", "x-ratelimit-remaining": "999999"}, url),
        FakeHTTPResponse(200, "{}", {"x-ratelimit-limit": "1000000", "x-ratelimit-remaining": "999998"}, url),
    ])

    stream._request(url)
    stream._request(url)

    bucket = "v1_0:TESTCLIENT:ProductSupplierAgreements"
    assert stream_module.ExtendStream._rate_limit_requests_per_second[bucket] == 15.0
    assert clock["sleeps"] == [1 / 15.0]


def test_request_defers_exhausted_bucket_until_reset(monkeypatch):
    class Tap:
        config = {"request_timeout_seconds": 10}

    clock = install_fake_clock(monkeypatch)
    url = "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/PurchaseOrders/RP-1"
    reset_epoch = clock["epoch"] + 30.0
    stream = stream_module.ExtendStream(tap=Tap())
    stream._session = FakeSession([
        FakeHTTPResponse(
            200,
            "{}",
            {
                "x-ratelimit-limit": "300",
                "x-ratelimit-remaining": "0",
                "x-ratelimit-reset": str(reset_epoch),
            },
            url,
        ),
        FakeHTTPResponse(200, "{}", {"x-ratelimit-limit": "300", "x-ratelimit-remaining": "299"}, url),
    ])

    stream._request(url)
    stream._request(url)

    assert clock["sleeps"] == [31.0]


def test_request_retries_transient_extend_400s_every_configured_wait(monkeypatch):
    class Tap:
        config = {
            "request_timeout_seconds": 10,
        }

    stream_module.ExtendStream._next_request_at = 0
    sleeps = []
    monkeypatch.setattr(stream_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    stream = stream_module.ExtendStream(tap=Tap())
    monkeypatch.setattr(stream, "_apply_client_throttle", lambda url=None: None)
    session = FakeSession([
        FakeHTTPResponse(400, DEADLOCK_BODY),
        FakeHTTPResponse(400, DEADLOCK_BODY),
        FakeHTTPResponse(200, "{}"),
    ])
    stream._session = session

    response = stream._request(
        "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
        params={"pageNumber": 11},
    )

    assert response.status_code == 200
    assert len(session.calls) == 3
    assert sleeps == [10.0, 10.0]


def test_request_can_limit_transient_extend_400_attempts(monkeypatch):
    class Tap:
        config = {
            "request_timeout_seconds": 10,
        }

    stream_module.ExtendStream._next_request_at = 0
    sleeps = []
    monkeypatch.setattr(stream_module.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(stream_module, "RETRYABLE_CLIENT_ERROR_MAX_ATTEMPTS", 2)

    stream = stream_module.ExtendStream(tap=Tap())
    monkeypatch.setattr(stream, "_apply_client_throttle", lambda url=None: None)
    session = FakeSession([
        FakeHTTPResponse(400, DEADLOCK_BODY),
        FakeHTTPResponse(400, DEADLOCK_BODY),
    ])
    stream._session = session

    try:
        stream._request(
            "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
            params={"pageNumber": 11},
        )
    except stream_module.requests.exceptions.HTTPError:
        pass
    else:
        raise AssertionError("Expected HTTPError after configured retry limit")

    assert len(session.calls) == 2
    assert sleeps == [10.0]


def test_iter_report_days_uses_full_day_window_and_paginates():
    fake_stream = FakeReportStream([
        {
            "orderHeaderList": [
                {"orderNumber": "1", "changeDate": "2026-04-15T12:00:00+02:00"}
            ],
            "paginationInfo": {"currentPage": 1, "totalPages": 2},
        },
        {
            "orderHeaderList": [
                {"orderNumber": "2", "changeDate": "2026-04-15T13:00:00+02:00"}
            ],
            "paginationInfo": {"currentPage": 2, "totalPages": 2},
        },
    ])

    records = list(
        stream_module._iter_report_days(
            fake_stream,
            "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
            "orderHeaderList",
            "2026-04-15T00:00:00Z",
            "2026-04-15T23:59:59Z",
        )
    )

    assert [record["orderNumber"] for record in records] == ["1", "2"]
    assert fake_stream.requests == [
        {
            "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
            "params": {
                "pageNumber": 1,
                "changeDate": "2026-04-15T00:00:00",
                "toChangeDate": "2026-04-15T23:59:59",
            },
        },
        {
            "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
            "params": {
                "pageNumber": 2,
                "changeDate": "2026-04-15T00:00:00",
                "toChangeDate": "2026-04-15T23:59:59",
            },
        },
    ]


def test_reports_order_headers_preserves_observed_header_report_fields(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-15T23:59:59+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-15T00:00:00Z",
        }

    sample = {
        "orderNumber": "ORDER-1001",
        "orderNumberExternal": "ORDER-1001",
        "orderNumberEndCustomer": None,
        "orderDate": "2026-04-15T23:52:04.107+02:00",
        "changeDate": "2026-04-15T23:52:05.583+02:00",
        "askedDeliveryDate": "2026-04-15T00:00:00+02:00",
        "orderType": "Normal",
        "orderStatus": "Incoming",
        "orderPaymentStatus": 0,
        "customerNumber": "2705237",
        "orderReference": "Example Customer",
        "invoiceEmail": "customer@example.test",
        "requestedForwarder": "Test Forwarder",
        "requestedTransportMode": "Test Transport Mode",
        "salesChannel": "Test Sales Channel",
        "paymentType": "TestPayment",
        "deliveryName1": "Example Customer",
        "deliveryAddress1": "Example Street 1",
        "deliveryPostalCode": "12345",
        "deliveryCity": "Example City",
        "deliveryCountryId": "SE",
        "invoiceName": "Example Customer",
        "invoiceAddress1": "Example Street 1",
        "invoicePostalCode": "12345",
        "invoiceCity": "Example City",
        "invoiceCountryId": "SE",
    }
    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        yield sample

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderHeadersStream(tap=Tap())
    records = list(stream.get_records())

    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
        "list_key": "orderHeaderList",
        "start_date": "2026-04-15",
        "end_date": "2026-04-15",
    }]
    assert records[0]["orderNumber"] == "ORDER-1001"
    assert records[0]["orderPaymentStatus"] == "0"
    assert records[0]["orderReference"] == "Example Customer"
    assert records[0]["requestedTransportMode"] == "Test Transport Mode"
    assert records[0]["deliveryAddress1"] == "Example Street 1"
    assert records[0]["invoiceCountryId"] == "SE"
    assert "customerName" not in records[0]
    assert "totalPrice" not in records[0]
    assert "currency" not in records[0]
    assert "warehouse" not in records[0]


def test_reports_order_rows_preserves_observed_row_report_fields(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-15T23:59:59+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-15T00:00:00Z",
        }

    sample = {
        "orderRowId": "69b27097-d6bd-450d-92a7-38ed30da5ef3",
        "position": 20,
        "subPosition": 0,
        "supplyMode": "Warehouse",
        "productNumber": "8744",
        "productName": "Test Product Name",
        "productUnitName": "ST",
        "orderQuantity": 1.0000,
        "price": 159.2000,
        "vatPercent": 25.000,
        "currencyId": "TST",
        "currencyExchangeRate": 1.000000000000,
        "expectedDeliveryDate": "2026-04-17T08:00:36+02:00",
        "shipDate": "2026-04-17T08:00:01+02:00",
        "backOrderHandling": "NONE",
        "notes": None,
        "orderRowStatus": "Incoming",
        "shipmentNumber": None,
        "warehouseShortName": "TESTCLIENT2",
        "orderNumber": "ORDER-1001",
        "productNotes": None,
        "handlingMark": None,
        "shippingMark": None,
        "batchNumber": None,
        "salesUnit": "ST",
        "salesUnitQuantity": 1.0000,
        "agreedOrderPickTime": "2026-04-16T08:00:36+02:00",
        "listPrice": 159.2000,
        "ordinalPrice": 159.2000,
        "productSalesUnitPrice": 159.2000,
        "productVisibility": "AlwaysVisible",
        "originalExpectedDeliveryDate": "2026-04-16T08:00:36+02:00",
        "orderReasonCode": "",
        "structuredCost": 0.0000,
        "exciseDutyCost": 0.0000,
        "customerBonusCost": 0.0000,
        "cost": 128.2400,
        "agreeedOrderRowId": "",
        "orderDate": "2026-04-15T23:52:04.107+02:00",
        "orderPriority": 10,
        "getBalanceFromAgreedOrder": True,
        "allocationStatus": "Physical",
        "releaseToWarehouseWhenAllocated": False,
        "changeDate": "2026-04-15T00:00:00+00:00",
    }
    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        yield sample

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderRowsStream(tap=Tap())
    records = list(stream.get_records())

    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
        "list_key": "orderRowList",
        "start_date": "2026-04-15",
        "end_date": "2026-04-15",
    }]
    assert records[0]["orderRowId"] == "69b27097-d6bd-450d-92a7-38ed30da5ef3"
    assert records[0]["orderQuantity"] == 1.0000
    assert records[0]["price"] == 159.2000
    assert records[0]["currencyId"] == "TST"
    assert records[0]["warehouseShortName"] == "TESTCLIENT2"
    assert records[0]["allocationStatus"] == "Physical"
    assert "quantity" not in records[0]
    assert "unitPrice" not in records[0]
    assert "currency" not in records[0]
    assert "warehouse" not in records[0]


def test_reports_state_date_range_overrides_start_date_and_sync_upper_bound(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-20T23:59:59+00:00"
        state = {
            "bookmarks": {
                "reports_order_rows": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-19T00:00:00Z",
                }
            },
            "reports_start_date": "2026-01-01",
            "reports_end_date": "2026-01-31",
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2024-01-01T00:00:00Z",
        }

    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        return iter(())

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderRowsStream(tap=Tap())
    records = list(stream.get_records())

    assert records == []
    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderRows",
        "list_key": "orderRowList",
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }]


def test_reports_state_date_range_reads_sdk_stream_tap_state():
    class Stream:
        name = "reports_order_rows"
        tap_state = {
            "bookmarks": {
                "reports_order_rows": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-19T00:00:00Z",
                }
            },
            "reports_start_date": "2026-01-01",
            "reports_end_date": "2026-01-31",
        }

    assert stream_module._report_state_date_range(Stream()) == (
        "2026-01-01",
        "2026-01-31",
    )


def test_top_level_reports_state_range_takes_precedence(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-20T23:59:59+00:00"
        state = {
            "reports_start_date": "2026-03-01",
            "reports_end_date": "2026-03-31",
            "bookmarks": {
                "reports_order_headers": {
                    "reports_start_date": "2026-02-01",
                    "reports_end_date": "2026-02-28",
                },
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2024-01-01T00:00:00Z",
        }

    calls = []

    def fake_iter_report_days(
        stream,
        url,
        list_key,
        start_date,
        end_date,
    ):
        calls.append({
            "url": url,
            "list_key": list_key,
            "start_date": start_date,
            "end_date": end_date,
        })
        return iter(())

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderHeadersStream(tap=Tap())
    records = list(stream.get_records())

    assert records == []
    assert calls == [{
        "url": "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
        "list_key": "orderHeaderList",
        "start_date": "2026-03-01",
        "end_date": "2026-03-31",
    }]


def test_reports_order_headers_skip_when_customer_orders_bookmark_exists(monkeypatch):
    class Tap:
        _loaded_state = {
            "bookmarks": {
                "customer_orders": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-30T10:27:06",
                }
            }
        }
        state = {
            "bookmarks": {
                "customer_orders": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-30T10:27:06",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2024-01-01T00:00:00Z",
        }

    def fail_iter(*args, **kwargs):
        raise AssertionError("reports_order_headers should not hit the reports endpoint")

    monkeypatch.setattr(stream_module, "_iter_report_days", fail_iter)

    stream = stream_module.ReportsOrderHeadersStream(tap=Tap())
    records = list(stream.get_records())

    assert records == []


def test_reports_order_rows_skip_when_customer_orders_bookmark_exists(monkeypatch):
    class Tap:
        _loaded_state = {
            "bookmarks": {
                "customer_orders": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-30T10:27:06",
                }
            }
        }
        state = {
            "bookmarks": {
                "customer_orders": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-30T10:27:06",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2024-01-01T00:00:00Z",
        }

    def fail_iter(*args, **kwargs):
        raise AssertionError("reports_order_rows should not hit the reports endpoint")

    monkeypatch.setattr(stream_module, "_iter_report_days", fail_iter)

    stream = stream_module.ReportsOrderRowsStream(tap=Tap())
    records = list(stream.get_records())

    assert records == []


def test_reports_order_headers_do_not_skip_when_customer_orders_bookmark_was_seeded_mid_run(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-20T23:59:59+00:00"
        _loaded_state = {"bookmarks": {}}
        state = {
            "bookmarks": {
                "customer_orders": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-20T23:59:59",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-20T00:00:00Z",
        }

    calls = []

    def fake_iter_report_days(stream, url, list_key, start_date, end_date):
        calls.append((url, list_key, start_date, end_date))
        return iter(())

    monkeypatch.setattr(stream_module, "_iter_report_days", fake_iter_report_days)

    stream = stream_module.ReportsOrderHeadersStream(tap=Tap())
    records = list(stream.get_records())

    assert records == []
    assert calls == [(
        "https://api.example.test/RESTAPI/reports/TESTCLIENT/OrderHeaders",
        "orderHeaderList",
        "2026-04-20",
        "2026-04-20",
    )]


def test_sanitize_signpost_bookmark_state_prefers_signpost_and_drops_noise():
    cleaned = stream_module._sanitize_signpost_bookmark_state(
        {
            "replication_key": "changeDate",
            "replication_key_value": "2026-04-30T08:31:57",
            "replication_key_signpost": "2026-04-30T10:31:26",
            "starting_replication_value": "2026-04-30T08:31:57",
            "progress_markers": {
                "replication_key": "changeDate",
                "replication_key_value": "2026-04-30 11:42:43",
            },
            "partitions": {"foo": "bar"},
        },
        "changeDate",
    )

    assert cleaned == {
        "replication_key": "changeDate",
        "replication_key_value": "2026-04-30T10:31:26",
    }


def test_sanitize_signpost_bookmark_state_uses_progress_marker_when_no_signpost():
    cleaned = stream_module._sanitize_signpost_bookmark_state(
        {
            "progress_markers": {
                "replication_key": "modifiedDate",
                "replication_key_value": "2026-04-30T10:31:26",
            },
            "starting_replication_value": "2026-04-30T09:00:00",
        },
        "modifiedDate",
    )

    assert cleaned == {
        "replication_key": "modifiedDate",
        "replication_key_value": "2026-04-30T10:31:26",
    }


def test_purchase_orders_uses_change_date_datetime_range(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-20T17:18:46+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-20T00:00:00Z",
        }
        warehouse_codes = None

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "purchaseOrderList": [],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.PurchaseOrdersStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    assert list(stream.get_records()) == []
    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/PurchaseOrders",
        "params": {
            "changeDateFrom": "2026-04-20T00:00:00",
            "changeDateTo": "2026-04-21T17:18:46",
            "pageNumber": 1,
        },
    }]


def test_purchase_orders_strips_timezone_offsets_from_problem_datetimes():
    class Tap:
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    stream = stream_module.PurchaseOrdersStream(tap=Tap())
    record = stream._map_detail(
        {
            "header": {
                "purchaseNumber": "RP-11423",
                "createDate": "2026-06-29T16:13:19.11+02:00",
                "changeDate": "2026-07-03T05:31:25+02:00",
            },
            "rows": [
                {
                    "expectedDeliveryDate": "2026-07-03T00:00:00+02:00",
                    "statusChangeDate": "2026-07-02T09:42:04+02:00",
                }
            ],
            "shipments": [],
        },
        {},
    )

    rows = json.loads(record["rows"])
    assert record["createDate"] == "2026-06-29T16:13:19.11"
    assert record["changeDate"] == "2026-07-03T05:31:25"
    assert rows[0]["expectedDeliveryDate"] == "2026-07-03T00:00:00"
    assert rows[0]["statusChangeDate"] == "2026-07-02T09:42:04"


def test_purchase_orders_missing_detail_400_falls_back_to_summary(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-22T11:20:00+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-22T00:00:00Z",
        }
        warehouse_codes = None

    purchase_summary = {
        "purchaseNumber": "RP-404",
        "status": "Ordered",
        "createDate": "2026-04-22T10:49:27.887+02:00",
        "warehouse": "TESTWH",
        "isOpen": True,
        "isReceived": False,
        "externalOrderNumber": "",
        "supplierNumber": "109",
        "supplierName": "Supplier Example",
        "supplierOrderNumber": "",
        "shippedDate": None,
        "changeDate": "2026-04-22T10:50:46.963",
    }

    def fake_request(url, params=None):
        if url.endswith("/PurchaseOrders"):
            return FakeResponse(
                {
                    "purchaseOrderList": [purchase_summary],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }
            )
        response = FakeHTTPResponse(
            400,
            '{"Message": "Error getting purchase order: There is no row at position 0."}',
        )
        response.raise_for_status()
        raise AssertionError("unreachable")

    stream = stream_module.PurchaseOrdersStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records())

    assert len(records) == 1
    assert records[0]["purchaseNumber"] == "RP-404"
    assert records[0]["rows"] == "[]"
    assert records[0]["shipments"] == "[]"
    assert records[0]["supplierAgreementNumber"] is None


def test_supplier_agreements_uses_change_date_datetime_range_and_maps_new_fields(monkeypatch):
    class Tap:
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-22T02:00:00Z",
        }

    sample = {
        "name": "Agreement Example",
        "customsLeadTime": 0.0,
        "transportLeadTime": 40.0,
        "deliveryMethod": "Carrier Example",
        "forwarderCustomerNumber": "",
        "paymentTerms": "100%",
        "penalty": "",
        "ordererAddress1": "Address Example",
        "ordererPostalCode": "12345",
        "ordererCity": "City Example",
        "ordererCountryId": "CN",
        "incoterms": "EXW",
        "transportConditionDescription": None,
        "currencyId": "USD",
        "makeAutomaticPurchase": False,
        "purchaseNotificationSystem": "Manual",
        "manufacturingLeadTime": 80.0,
        "useRowBasedLeadTime": False,
        "validFrom": None,
        "validTo": None,
        "active": True,
        "supplierAgreementNumber": 879127612,
        "capacity": None,
        "authorizationNumber": None,
        "latitude": None,
        "longitude": None,
        "internalContact": "Contact Example",
        "externalContact": " ",
        "purchaseNotificationAddress": "",
        "changeDate": "2026-04-22T12:49:35.677",
        "supplierAgreementId": "agreement-id-1",
    }
    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "SupplierAgreementList": [sample],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.SupplierAgreementsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records())

    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/SupplierAgreement",
        "params": {
            "active": "true",
            "pageNumber": 1,
        },
    }]
    assert records == [{
        "supplierAgreementNumber": 879127612,
        "supplierAgreementId": "agreement-id-1",
        "name": "Agreement Example",
        "active": True,
        "currencyId": "USD",
        "manufacturingLeadTime": 80.0,
        "transportLeadTime": 40.0,
        "customsLeadTime": 0.0,
        "paymentTerms": "100%",
        "deliveryMethod": "Carrier Example",
        "forwarderCustomerNumber": "",
        "penalty": "",
        "incoterms": "EXW",
        "transportConditionDescription": None,
        "purchaseNotificationSystem": "Manual",
        "useRowBasedLeadTime": False,
        "validFrom": None,
        "validTo": None,
        "ordererAddress1": "Address Example",
        "ordererPostalCode": "12345",
        "ordererCity": "City Example",
        "ordererCountryId": "CN",
        "makeAutomaticPurchase": False,
        "capacity": None,
        "authorizationNumber": None,
        "latitude": None,
        "longitude": None,
        "internalContact": "Contact Example",
        "externalContact": " ",
        "purchaseNotificationAddress": "",
        "changeDate": "2026-04-22T12:49:35.677",
    }]
    assert stream.get_child_context(records[0], None) == {"supplierAgreementNumber": 879127612}


def test_product_supplier_agreements_maps_change_date(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-22T14:00:00+00:00"
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    sample = {
        "productNumber": "SKU-1",
        "supplierAgreementNumber": 101,
        "supplierAgreementName": "Agreement Example",
        "supplierName": "Supplier Example",
        "supplierAgreementCurrencyId": "USD",
        "supplierProductNumber": "SUP-1",
        "supplierProductName": "Supplier Product Example",
        "price": 10.5,
        "vatPercent": 25.0,
        "manufacturingLeadTimeHour": 24.0,
        "supplierAgreementProductionLeadtimeHours": 12,
        "supplierAgreementTransportLeadtimeHours": 36,
        "inactive": False,
        "statisticalNumber": "123",
        "country": "CN",
        "productUnitId": "ST",
        "useOtherPurchaseUnit": False,
        "purchaseProductUnit": "BOX",
        "quantityPerPurchaseProductUnit": 5.0,
        "changeDate": "2026-04-22T12:49:35.677",
    }
    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "productSupplierAgreementList": [sample],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.ProductSupplierAgreementsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records({"supplierAgreementNumber": 101}))

    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/ProductSupplierAgreements",
        "params": {
            "supplierAgreementNumber": 101,
            "pageNumber": 1,
        },
    }]
    assert records == [{
        "productNumber": "SKU-1",
        "supplierAgreementNumber": 101,
        "supplierAgreementName": "Agreement Example",
        "supplierName": "Supplier Example",
        "supplierAgreementCurrencyId": "USD",
        "supplierProductNumber": "SUP-1",
        "supplierProductName": "Supplier Product Example",
        "price": 10.5,
        "vatPercent": 25.0,
        "manufacturingLeadTimeHour": 24.0,
        "supplierAgreementProductionLeadtimeHours": 12,
        "supplierAgreementTransportLeadtimeHours": 36,
        "inactive": False,
        "statisticalNumber": "123",
        "country": "CN",
        "productUnitId": "ST",
        "useOtherPurchaseUnit": False,
        "purchaseProductUnit": "BOX",
        "quantityPerPurchaseProductUnit": 5.0,
        "changeDate": "2026-04-22T12:49:35.677",
    }]
    assert stream.parent_stream_type is stream_module.SupplierAgreementsStream


def test_product_supplier_agreements_uses_bookmark_and_persists_run_upper_bound(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-22T14:00:00+00:00"
        state = {
            "bookmarks": {
                "product_supplier_agreements": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-22T10:00:00",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "productSupplierAgreementList": [],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.ProductSupplierAgreementsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records({"supplierAgreementNumber": 202}))
    stream.finalize_state_progress_markers()

    assert records == []
    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/ProductSupplierAgreements",
        "params": {
            "supplierAgreementNumber": 202,
            "changeDateFrom": "2026-04-22T10:00:00",
            "changeDateTo": "2026-04-22T14:00:00",
            "pageNumber": 1,
        },
    }]
    assert stream.stream_state["replication_key"] == "changeDate"
    assert stream.stream_state["replication_key_value"] == "2026-04-22T14:00:00"


def test_product_supplier_agreements_uses_legacy_signpost_bookmark(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-30T08:31:57+00:00"
        state = {
            "bookmarks": {
                "product_supplier_agreements": {
                    "replication_key_signpost": "2026-04-30T06:31:48",
                    "starting_replication_value": "2010-01-01T00:00:00.000Z",
                    "progress_markers": {
                        "Note": "Progress is not resumable if interrupted.",
                        "replication_key": "changeDate",
                        "replication_key_value": "2026-04-30 08:20:35",
                    },
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "productSupplierAgreementList": [],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.ProductSupplierAgreementsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records({"supplierAgreementNumber": 303}))
    stream.finalize_state_progress_markers()

    assert records == []
    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/ProductSupplierAgreements",
        "params": {
            "supplierAgreementNumber": 303,
            "changeDateFrom": "2026-04-30T06:31:48",
            "changeDateTo": "2026-04-30T08:31:57",
            "pageNumber": 1,
        },
    }]
    assert stream.stream_state == {
        "replication_key": "changeDate",
        "replication_key_value": "2026-04-30T08:31:57",
    }


def test_product_supplier_agreements_ignores_current_run_signpost_on_first_sync(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-30T08:56:43+00:00"
        state = {
            "bookmarks": {
                "product_supplier_agreements": {
                    "replication_key_signpost": "2026-04-30T08:56:43",
                    "starting_replication_value": "2010-01-01T00:00:00.000Z",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2010-01-01T00:00:00Z",
        }

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "productSupplierAgreementList": [],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.ProductSupplierAgreementsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records({"supplierAgreementNumber": 404}))

    assert records == []
    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/ProductSupplierAgreements",
        "params": {
            "supplierAgreementNumber": 404,
            "pageNumber": 1,
        },
    }]


def test_product_supplier_agreements_first_run_ignores_start_date_until_bookmark_exists(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-22T14:00:00+00:00"
        state = {"bookmarks": {"product_supplier_agreements": {}}}
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2010-01-01T00:00:00Z",
        }

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return {
                    "productSupplierAgreementList": [],
                    "paginationInfo": {"currentPage": 1, "totalPages": 1},
                }

        return Response()

    stream = stream_module.ProductSupplierAgreementsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records({"supplierAgreementNumber": 303}))
    stream.finalize_state_progress_markers()

    assert records == []
    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/ProductSupplierAgreements",
        "params": {
            "supplierAgreementNumber": 303,
            "pageNumber": 1,
        },
    }]
    assert stream.stream_state["replication_key"] == "changeDate"
    assert stream.stream_state["replication_key_value"] == "2026-04-22T14:00:00"


def test_product_supplier_agreements_skips_state_increment_for_null_change_date():
    class Tap:
        state = {
            "bookmarks": {
                "product_supplier_agreements": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-22T10:00:00",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    stream = stream_module.ProductSupplierAgreementsStream(tap=Tap())
    before_state = dict(stream.stream_state)

    stream._increment_stream_state({
        "productNumber": "SKU-NULL",
        "supplierAgreementNumber": 404,
        "changeDate": None,
    })

    assert stream.stream_state == before_state


def test_products_first_run_ignores_start_date_until_bookmark_exists(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-22T14:00:00+00:00"
        state = {"bookmarks": {"products": {}}}
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2010-01-01T00:00:00Z",
        }

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                if url.endswith("/Products/SKU-1"):
                    return {"productData": {}}
                return [{
                    "productNumber": "SKU-1",
                    "productName": "Product Example",
                    "createDate": "2026-04-22T12:49:35.677",
                    "productUnit": "PCS",
                    "cost": 10.0,
                    "currency": "USD",
                    "countryOfOrigin": "CN",
                    "supplyMode": "Stocked",
                    "manufacturer": "Maker",
                    "manufacturerProductNumber": "M-1",
                    "gtinNumberList": "123",
                    "enabled": True,
                    "statisticalCategory1": "A",
                    "statisticalCategory2": "B",
                    "statisticalCategory3": "C",
                    "productGroupsAndCategories": {"companyGroup": "Group", "financialCategory": "Cat"},
                    "warehouse": "WH1",
                    "availableBalance": 5,
                }]

        return Response()

    stream = stream_module.ProductsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records())
    stream.finalize_state_progress_markers()

    assert captured == [
        {
            "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/Products",
            "params": {
                "pageCount": 100,
                "pageOffset": 0,
            },
        },
        {
            "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/Products/SKU-1",
            "params": {},
        },
    ]
    assert records == [{
        "productNumber": "SKU-1",
        "productName": "Product Example",
        "createDate": "2026-04-22T12:49:35.677",
        "productUnit": "PCS",
        "cost": 10.0,
        "currency": "USD",
        "countryOfOrigin": "CN",
        "supplyMode": "Stocked",
        "manufacturer": "Maker",
        "manufacturerProductNumber": "M-1",
        "gtinNumberList": "123",
        "enabled": True,
        "statisticalCategory1": "A",
        "statisticalCategory2": "B",
        "statisticalCategory3": "C",
        "companyGroup": "Group",
        "financialCategory": "Cat",
        "modifiedDate": None,
        "annulled": None,
        "productHandlings": None,
        "assortmentCategory": None,
        "productVisibility": None,
        "detailChangedDate": None,
        "warehouse_stock": '[{"warehouse": "WH1", "availableBalance": 5}]',
    }]
    assert stream.stream_state["replication_key"] == "modifiedDate"
    assert stream.stream_state["replication_key_value"] == "2026-04-22T14:00:00"


def test_products_subsequent_run_uses_bookmark_window(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-04-22T14:00:00+00:00"
        state = {
            "bookmarks": {
                "products": {
                    "replication_key": "modifiedDate",
                    "replication_key_value": "2026-04-21T10:00:00",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                return []

        return Response()

    stream = stream_module.ProductsStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records())
    stream.finalize_state_progress_markers()

    assert records == []
    assert captured == [{
        "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/Products",
        "params": {
            "pageCount": 100,
            "pageOffset": 0,
            "modifiedDateFrom": "2026-04-21T10:00:00",
            "modifiedDateTo": "2026-04-22T14:00:00",
        },
    }]
    assert stream.stream_state["replication_key"] == "modifiedDate"
    assert stream.stream_state["replication_key_value"] == "2026-04-22T14:00:00"


def test_products_created_groups_before_detail_and_falls_back_for_invalid_dates(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-08-06T12:00:00+00:00"
        state = {"bookmarks": {"products_created": {}}}
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    calls = []

    def fake_request(url, params=None):
        calls.append({"url": url, "params": dict(params or {})})

        class Response:
            def json(self):
                if url.endswith("/Products/SKU-NEW"):
                    return {
                        "productData": {
                            "annulled": False,
                            "productVisibility": "AlwaysVisible",
                            "productServices": {"productHandlings": ["Standard"]},
                            "productGroupsAndCategories": {"assortmentCategory": "SALE"},
                            "productDates": {"changedDate": "2026-08-06T10:55:00+02:00"},
                        }
                    }
                if "/Products/" in url:
                    return {"productData": {}}
                return [
                    {
                        "productNumber": "SKU-NEW",
                        "productName": "New product",
                        "createDate": "not-a-date",
                        "warehouse": "WH0",
                        "availableBalance": 1,
                        "enabled": True,
                    },
                    {
                        "productNumber": "SKU-NEW",
                        "productName": "New product",
                        "createDate": "2026-08-06T10:52:20.667+02:00",
                        "warehouse": "WH1",
                        "availableBalance": 2,
                        "enabled": True,
                    },
                    {
                        "productNumber": "SKU-NEW",
                        "productName": "New product",
                        "createDate": "2026-08-06T10:52:20.667+02:00",
                        "warehouse": "WH2",
                        "availableBalance": 3,
                        "enabled": True,
                    },
                    {
                        "productNumber": "SKU-OLD",
                        "productName": "Old product",
                        "createDate": "2026-08-05T11:59:59Z",
                    },
                    {
                        "productNumber": "SKU-BAD-DATE",
                        "productName": "Invalid date",
                        "createDate": "not-a-date",
                        "warehouse": "BAD1",
                    },
                    {
                        "productNumber": "SKU-BAD-DATE",
                        "productName": "Invalid date",
                        "createDate": None,
                        "warehouse": "BAD2",
                    },
                    {
                        "productNumber": "SKU-BAD-DATE",
                        "productName": "Invalid date",
                        "createDate": "",
                        "warehouse": "BAD3",
                    },
                ]

        return Response()

    stream = stream_module.ProductsCreatedStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records())
    stream.finalize_state_progress_markers()

    assert [record["productNumber"] for record in records] == [
        "SKU-NEW",
        "SKU-BAD-DATE",
    ]
    assert records[0]["productHandlings"] == '["Standard"]'
    assert records[0]["warehouse_stock"] == (
        '[{"warehouse": "WH0", "availableBalance": 1}, '
        '{"warehouse": "WH1", "availableBalance": 2}, '
        '{"warehouse": "WH2", "availableBalance": 3}]'
    )
    assert records[1]["createDate"] is None
    assert records[1]["warehouse_stock"] == (
        '[{"warehouse": "BAD1", "availableBalance": 0}, '
        '{"warehouse": "BAD2", "availableBalance": 0}, '
        '{"warehouse": "BAD3", "availableBalance": 0}]'
    )
    assert calls == [
        {
            "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/Products",
            "params": {"pageCount": 100, "pageOffset": 0},
        },
        {
            "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/Products/SKU-NEW",
            "params": {},
        },
        {
            "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/Products/SKU-BAD-DATE",
            "params": {},
        },
    ]
    assert stream.stream_state["replication_key"] == "createDate"
    assert stream.stream_state["replication_key_value"] == "2026-08-06T12:00:00"


def test_products_created_uses_bookmark_with_24_hour_overlap(monkeypatch):
    class Tap:
        _extend_sync_upper_bound = "2026-08-07T12:00:00+00:00"
        state = {
            "bookmarks": {
                "products_created": {
                    "replication_key": "createDate",
                    "replication_key_value": "2026-08-07T10:00:00Z",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }

    detail_products = []

    def fake_request(url, params=None):
        class Response:
            def json(self):
                if "/Products/" in url:
                    detail_products.append(url.rsplit("/", 1)[-1])
                    return {"productData": {}}
                return [
                    {
                        "productNumber": "SKU-OVERLAP",
                        "productName": "Overlap product",
                        "createDate": "2026-08-06T10:00:00Z",
                    },
                    {
                        "productNumber": "SKU-TOO-OLD",
                        "productName": "Too old",
                        "createDate": "2026-08-06T09:59:59Z",
                    },
                ]

        return Response()

    stream = stream_module.ProductsCreatedStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)

    records = list(stream.get_records())
    stream.finalize_state_progress_markers()

    assert [record["productNumber"] for record in records] == ["SKU-OVERLAP"]
    assert detail_products == ["SKU-OVERLAP"]
    assert stream.stream_state["replication_key"] == "createDate"
    assert stream.stream_state["replication_key_value"] == "2026-08-07T12:00:00"


def test_customer_orders_first_run_skips_endpoint_and_seeds_bookmark(monkeypatch):
    class Tap:
        state = {"bookmarks": {"customer_orders": {}}}
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
            "start_date": "2026-04-20T00:00:00Z",
        }
        _extend_sync_upper_bound = "2026-04-22T14:00:00+00:00"

    stream = stream_module.CustomerOrdersStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("CustomerOrders endpoint should not be called on first run")))

    records = list(stream.get_records())
    stream.finalize_state_progress_markers()

    assert records == []
    assert stream.stream_state["replication_key"] == "changeDate"
    assert stream.stream_state["replication_key_value"] == "2026-04-22T14:00:00"


def test_customer_orders_incremental_uses_customer_orders_and_detail(monkeypatch):
    class Tap:
        state = {
            "bookmarks": {
                "customer_orders": {
                    "replication_key": "changeDate",
                    "replication_key_value": "2026-04-20T10:00:00Z",
                }
            }
        }
        config = {
            "api_url": "https://api.example.test/RESTAPI",
            "client": "TESTCLIENT",
        }
        _extend_sync_upper_bound = "2026-04-22T14:00:00+00:00"

    captured = []

    def fake_request(url, params=None):
        captured.append({"url": url, "params": dict(params or {})})
        if url.endswith("/CustomerOrders"):
            return FakeResponse([{
                "orderNumber": "SO-2",
                "orderNumberExternal": "EXT-2",
                "orderType": "Normal",
                "orderStatus": "Reserved",
                "orderDate": "2026-04-22T12:00:00+00:00",
                "askedDeliveryDate": "2026-04-23T00:00:00+00:00",
                "slaDate": None,
                "customerNumber": "456",
                "customerName": "Customer Example",
                "totalPrice": 44.0,
                "changeDate": "2026-04-22T12:01:00+00:00",
            }])
        if url.endswith("/CustomerOrders/SO-2"):
            return FakeResponse({
                "orderHeader": {"orderNumber": "SO-2"},
                "orderRows": [{
                    "orderRowId": "row-2",
                    "position": 20,
                    "subPosition": 0,
                    "supplyMode": "Warehouse",
                    "warehouse": "MAIN",
                    "orderRowStatus": "Reserved",
                    "shipmentNumber": "SHIP-1",
                    "expectedDeliveryDate": "2026-04-23T00:00:00+00:00",
                    "shipDate": "2026-04-22T15:00:00+00:00",
                    "allocationStatus": "Physical",
                    "changeDate": "2026-04-22T12:02:00+00:00",
                    "product": {"productNumber": "SKU-2", "productName": "Product Two"},
                    "salesData": {
                        "quantity": 4,
                        "unit": "ST",
                        "unitPrice": 11.0,
                        "vatPercent": 25.0,
                        "currency": "EUR",
                    },
                }],
            })
        raise AssertionError(f"Unexpected URL {url}")

    stream = stream_module.CustomerOrdersStream(tap=Tap())
    monkeypatch.setattr(stream, "_request", fake_request)
    monkeypatch.setattr(stream_module, "_iter_report_days", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Reports should not be used when customer_orders bookmark exists")))

    records = list(stream.get_records())

    assert captured == [
        {
            "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/CustomerOrders",
            "params": {
                "pageCount": 100,
                "pageOffset": 0,
                "modifiedDateFrom": "2026-04-20T10:00:00",
            },
        },
        {
            "url": "https://api.example.test/RESTAPI/v1_0/TESTCLIENT/CustomerOrders/SO-2",
            "params": {},
        },
    ]
    assert records == [{
        "orderNumber": "SO-2",
        "orderNumberExternal": "EXT-2",
        "orderType": "Normal",
        "orderStatus": "Reserved",
        "orderDate": "2026-04-22T12:00:00+00:00",
        "askedDeliveryDate": "2026-04-23T00:00:00+00:00",
        "slaDate": None,
        "customerNumber": "456",
        "customerName": "Customer Example",
        "totalPrice": 44.0,
        "changeDate": "2026-04-22T12:01:00+00:00",
        "order_rows": json.dumps([{
            "orderRowId": "row-2",
            "position": 20,
            "subPosition": 0,
            "supplyMode": "Warehouse",
            "productNumber": "SKU-2",
            "productName": "Product Two",
            "orderQuantity": 4,
            "price": 11.0,
            "vatPercent": 25.0,
            "currencyId": "EUR",
            "expectedDeliveryDate": "2026-04-23T00:00:00+00:00",
            "shipDate": "2026-04-22T15:00:00+00:00",
            "orderRowStatus": "Reserved",
            "shipmentNumber": "SHIP-1",
            "warehouseShortName": "MAIN",
            "orderNumber": "SO-2",
            "salesUnit": "ST",
            "salesUnitQuantity": 4,
            "productSalesUnitPrice": 11.0,
            "allocationStatus": "Physical",
            "changeDate": "2026-04-22T12:02:00+00:00",
        }]),
    }]
