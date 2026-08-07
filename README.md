# tap-extend

A [Singer](https://www.singer.io/) tap for the **Extend Commerce (Lxir) REST API**, built with the [hotglue Singer SDK](https://github.com/hotgluexyz/singer-sdk).

Developed and maintained by [Optiply](mailto:dev@optiply.com) · License: Apache-2.0

---

## Overview

`tap-extend` extracts data from the Extend Commerce REST API and outputs it in the Singer format, making it compatible with any Singer-based data pipeline (Meltano, hotglue, etc.).

---

## Streams

| Stream | Endpoint | Replication |
|---|---|---|
| `SuppliersStream` | `GET /Supplier` | FULL_TABLE |
| `SupplierAgreementsStream` | `GET /SupplierAgreement` | FULL_TABLE (active=true) |
| `ProductSupplierAgreementsStream` | `GET /ProductSupplierAgreements` | INCREMENTAL child of `SupplierAgreementsStream` (first run unfiltered, later runs use `supplierAgreementNumber`, `changeDateFrom`, `changeDateTo`) |
| `ProductsStream` | `GET /Products` | INCREMENTAL (first run unfiltered, later runs use `modifiedDateFrom` + `modifiedDateTo`) |
| `ProductsCreatedStream` | `GET /Products` + `GET /Products/{productNumber}` | INCREMENTAL (unfiltered list scan; local `createDate` bookmark window with a 24-hour overlap; detail only for retained products) |
| `ProductAvailabilityStream` | `GET /ProductAvailability` | INCREMENTAL (`modifiedDateFrom`) |
| `CustomerOrdersStream` | `GET /CustomerOrders` | INCREMENTAL (used after `customer_orders` bookmark exists; fetches `CustomerOrders` + detail with `modifiedDateFrom` + `modifiedDateTo`) |
| `PurchaseOrdersStream` | `GET /PurchaseOrders` | INCREMENTAL (`createDateFrom`) |
| `ReportsOrderHeadersStream` | `GET /reports/{client}/OrderHeaders` | INCREMENTAL (day-by-day `changeDate`) |
| `ReportsOrderRowsStream` | `GET /reports/{client}/OrderRows` | INCREMENTAL (day-by-day `changeDate`) |

All streams share a common `ExtendStream` base class that handles authentication, HTTP requests, and state management.

`ProductsCreatedStream` is a temporary recovery stream for an Extend API defect where newly created products can be absent from the modified-date filtered `ProductsStream`. It scans the unfiltered list on every run, retains products created after the previous successful watermark minus 24 hours, and advances its signpost bookmark even when no records are emitted.

---

## Requirements

- Python `>=3.8, <3.12`
- [Poetry](https://python-poetry.org/)

---

## Installation

```bash
cd taps/tap-extend
poetry install
```

---

## Configuration

Copy `config.json.example` to `config.json` and fill in your credentials:

```json
{
  "api_url": "https://s05.extend.se/RESTAPI",
  "client": "YOURCLIENT",
  "username": "your-username",
  "password": "your-password",
  "start_date": "2024-01-01T00:00:00Z",
  "warehouse_codes": ["WAREHOUSE1", "WAREHOUSE2"]
}
```

### Configuration Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `api_url` | string | ✅ | Base URL for the Extend Commerce REST API |
| `client` | string | ✅ | Your Extend client identifier |
| `username` | string | ✅ | API username |
| `password` | string | ✅ | API password |
| `start_date` | string (ISO 8601) | ✅ | Earliest date to sync incremental streams from |
| `warehouse_codes` | array of strings | ❌ | Filter by specific warehouse codes |

---

## Usage

### Run the tap directly

```bash
tap-extend --config config.json
```

Or using the alternative entry point alias:

```bash
tap-extend-commerce --config config.json
```

### Run with a Singer target

```bash
tap-extend --config config.json | target-jsonl
```

### Run with state (for incremental streams)

```bash
tap-extend --config config.json --state state.json | target-jsonl
```

#### Report-only date override state

For a one-off Report Headers/Rows backfill, put report date bounds at the top level of the Singer state file. These override report bookmarks and `config.start_date` for report streams only.

```json
{
  "bookmarks": {
    "reports_order_headers": {
      "replication_key": "changeDate",
      "replication_key_value": "2026-04-19T00:00:00Z"
    },
    "reports_order_rows": {
      "replication_key": "changeDate",
      "replication_key_value": "2026-04-19T00:00:00Z"
    }
  },
  "reports_start_date": "2026-01-01",
  "reports_end_date": "2026-01-31"
}
```

---

## Development

### Running Tests

```bash
poetry run pytest tests/
```

### Project Structure

```
tap-extend/
├── tap_extend/
│   ├── __init__.py
│   ├── tap.py        # TapExtend class and CLI entry point
│   └── streams.py    # All stream definitions
├── tests/
├── config.json.example
├── connector-config.json
├── pyproject.toml
└── Extend Commerce.postman_collection.json
```

---

## Additional Resources

- **Postman Collection**: `Extend Commerce.postman_collection.json` — a ready-to-use Postman collection for exploring the Extend Commerce API endpoints.
- **Connector Config**: `connector-config.json` — connector-level configuration for use with hotglue or similar platforms.
