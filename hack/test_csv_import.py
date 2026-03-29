#!/usr/bin/env python3
"""
E2E test for CSV/JSON import into a view.

Tests the full flow:
1. Upload a test CSV file via presigned URL
2. Import it into a view sheet
3. Verify columns were auto-created in the view definition
4. Verify rows were upserted into MongoDB via the data resolver
5. Test JSON import into a second sheet
6. Test large-column CSV (LRE Pilot Sample) with column capping
7. Test sheet switching preserves data

Usage:
    python hack/test_csv_import.py [--view-id VIEW_ID] [--sheet-id SHEET_ID]

If no view/sheet IDs are provided, a temporary view is created and cleaned up.
"""

import argparse
import concurrent.futures
import json
import sys
import time
import uuid

import requests

API_BASE = "http://localhost:1994/api/v1"
TOKEN = "e8c8f312c3a7959d56b8186521add2374bc1cd7c896a2a21ba1a12a402e9f72f"
WORKSPACE_EXT_ID = "737af74d-902f-4464-9eb2-19cbd4dd0247"

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

TEST_CSV = """name,email,phone,company,role,status
John Smith,john@example.com,555-0101,ABC Realty,broker,new
Jane Doe,jane@example.com,555-0202,XYZ Properties,owner,new
Bob Wilson,bob@example.com,555-0303,123 Homes,broker,contacted
Alice Brown,alice@example.com,555-0404,Sunset Realty,owner,responded
"""

TEST_JSON = json.dumps([
    {"id": "p1", "address": "123 Main St", "market": "Phoenix", "sq_ft": 2500, "rent": 3500, "status": "prospect"},
    {"id": "p2", "address": "456 Oak Ave", "market": "Phoenix", "sq_ft": 1800, "rent": 2800, "status": "shortlist"},
    {"id": "p3", "address": "789 Elm Dr", "market": "Tempe", "sq_ft": 3200, "rent": 4200, "status": "prospect"},
])

# Subset of real LRE Pilot Sample CSV — 5 rows, 300+ columns.
# Tests large-file import with BAML-driven column curation.
LRE_CSV_HEADERS = (
    ",Property Address,City,State,Zip,LINK,Leasing Broker Name 1,"
    "Listing Broker Title 1,Leasing Broker Email 1,Property Owner Name,"
    "Property Owner Email,Property Owner Phone Number,SF Available For Lease,"
    "Asking Rent PSF,LINK,Median Household Income (TTA 70%),"
    "Population Size (TTA 70%),Renter % (TTA 70%),VPD,"
    "Visits per yr. (Shopping Center),Visits per yr. (Individual Suite),"
    "Property Name,County Name,Submarket Name,Property Type,Secondary Type,"
    "RBA,Total Available Space (SF),Rent/SF/Yr,Year Built,Zoning,"
    "Leasing Company Name,Leasing Company Contact,Sale Company Name,"
    "Sale Company Contact,Owner Address,Owner City State Zip,Owner Contact,"
    "Owner Name,Owner Phone,Property Manager Address,"
    "Property Manager City State Zip,Property Manager Contact,"
    "Property Manager Name,Property Manager Phone,Last Sale Date,"
    "Last Sale Price,For Sale Price,For Sale Status,Percent Leased,"
    "Year Renovated,Typical Floor Size,Parking Ratio,Tenancy"
)

LRE_CSV_ROW1 = (
    ',2539 Telegraph Ave,Berkeley,CA,94704-2917,'
    'https://product.costar.com/detail/all-properties/9275759/contacts,'
    'Cody Milch,Director of Marketing,cmilch@laundrylux.com,'
    'Patrick Kennedy,patrick@panoramic.com,"(510) 883-1000 X300 (p)",'
    '"2,000 - 4,434 SF",$28.00 NNN,'
    'https://analytics.placer.ai/insights/venues/1a2a21f630a89d7a29dff4af,'
    '"$91,148.93","151,562",69.70%,7.1K,N/A,36.1K,'
    'The Panoramic Berkeley,Alameda,Elmwood,Student,"Apartments (Student)",'
    '"70,000","4,434",$33.00,2019,,Panoramic Interests,Niloo Nouri,,,'
    '2539 Telegraph Ave Suite # 101,"Berkeley, CA 94704-2917",'
    'Patrick Kennedy,Panoramic Interests,5108831000,2539 Telegraph Ave,'
    '"Berkeley, CA 94704-2917",,Panoramic - The Panoramic Berkeley,'
    '5108839000,,,,N,,,11500,,,08/03/2009'
)

LRE_CSV_ROW2 = (
    ',320 Hillcrest Rd,Hollister,CA,95023,'
    'https://product.costar.com/detail/all-properties/7869935/contacts,'
    'Cody Milch,Commercial Real Estate Broker Associate,cmilch@laundrylux.com,'
    'Robert Alonso,N/A,"(650) 690-4753 (p)",'
    '"3,850","$12.60 MG",'
    'https://analytics.placer.ai/insights/venues/5636c8b19d9411a2c539fab8,'
    '"$98,493.03","49,594",35.80%,7K,N/A,7.2K,'
    ',San Benito,San Benito County,Industrial,Showroom,'
    '"36,046","9,370","$12.60 - 18.00",1920,NMU Commercial,,,'
    'Renz & Renz Investment & Commercial Brokerage,George Renz,'
    '6800 Glenview Dr,"Gilroy, CA 95020",Robert Alonso,'
    'Stanley J Alonso Issue Trust,6506904753,,,,,,2/10/2016,'
    '"$1,992,500","$3,400,000",Y,74.01,,36046,0.16,Multi,04/16/2009'
)

LRE_CSV_ROW3 = (
    ',708-714 1st St,Napa,CA,94559,'
    'https://product.costar.com/detail/all-properties/7412053/contacts,'
    'Cody Milch,Owner,cmilch@laundrylux.com,'
    'Mary Jane Stevens,maryjane@stevensmj.com,"(707) 732-0085 (p)",'
    '"2,605","$59.40 NNN",'
    'https://analytics.placer.ai/insights/venues/aa4805bd7a9b5c2cf2ed682a,'
    '"$46,877.11","29,680",67.30%,6.3K,N/A,27.4K,'
    ',Napa,Napa County,Retail,"Freestanding",'
    '"7,398","2,605",$59.40,1948,CT,Mary Jane Stevens,Mary Jane Stevens,,,'
    '3396 Soda Canyon Rd,"Napa, CA 94558",Mary Jane Stevens,'
    'Mary Jane Stevens,7077320085,,,,,,,,,N,64.79,,7398,1.35,Multi,09/29/2010'
)

LRE_CSV = "\n".join([LRE_CSV_HEADERS, LRE_CSV_ROW1, LRE_CSV_ROW2, LRE_CSV_ROW3])

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"


def api(method, path, body=None, raw=False, params=None, timeout=30):
    url = f"{API_BASE}/workspaces/{WORKSPACE_EXT_ID}{path}"
    resp = requests.request(method, url, headers=HEADERS, json=body, params=params, timeout=timeout)
    if raw:
        return resp
    payload = resp.json()
    if resp.ok and isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def find_sheet(definition, sheet_id):
    return next((s for s in definition.get("sheets", []) if s["id"] == sheet_id), None)


def find_table_component(sheet):
    if not sheet:
        return None
    return next((c for c in sheet.get("components", []) if c.get("type") == "table"), None)


def ensure_sheet(view_id, sheet_id, name, component_id, title):
    view = get_view(view_id)
    defn = view.get("definition", {})
    if find_sheet(defn, sheet_id):
        return

    defn.setdefault("sheets", []).append({
        "id": sheet_id,
        "name": name,
        "description": f"{name} test sheet",
        "layout": {"columns": 12},
        "components": [{
            "id": component_id,
            "type": "table",
            "title": title,
            "position": {"col": 0, "row": 0, "colSpan": 12, "rowSpan": 1},
            "dataSource": {"transform": []},
            "config": {"columns": []},
        }],
    })
    api("PATCH", f"/views/{view_id}", {"definition": defn})


def upload_file(filename, content, content_type="text/csv"):
    resp = api("POST", "/fs/upload-url", {"path": f"/uploads/test-import/{filename}", "content_type": content_type})
    upload_url = resp.get("upload_url")
    key = resp.get("key")
    if not upload_url:
        print(f"  {FAIL} Failed to get upload URL: {resp}")
        return None

    put_resp = requests.put(upload_url, data=content.encode("utf-8"), headers={"Content-Type": content_type}, timeout=30)
    if put_resp.status_code not in (200, 204):
        print(f"  {FAIL} PUT to presigned URL failed: {put_resp.status_code}")
        return None

    api("POST", "/fs/upload-complete", {"key": key})
    return f"/uploads/test-import/{filename}"


def create_test_view():
    defn = {
        "name": "Import Test View",
        "description": "Temporary view for import E2E test",
        "agents": [],
        "sheets": [{
            "id": "sheet-test",
            "name": "Contacts",
            "description": "Test sheet",
            "layout": {"columns": 12},
            "components": [{
                "id": "table-test",
                "type": "table",
                "title": "Contacts",
                "position": {"col": 0, "row": 0, "colSpan": 12, "rowSpan": 1},
                "dataSource": {"transform": []},
                "config": {"columns": []},
            }],
        }],
    }
    resp = api("POST", "/views", {"name": "Import Test View", "description": "E2E test", "definition": defn})
    view_id = resp.get("id")
    if not view_id:
        print(f"  {FAIL} Failed to create test view: {resp}")
        sys.exit(1)
    print(f"  {PASS} Created test view: {view_id}")
    return view_id, "sheet-test"


def delete_test_view(view_id):
    api("DELETE", f"/views/{view_id}")
    print(f"  Cleaned up test view: {view_id}")


def get_view(view_id):
    return api("GET", f"/views/{view_id}")


def get_view_data(view_id, sheet_id, component_id=None):
    params = {"sheet": sheet_id}
    if component_id:
        params["component"] = component_id
    return api("GET", f"/views/{view_id}/data", params=params)


def make_progressive_csv(row_count=400):
    rows = [
        ("John Smith", "john@example.com", "ABC Realty", "broker", "new"),
        ("Jane Doe", "jane@example.com", "XYZ Properties", "owner", "new"),
        ("Bob Wilson", "bob@example.com", "123 Homes", "broker", "contacted"),
        ("Alice Brown", "alice@example.com", "Sunset Realty", "owner", "responded"),
    ]
    lines = ["name,email,phone,company,role,status"]
    for i in range(row_count):
        name, email, company, role, status = rows[i % len(rows)]
        lines.append(
            ",".join([
                f"{name} {i}",
                f"{i}_{email}",
                f"555-{1000 + i:04d}",
                company,
                role,
                status,
            ])
        )
    return "\n".join(lines)


def test_csv_import(view_id, sheet_id):
    print("\n=== Test 1: CSV Import (basic) ===")

    print("  Uploading test CSV...")
    filename = f"test-{uuid.uuid4().hex[:8]}.csv"
    file_path = upload_file(filename, TEST_CSV)
    if not file_path:
        return False

    print(f"  {PASS} Uploaded to {file_path}")

    print("  Importing CSV into sheet...")
    resp = api("POST", f"/views/{view_id}/sheets/{sheet_id}/import", {"file_path": file_path})

    if "error" in str(resp).lower() and "row_count" not in resp:
        print(f"  {FAIL} Import failed: {resp}")
        return False

    row_count = resp.get("row_count", 0)
    col_count = resp.get("column_count", 0)
    new_cols = resp.get("new_columns", [])
    parse_errors = resp.get("parse_errors", [])

    print(f"  {PASS} Import succeeded: {row_count} rows, {col_count} columns")
    if new_cols:
        print(f"  {PASS} Auto-created columns: {new_cols}")
    if parse_errors:
        print(f"  ⚠ Parse errors: {parse_errors}")

    if row_count != 4:
        print(f"  {FAIL} Expected 4 rows, got {row_count}")
        return False

    print("  Verifying view definition has columns...")
    view = get_view(view_id)
    defn = view.get("definition", {})
    sheets = defn.get("sheets", [])
    target_sheet = next((s for s in sheets if s["id"] == sheet_id), None)
    if not target_sheet:
        print(f"  {FAIL} Sheet {sheet_id} not found in view definition")
        return False

    table_comp = next((c for c in target_sheet.get("components", []) if c.get("type") == "table"), None)
    if not table_comp:
        print(f"  {FAIL} No table component found")
        return False

    config_cols = table_comp.get("config", {}).get("columns", [])
    col_keys = {c.get("key") for c in config_cols}
    expected_keys = {"name", "email", "phone", "company", "role", "status"}

    missing = expected_keys - col_keys
    if missing:
        print(f"  {FAIL} Missing columns in view definition: {missing}")
        return False
    print(f"  {PASS} All expected columns present in view definition: {col_keys}")

    print("  Verifying data resolves correctly...")
    table_component_id = table_comp.get("id")
    data = get_view_data(view_id, sheet_id, table_component_id)
    rows = data.get("rows", [])
    if len(rows) < 4:
        print(f"  {FAIL} Expected at least 4 rows in resolved data, got {len(rows)}")
        return False
    print(f"  {PASS} Resolved {len(rows)} rows from data endpoint")

    return True


def test_json_import(view_id, sheet_id):
    print("\n=== Test 2: JSON Import ===")

    ensure_sheet(view_id, "sheet-properties", "Properties", "table-properties", "Properties")
    print(f"  {PASS} Properties sheet ready")

    print("  Uploading test JSON...")
    filename = f"test-{uuid.uuid4().hex[:8]}.json"
    file_path = upload_file(filename, TEST_JSON, content_type="application/json")
    if not file_path:
        return False
    print(f"  {PASS} Uploaded to {file_path}")

    print("  Importing JSON into properties sheet...")
    resp = api("POST", f"/views/{view_id}/sheets/sheet-properties/import", {"file_path": file_path})

    if "error" in str(resp).lower() and "row_count" not in resp:
        print(f"  {FAIL} Import failed: {resp}")
        return False

    row_count = resp.get("row_count", 0)
    new_cols = resp.get("new_columns", [])
    print(f"  {PASS} Import succeeded: {row_count} rows")
    if new_cols:
        print(f"  {PASS} Auto-created columns: {new_cols}")

    if row_count != 3:
        print(f"  {FAIL} Expected 3 rows, got {row_count}")
        return False

    print("  Verifying view definition has JSON-derived columns...")
    view = get_view(view_id)
    defn = view.get("definition", {})
    prop_sheet = find_sheet(defn, "sheet-properties")
    if not prop_sheet:
        print(f"  {FAIL} Properties sheet not found")
        return False

    table_comp = find_table_component(prop_sheet)
    config_cols = table_comp.get("config", {}).get("columns", []) if table_comp else []
    col_keys = {c.get("key") for c in config_cols}
    expected_keys = {"address", "market", "sq_ft", "rent", "status"}

    missing = expected_keys - col_keys
    if missing:
        print(f"  {FAIL} Missing columns: {missing}")
        return False
    print(f"  {PASS} All expected columns present: {col_keys}")

    print("  Verifying resolved JSON rows...")
    data = get_view_data(view_id, "sheet-properties", table_comp.get("id") if table_comp else None)
    rows = data.get("rows", [])
    if len(rows) < 3:
        print(f"  {FAIL} Expected at least 3 rows in resolved data, got {len(rows)}")
        return False
    print(f"  {PASS} Resolved {len(rows)} JSON rows")

    return True


def test_lre_csv_import(view_id, sheet_id):
    """Test large-column CSV import with column capping."""
    print("\n=== Test 3: LRE Pilot CSV (large column set) ===")

    ensure_sheet(view_id, "sheet-lre", "LRE Properties", "table-lre", "LRE Properties")
    print(f"  {PASS} LRE sheet ready")

    print("  Uploading LRE CSV...")
    filename = f"lre-{uuid.uuid4().hex[:8]}.csv"
    file_path = upload_file(filename, LRE_CSV)
    if not file_path:
        return False
    print(f"  {PASS} Uploaded to {file_path}")

    print("  Importing LRE CSV...")
    resp = api("POST", f"/views/{view_id}/sheets/sheet-lre/import", {"file_path": file_path})

    if "error" in str(resp).lower() and "row_count" not in resp:
        print(f"  {FAIL} Import failed: {resp}")
        return False

    row_count = resp.get("row_count", 0)
    col_count = resp.get("column_count", 0)
    new_cols = resp.get("new_columns", [])

    print(f"  {PASS} Import succeeded: {row_count} rows, {col_count} total columns mapped")
    if new_cols:
        print(f"  {PASS} Columns added to view definition: {len(new_cols)} (capped from {col_count})")

    if row_count != 3:
        print(f"  {FAIL} Expected 3 rows, got {row_count}")
        return False

    view = get_view(view_id)
    defn = view.get("definition", {})
    lre_sheet = find_sheet(defn, "sheet-lre")
    table_comp = find_table_component(lre_sheet)
    config_cols = table_comp.get("config", {}).get("columns", []) if table_comp else []

    if len(config_cols) < 4:
        print(f"  {FAIL} Expected at least 4 columns, got {len(config_cols)}")
        return False
    print(f"  {PASS} View definition has {len(config_cols)} columns")

    key_cols = {c.get("key") for c in config_cols}
    core_keys = {"property_address", "city", "state", "zip"}
    found = core_keys & key_cols
    if len(found) < 3:
        print(f"  {FAIL} Expected core columns (address/city/state/zip) in visible set, found: {found}")
        return False
    print(f"  {PASS} Core columns present in visible set: {found}")

    print("  Verifying data resolves...")
    data = get_view_data(view_id, "sheet-lre", table_comp.get("id") if table_comp else None)
    rows = data.get("rows", [])
    if len(rows) < 3:
        print(f"  {FAIL} Expected at least 3 rows in resolved data, got {len(rows)}")
        return False
    print(f"  {PASS} Resolved {len(rows)} rows")

    return True


def test_large_import_and_reimport(view_id):
    print("\n=== Test 4: Large import + re-import idempotency ===")

    sheet_id = "sheet-progressive"
    component_id = "table-progressive"
    expected_rows = 400

    ensure_sheet(view_id, sheet_id, "Large Import", component_id, "Large Import")
    print(f"  {PASS} Sheet ready")

    filename = f"large-{uuid.uuid4().hex[:8]}.csv"
    file_path = upload_file(filename, make_progressive_csv(expected_rows))
    if not file_path:
        return False
    print(f"  {PASS} Uploaded to {file_path}")

    resp = api("POST", f"/views/{view_id}/sheets/{sheet_id}/import", {"file_path": file_path}, False, None, 120)
    if "error" in str(resp).lower() and "row_count" not in resp:
        print(f"  {FAIL} Import failed: {resp}")
        return False

    row_count = resp.get("row_count", 0)
    if row_count != expected_rows:
        print(f"  {FAIL} Expected {expected_rows} rows, got {row_count}")
        return False
    print(f"  {PASS} First import: {row_count} rows")

    data = get_view_data(view_id, sheet_id, component_id)
    resolved = len(data.get("rows", []))
    if resolved != expected_rows:
        print(f"  {FAIL} Expected {expected_rows} resolved rows, got {resolved}")
        return False
    print(f"  {PASS} Resolved {resolved} rows")

    filename2 = f"large-reimport-{uuid.uuid4().hex[:8]}.csv"
    file_path2 = upload_file(filename2, make_progressive_csv(expected_rows))
    if not file_path2:
        return False

    resp2 = api("POST", f"/views/{view_id}/sheets/{sheet_id}/import", {"file_path": file_path2}, False, None, 120)
    row_count2 = resp2.get("row_count", 0)
    if row_count2 != expected_rows:
        print(f"  {FAIL} Re-import expected {expected_rows} rows, got {row_count2}")
        return False

    data2 = get_view_data(view_id, sheet_id, component_id)
    resolved2 = len(data2.get("rows", []))
    if resolved2 != expected_rows:
        print(f"  {FAIL} Re-import expected {expected_rows} resolved rows, got {resolved2} (duplicates?)")
        return False
    print(f"  {PASS} Re-import idempotent: still {resolved2} rows (no duplicates)")

    return True


def test_sheet_switching(view_id, sheet_id):
    """Verify that switching between sheets preserves data — the core 'data disappearing' bug."""
    print("\n=== Test 5: Sheet switching preserves data ===")

    print("  Fetching contacts sheet data...")
    contacts_view = get_view(view_id)
    contacts_defn = contacts_view.get("definition", {})
    contacts_sheet = find_sheet(contacts_defn, sheet_id)
    contacts_table = find_table_component(contacts_sheet)
    data1 = get_view_data(view_id, sheet_id, contacts_table.get("id") if contacts_table else None)
    rows1 = data1.get("rows", [])
    if len(rows1) == 0:
        print(f"  {FAIL} Contacts sheet has no rows before switch")
        return False
    print(f"  {PASS} Contacts sheet: {len(rows1)} rows")

    print("  Switching to properties sheet...")
    data2 = get_view_data(view_id, "sheet-properties", "table-properties")
    rows2 = data2.get("rows", [])
    print(f"  Properties sheet: {len(rows2)} rows")

    print("  Switching to LRE sheet...")
    data3 = get_view_data(view_id, "sheet-lre", "table-lre")
    rows3 = data3.get("rows", [])
    print(f"  LRE sheet: {len(rows3)} rows")

    print("  Switching back to contacts sheet...")
    data1_after = get_view_data(view_id, sheet_id, contacts_table.get("id") if contacts_table else None)
    rows1_after = data1_after.get("rows", [])

    if len(rows1_after) != len(rows1):
        print(f"  {FAIL} Contacts sheet row count changed after switching: {len(rows1)} -> {len(rows1_after)}")
        return False
    print(f"  {PASS} Contacts sheet still has {len(rows1_after)} rows after switching")

    print("  Switching back to LRE sheet...")
    data3_after = get_view_data(view_id, "sheet-lre", "table-lre")
    rows3_after = data3_after.get("rows", [])

    if len(rows3_after) != len(rows3):
        print(f"  {FAIL} LRE sheet row count changed after switching: {len(rows3)} -> {len(rows3_after)}")
        return False
    print(f"  {PASS} LRE sheet still has {len(rows3_after)} rows after switching")

    return True


def main():
    parser = argparse.ArgumentParser(description="E2E test for CSV/JSON import")
    parser.add_argument("--view-id", help="Existing view ID to test against")
    parser.add_argument("--sheet-id", help="Sheet ID within the view")
    args = parser.parse_args()

    created = False
    if args.view_id:
        view_id = args.view_id
        sheet_id = args.sheet_id or "sheet-1"
    else:
        view_id, sheet_id = create_test_view()
        created = True

    results = []
    try:
        results.append(("CSV Import", test_csv_import(view_id, sheet_id)))
        results.append(("JSON Import", test_json_import(view_id, sheet_id)))
        results.append(("LRE CSV Import", test_lre_csv_import(view_id, sheet_id)))
        results.append(("Large Import + Re-import", test_large_import_and_reimport(view_id)))
        results.append(("Sheet Switching", test_sheet_switching(view_id, sheet_id)))
    finally:
        if created:
            delete_test_view(view_id)

    print("\n=== Results ===")
    all_passed = True
    for name, passed in results:
        status = PASS if passed else FAIL
        print(f"  {status} {name}")
        if not passed:
            all_passed = False

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
