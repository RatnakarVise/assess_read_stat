from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import re
import json

app = FastAPI(title="ABAP READ TABLE Remediator")

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""

# Regex to capture READ TABLE statements
READ_TABLE_RE = re.compile(
    r"READ\s+TABLE\s+(?P<itab>\w+).*?WITH\s+KEY\s+(?P<keys>.+?)(?:\.|\n)",
    re.IGNORECASE | re.DOTALL,
)

# Regex to capture SORT statements
SORT_RE = re.compile(
    r"SORT\s+(?P<itab>\w+)\s+BY\s+(?P<fields>.+?)(?:\.|\n)",
    re.IGNORECASE | re.DOTALL,
)

def extract_sort_statements(txt: str):
    """Find all SORT statements and map them by table"""
    sort_map = {}
    for m in SORT_RE.finditer(txt or ""):
        itab = m.group("itab").upper()
        fields_raw = m.group("fields")
        fields = [f.strip().upper() for f in re.split(r"[ ,]+", fields_raw) if f.strip()]
        sort_map[itab] = fields
    return sort_map

def fields_match(sort_fields: List[str], key_fields: List[str]) -> bool:
    """
    Return True if sort_fields covers all key_fields in the same order.
    Example:
        sort_fields = ["MATNR", "MATKL"]
        key_fields  = ["MATNR", "MATKL"]  -> True
        sort_fields = ["MATNR"] , key_fields = ["MATNR","MATKL"] -> False
    """
    if not sort_fields or not key_fields:
        return False

    if len(sort_fields) < len(key_fields):
        return False

    # check prefix order
    return sort_fields[:len(key_fields)] == key_fields

def find_read_table_usage(txt: str):
    matches = []
    sort_map = extract_sort_statements(txt)

    for m in READ_TABLE_RE.finditer(txt or ""):
        itab = m.group("itab")
        keys_raw = m.group("keys")

        # Extract fields from WITH KEY clause
        fields = re.findall(r"(\w+)\s*=", keys_raw, re.IGNORECASE)
        fields = [f.upper() for f in fields]

        sort_fields = sort_map.get(itab.upper(), [])
        already_sorted = fields_match(sort_fields, fields)

        if not already_sorted:
            matches.append({
                "table": itab,
                "target_type": "READ_TABLE",
                "target_name": itab,
                "start_char_in_unit": m.start(),
                "end_char_in_unit": m.end(),
                "used_fields": fields,
                "ambiguous": False,
                "suggested_statement": f"SORT {itab} BY {', '.join(fields)}." if fields else None,
                "suggested_fields": fields if fields else None,
            })

    return matches

@app.post("/remediate-read-table")
def remediate_read_table(units: List[Unit]):
    results = []
    for u in units:
        src = u.code or ""
        metadata = find_read_table_usage(src)

        obj = json.loads(u.model_dump_json())
        obj["read_table_usage"] = metadata
        results.append(obj)
    return results
