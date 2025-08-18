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

def find_read_table_usage(txt: str):
    matches = []
    for m in READ_TABLE_RE.finditer(txt or ""):
        itab = m.group("itab")
        keys_raw = m.group("keys")

        # Extract fields from WITH KEY clause
        fields = re.findall(r"(\w+)\s*=", keys_raw, re.IGNORECASE)

        matches.append({
            "table": itab,
            "target_type": "READ_TABLE",
            "target_name": itab,
            "start_char_in_unit": m.start(),
            "end_char_in_unit": m.end(),
            "used_fields": [f.upper() for f in fields],
            "ambiguous": False,
            "suggested_statement": f"SORT {itab} BY {', '.join(f.upper() for f in fields)}." if fields else None,
            "suggested_fields": [f.upper() for f in fields] if fields else None,
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
