"""Debug: check HeaderItem bool behavior."""
import sys
sys.path.insert(0, "dlis_builder/src")

import lasio, warnings, textwrap, tempfile

content = textwrap.dedent("""\
    ~VERSION INFORMATION
     VERS.                          2.0 : CWLS LOG ASCII STANDARD-VERSION 2.0
     WRAP.                           NO : ONE LINE PER DEPTH STEP
    ~WELL INFORMATION
     WELL.                      WELL_A  : Well Name
     NULL.                     -999.25  : Null Value
     COMP.                 Acme Energy  :
     FLD .                NORTH_BLOCK   :
    ~CURVE INFORMATION
     DEPT .M                           : Depth
     GR   .GAPI                        : Gamma Ray
    ~A
     300.00  45.2
     300.15  47.8
""")

with tempfile.NamedTemporaryFile(suffix=".las", mode="w", delete=False) as f:
    f.write(content)
    fname = f.name

from pathlib import Path
raw_text = Path(fname).read_text(encoding="utf-8", errors="replace")
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    las = lasio.read(raw_text, engine="normal")

item = las.well.get("WELL")
print(f"item = {item!r}")
print(f"type(item) = {type(item)}")
print(f"bool(item) = {bool(item)}")
print(f"item.value = {item.value!r}")
print(f"type(item.value) = {type(item.value)}")
print(f"bool(item.value) = {bool(item.value)}")
print(f"item and item.value = {item and item.value!r}")

# Direct _well simulation
def _well(key, default=""):
    i = las.well.get(key)
    return str(i.value).strip() if i and i.value else default

print(f"_well('WELL', 'UNKNOWN') = {_well('WELL', 'UNKNOWN')!r}")
print(f"_well('COMP', 'UNKNOWN') = {_well('COMP', 'UNKNOWN')!r}")

# Now test with the actual converter
from dlis_builder.converters import LASConverter
ds = LASConverter().read(fname)
print(f"\nLASConverter well_name = {ds.metadata.well_name!r}")
