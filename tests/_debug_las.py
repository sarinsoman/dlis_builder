"""Debug script for LAS metadata parsing."""
import sys
sys.path.insert(0, "dlis_builder/src")

import lasio, warnings, textwrap, tempfile
from pathlib import Path

# Monkeypatch to trace _well() inside the converter
import dlis_builder.converters.las as las_mod

original_read = las_mod.LASConverter.read

def patched_read(self, source, **kwargs):
    las_path = str(Path(source).resolve())
    raw_text = Path(las_path).read_text(encoding="utf-8", errors="replace")
    modified, bmap = las_mod._preprocess_bracket_arrays(raw_text)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        las = lasio.read(modified, engine="normal")
    print("DEBUG well keys:", list(las.well.keys()))
    for k in ["WELL", "COMP", "FLD"]:
        item = las.well.get(k)
        print(f"DEBUG {k!r}: item={item!r}, value={getattr(item,'value',None)!r}")
    return original_read(self, source, **kwargs)

las_mod.LASConverter.read = patched_read

from dlis_builder.converters import LASConverter

content = textwrap.dedent("""\
    ~VERSION INFORMATION
     VERS.                          2.0 : CWLS LOG ASCII STANDARD-VERSION 2.0
     WRAP.                           NO : ONE LINE PER DEPTH STEP
    ~WELL INFORMATION
     WELL.                      WELL_A  : Well Name
     NULL.                     -999.25  : Null Value
     STRT.M                     300.00  :
     STOP.M                     300.60  :
     STEP.M                       0.15  :
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

ds = LASConverter().read(fname)
print("well_name:", repr(ds.metadata.well_name))
print("company:", repr(ds.metadata.company))


# Also test numbered suffix detection
content2 = textwrap.dedent("""\
    ~VERSION INFORMATION
     VERS. 2.0 :
     WRAP.  NO :
    ~WELL INFORMATION
     WELL.  TEST_WELL  :
     NULL.  -999.25    :
    ~CURVE INFORMATION
     DEPT .M  :
     AMP01 .MV                           : Amplitude
     AMP02 .MV                           : Amplitude
     AMP03 .MV                           : Amplitude
     AMP04 .MV                           : Amplitude
     AMP05 .MV                           : Amplitude
     AMP06 .MV                           : Amplitude
     AMP07 .MV                           : Amplitude
     AMP08 .MV                           : Amplitude
    ~A
     300.00  0.500  0.510  0.520  0.530  0.540  0.550  0.560  0.570
     300.15  0.510  0.520  0.530  0.540  0.550  0.560  0.570  0.580
     300.30  0.520  0.530  0.540  0.550  0.560  0.570  0.580  0.590
     300.45  0.530  0.540  0.550  0.560  0.570  0.580  0.590  0.600
     300.60  0.540  0.550  0.560  0.570  0.580  0.590  0.600  0.610
""")

with tempfile.NamedTemporaryFile(suffix=".las", mode="w", delete=False) as f:
    f.write(content2)
    fname2 = f.name

print("\n--- Array detection ---")
ds2 = LASConverter().read(fname2)
array_channels = [c for c in ds2.channels if c.is_array]
print("Array channels:", [(c.name, c.array_size) for c in array_channels])
all_channels = [(c.name, c.is_array) for c in ds2.channels]
print("All channels:", all_channels)
