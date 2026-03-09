# dlis-builder

Python library for creating **DLIS** (Digital Log Interchange Standard) files from any data source — LAS files, CSV files, databases, REST APIs, or any in-memory dataset.

---

## Table of Contents

1. [Installation](#1-installation)
2. [Prerequisites](#2-prerequisites)
3. [LAS → DLIS](#3-las--dlis)
   - [Minimal conversion](#31-minimal-conversion)
   - [Metadata override](#32-metadata-override)
   - [Array / image channels](#33-array--image-channels)
   - [Instance-level array map](#34-instance-level-array-map)
   - [Batch conversion](#35-batch-conversion)
   - [CLI](#36-cli)
4. [CSV → DLIS](#4-csv--dlis)
   - [Auto-detection mode](#41-auto-detection-mode)
   - [Explicit configuration](#42-explicit-configuration)
   - [Column whitelist](#43-column-whitelist)
   - [Array grouping](#44-array-grouping)
   - [Batch conversion](#45-batch-conversion)
5. [Programmatic builder](#5-programmatic-builder)
   - [Fluent API](#51-fluent-api)
   - [DLISBuilder.from_las()](#52-dlisbuilderfrom_las)
   - [DLISBuilder.from_csv()](#53-dlisbuilderfrom_csv)
   - [DataSource protocol](#54-datasource-protocol)
   - [DLISFileConfig / OriginConfig](#55-dlisfileconfig--originconfig)
   - [Multi-frame export](#56-multi-frame-export)
   - [String / categorical channels](#57-string--categorical-channels)
6. [Configuration reference](#6-configuration-reference)
   - [WellMetadata](#61-wellmetadata)
   - [DLISFileConfig / OriginConfig](#62-dlisfileconfig--originconfig)
   - [ChannelDef](#63-channeldef)
   - [ParameterDef](#64-parameterdef)
   - [FrameDef](#65-framedef)
7. [Null / absent value handling](#7-null--absent-value-handling)
8. [Validation](#8-validation)
9. [⚠️ Developer pitfalls](#9-developer-pitfalls)
10. [Dependencies](#10-dependencies)

---

## 1. Installation

```bash
pip install dlis-builder               # core + programmatic builder only
pip install "dlis-builder[las]"        # + LAS converter  (requires: lasio)
pip install "dlis-builder[csv]"        # + CSV converter  (requires: pandas)
pip install "dlis-builder[all]"        # all converters
pip install "dlis-builder[dev]"        # all converters + test/lint/type-check tools
```

The library is MIT-licensed.  See [LICENSE](LICENSE), [CHANGELOG](CHANGELOG.md), and [CONTRIBUTING](CONTRIBUTING.md) for release history and contribution guidelines.  A GitHub Actions CI workflow (`ruff` + `mypy` + `pytest` across Python 3.10–3.13) runs on every pull request.

---

## 2. Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10+ | 3.12 / 3.14 tested |
| [dliswriter](https://github.com/well-log/dliswriter) | 1.2+ | Core DLIS writing engine |
| numpy | 1.24+ | Array handling |
| lasio | 0.32+ | Only for `LASConverter` |
| pandas | 2.0+ | Only for `CSVConverter` |

---

## 3. LAS → DLIS

### 3.1 Minimal conversion

```python
from dlis_builder.converters import LASConverter

# Metadata is read from the LAS ~Well section.
# image_resolution defaults to "auto" (see §3.3).
path = LASConverter().convert("well.las", "well.dlis")
print(path)   # → /abs/path/to/well.dlis
```

### 3.2 Metadata override

Override or supplement LAS metadata from an external system (database, API):

```python
from dlis_builder.converters import LASConverter
from dlis_builder.models import WellMetadata

path = LASConverter().convert(
    "well.las",
    "well.dlis",
    metadata=WellMetadata(
        well_name="WELL_A",
        company="Acme Energy",
        field_name="NORTH BLOCK",
        null_value=-999.25,
    ),
)
```

### 3.3 Array / image channels

LAS 3.0 files and many LAS 2.0 files encode multi-element (array/image) channels using several conventions.  `LASConverter` handles all of them automatically:

| Convention | Example columns | How detected |
|---|---|---|
| Bracket notation | `FMI[1]`, `FMI[2]`, … | Pattern `NAME[N]` |
| Numbered suffix  | `PAD_001`, `PAD_002`, … | Suffix `_NNN` or `.NNN` |
| Explicit map     | provided via `array_map` | User-supplied |

```python
# Auto-detect arrays; "auto" resolution (the default) picks float32 for wide
# image channels and float64 for narrow ones.
path = LASConverter().convert("borehole_image.las", "image.dlis")

# Force high precision for all array channels
path = LASConverter(image_resolution="high").convert(
    "borehole_image.las", "image.dlis",
)

# Explicitly map columns at call time
path = LASConverter().convert(
    "borehole_image.las",
    "image.dlis",
    array_map={"IMAGE": [f"PAD_{i:03d}" for i in range(1, 129)]},
)
```

`image_resolution` controls the DLIS `REPRESENTATION-CODE` for array / image channels only.  Scalar channels are always `float64`.

| `image_resolution` | dtype | When to use |
|---|---|---|
| `"auto"` *(default)* | float32 if dimension ≥ 32, else float64 | Best default; balances file size and precision |
| `"low"` | float32 (4 B/element) | Baker Hughes / Techlog FMI/XRMI native encoding |
| `"high"` | float64 (8 B/element) | Spectral or acoustic data requiring full precision |

### 3.4 Instance-level array map

Set an `array_map` on the converter instance so every call to `read()` and `convert()` uses it automatically — no need to repeat the map on each call:

```python
# Applies to every conversion made with this instance
conv = LASConverter(
    image_resolution="low",
    array_map={"IMAGE": [f"PAD_{i:03d}" for i in range(1, 129)]},
)
conv.convert("run1.las", "run1.dlis")
conv.convert("run2.las", "run2.dlis")

# Per-call array_map is merged on top (per-call takes priority)
conv.convert("run3.las", "run3.dlis", array_map={"EXTRA": ["A", "B", "C"]})
```

### 3.5 Batch conversion

```python
results = LASConverter(image_resolution="auto").convert_batch(
    "/data/las/",
    output_dir="/data/dlis/",
    recursive=True,
    max_workers=4,    # tune to your disk I/O; keep low for large image files
)

for r in results:
    if r["success"]:
        print("OK  ", r["dlis_path"])
    else:
        print("ERR ", r["las_path"], "→", r["error"])
```

### 3.6 CLI

```bash
# Single file
dlis-builder well.las
dlis-builder well.las -o well.dlis --resolution high

# Batch
dlis-builder /data/las_files/ -o /data/dlis/ --recursive

# With explicit array grouping
dlis-builder image.las -o image.dlis \
    --array-map "IMAGE:PAD_001,PAD_002,PAD_003"

# Override well / company metadata
dlis-builder well.las --well "WELL_A" --company "Acme"
```

---

## 4. CSV → DLIS

### 4.1 Auto-detection mode

When `depth_column` and/or `unit_row` are not specified, `CSVConverter` calls `detect_csv_layout()` automatically:

```python
from dlis_builder.converters import CSVConverter

# Both depth column and unit row auto-detected
ds = CSVConverter().read("data.csv")
```

`detect_csv_layout` inspects the first 10 rows of the file:
- **Depth column** — column whose header matches a known mnemonic (`DEPTH`, `DEPT`, `MD`, `TVD`, etc.)
- **Unit row** — the first data row after the header where ≥ 50 % of cells look like physical units (`m`, `ft`, `gAPI`, `ohm.m`, …)

Inspect what is detected before converting:

```python
from dlis_builder import detect_csv_layout

layout = detect_csv_layout("petrel_export.csv")
print(layout)
# → CSVLayout(depth_column='MD', unit_row=1, first_data_row=2,
#             detected_units={'MD': 'm', 'GR': 'gAPI', 'RHOB': 'g/cm3'},
#             detection_confidence='high')
```

### 4.2 Explicit configuration

```python
from dlis_builder.converters import CSVConverter
from dlis_builder.models import WellMetadata

path = CSVConverter(
    depth_column="DEPTH",
    column_units={"DEPTH": "m", "GR": "gAPI", "RHOB": "g/cm3"},
    null_value=-9999.0,
).convert(
    "data.csv",
    "output.dlis",
    metadata=WellMetadata(well_name="WELL_A", company="Acme"),
)
```

Disable unit-row detection entirely:

```python
CSVConverter(depth_column="DEPTH", unit_row=-1)
```

### 4.3 Column whitelist

Use `columns` to include only a specific subset of CSV columns in the output.  The depth / index column is always included regardless.

```python
# Only depth + GR are written; all other columns are ignored
conv = CSVConverter(
    depth_column="DEPTH",
    columns=["GR"],
)
ds = conv.read("data.csv")
```

Columns listed in `columns` that do not exist in the CSV are silently skipped and a warning is logged.

### 4.4 Array grouping

Group multiple CSV columns into a single multi-element DLIS channel:

```python
image_cols = [f"IMG_{i:03d}" for i in range(1, 129)]

path = CSVConverter(
    depth_column="DEPTH",
    array_columns={"IMAGE": image_cols},
).convert("image_data.csv", "image.dlis")
```

### 4.5 Batch conversion

```python
results = CSVConverter(depth_column="DEPTH").convert_batch(
    "/data/csv/",
    output_dir="/data/dlis/",
    recursive=False,
    max_workers=4,
    metadata=WellMetadata(well_name="FIELD_EXPORT"),
)
```

---

## 5. Programmatic builder

### 5.1 Fluent API

Build a DLIS file entirely from in-memory data — no LAS or CSV file required:

```python
import numpy as np
from dlis_builder import DLISBuilder
from dlis_builder.models import WellMetadata, ChannelDef, DLISFileConfig, ParameterDef, Resolution

depth = np.linspace(300.0, 1200.0, 6001)           # (N,)
gr    = np.random.uniform(20, 120, 6001)            # (N,)
image = np.random.uniform(0.1, 10.0, (6001, 120))  # (N, elements)

path = (
    DLISBuilder()
    .set_origin(WellMetadata(well_name="WELL_A", company="Acme", field_name="NORTH"))
    .set_file_config(DLISFileConfig(producer_name="Acme", file_type="WELL-LOG"))
    .add_parameter(ParameterDef.numeric("BHT",  85.0,  unit="degC",  description="Bottom Hole Temp"))
    .add_parameter(ParameterDef.text  ("TOOL", "FMI",               description="Imaging Tool"))
    .add_channel(ChannelDef(name="DEPT",  unit="m",     data=depth))
    .add_channel(ChannelDef(name="GR",    unit="gAPI",  data=gr))
    .add_channel(ChannelDef(name="IMAGE", unit="ohm.m", data=image,
                            dimension=[120], resolution=Resolution.LOW))
    .build("output.dlis")
)
```

### 5.2 DLISBuilder.from_las()

Read a LAS file and return a pre-populated builder, ready to customise or build immediately:

```python
# Defaults: image_resolution="auto", no array_map, flatten_arrays=False
builder = DLISBuilder.from_las("well.las")
builder.set_origin(WellMetadata(well_name="OVERRIDE")).build("out.dlis")

# With explicit options
builder = DLISBuilder.from_las(
    "borehole_image.las",
    image_resolution="low",
    array_map={"IMAGE": [f"PAD_{i:03d}" for i in range(1, 129)]},
    flatten_arrays=False,
)
builder.build("image.dlis")
```

| Parameter | Default | Notes |
|---|---|---|
| `image_resolution` | `"auto"` | `"low"`, `"high"`, or `"auto"` |
| `array_map` | `None` | Per-call explicit grouping map |
| `flatten_arrays` | `False` | Explode array channels into scalar columns |

### 5.3 DLISBuilder.from_csv()

```python
builder = DLISBuilder.from_csv(
    "data.csv",
    depth_column="DEPTH",
    column_units={"DEPTH": "m", "GR": "gAPI"},
    columns=["GR", "RHOB"],          # optional column whitelist
    metadata=WellMetadata(well_name="WELL_B"),
)
builder.build("out.dlis")
```

### 5.4 DataSource protocol

Integrate any database or API object without subclassing — just implement three methods:

```python
from dlis_builder import DLISBuilder
from dlis_builder.models import WellMetadata, ChannelDef, ParameterDef

class WellRepository:
    """Adapter for your database layer."""

    def __init__(self, db_conn, well_id: int) -> None:
        self._db = db_conn
        self._well_id = well_id

    def get_metadata(self) -> WellMetadata:
        row = self._db.query("SELECT * FROM wells WHERE id = ?", self._well_id)
        return WellMetadata.from_dict(row)

    def get_channels(self) -> list[ChannelDef]:
        ...  # query channel data, return list of ChannelDef

    def get_parameters(self) -> list[ParameterDef]:
        ...

path = DLISBuilder.from_source(WellRepository(db, well_id=42)).build("output.dlis")
```

### 5.5 DLISFileConfig / OriginConfig

Control the DLIS Origin record (producer info, file type, frame name):

```python
from dlis_builder.models import DLISFileConfig

cfg = DLISFileConfig(
    producer_name="Acme Logging Ltd",   # your company / org name
    product_name="WellExport",          # your application name
    version="2.0.0",                    # your application version
    file_type="LAS CONVERSION",         # or "CALIBRATED", "PROCESSED", "WELL-LOG"
    frame_name="MAIN-FRAME",
)

# Or build from a dict (API payload, app config, environment variables, etc.)
cfg = DLISFileConfig.from_dict(app_settings)   # unknown keys silently ignored

builder.set_file_config(cfg)
```

`DLISFileConfig` is an alias for `OriginConfig` — they are the same class.

### 5.6 Multi-frame export

A DLIS logical file can contain multiple frames (RP 66 V1 §5.7).  When you add channels with **different row counts** — for example, scalar picks sampled at 14 000 depths alongside a borehole image sampled at 300 000 depths — the builder automatically groups them into separate frames within the **same output file**.

```python
import numpy as np
from dlis_builder import DLISBuilder
from dlis_builder.models import ChannelDef

# ── Scalar frame (14 117 samples) ───────────────────────────────────────────
scalar_depth = np.linspace(0.0, 16800.0, 14117)   # ft
gr           = np.random.uniform(20, 150, 14117).astype(np.float32)
lith_type    = np.array(["Sedimentary"] * 14117, dtype=object)

# ── Image frame (315 066 samples) ───────────────────────────────────────────
image_depth  = np.linspace(0.0, 16800.0, 315066)  # ft
features_map = np.random.uniform(0, 10, (315066, 22)).astype(np.float32)

path = (
    DLISBuilder()
    # scalar frame
    .add_channel(ChannelDef("DEPT",         unit="ft",       data=scalar_depth))
    .add_channel(ChannelDef("GR",           unit="gAPI",     data=gr))
    .add_channel(ChannelDef("LITH_TYPE",    unit="",         data=lith_type))
    # image frame — note: its own depth channel is required
    .add_channel(ChannelDef("DEPT_FMI",     unit="ft",       data=image_depth))
    .add_channel(ChannelDef("features_map", unit="unitless", data=features_map,
                            dimension=[22]))
    .build("combined.dlis")   # group_by_depth=True is the default
)
```

The grouping is performed by `build()` before writing.  Opt out with `build(group_by_depth=False)` to restore the pre-existing behaviour where mismatched row counts raise a `ValueError`.

| `group_by_depth` | Behaviour |
|---|---|
| `True` *(default)* | Channels with different row counts → separate DLIS Frames in one file |
| `False` | Raises `ValueError` if any two channels have different row counts |

**Rules for auto-grouping:**
- Each group must contain exactly one recognisable depth / index channel (matched by name or length unit — see §9.11).
- If no index channel is found for a group, the builder falls back to single-frame mode and `ValueError` is raised.
- Cross-frame channel name conflicts are resolved automatically: the second occurrence is renamed `<name>_F<n>` and a `WARNING` is logged.

**Manual multi-frame via `FrameDef`** — for full control, build `FrameDef` objects directly and pass them to `WellDataset`:

```python
from dlis_builder.models import FrameDef, WellDataset, WellMetadata
from dlis_builder._writer import write_dlis

frame1 = FrameDef(name="SCALAR-FRAME", channels=[dept_ch, gr_ch, lith_ch])
frame2 = FrameDef(name="IMAGE-FRAME",  channels=[dept_fmi_ch, features_ch])

dataset = WellDataset(
    metadata=WellMetadata(well_name="WELL_A"),
    frames=[frame1, frame2],
)
write_dlis(dataset, "combined.dlis")
```

### 5.7 String / categorical channels

DLIS frame data is strictly numeric; the library handles string / categorical channels transparently by encoding them as `int32` category codes.  A companion DLIS Parameter `LABELS_<channel_name>` stores the lookup table so downstream consumers can decode the integers back to labels.

```python
lith   = np.array(["Sedimentary", "Igneous", None, "Sedimentary"], dtype=object)
f_type = np.array(["BHI-Vug-Iso-SE"] * 100, dtype=object)

builder
    .add_channel(ChannelDef(name="LITH",  unit="", data=lith))
    .add_channel(ChannelDef(name="F_TYPE", unit="", data=f_type))
```

**Encoding rules:**

| Input value | Encoded code |
|---|---|
| `None` | `0` |
| `float NaN` | `0` |
| Empty string after `.strip()` | `0` |
| First unique non-null label | `1` |
| Second unique non-null label | `2` |
| … | … |

The lookup Parameter value for `LITH` above would be `"0=<null>,1=Sedimentary,2=Igneous"`.

Detected automatically when `data.dtype.kind` is `'O'` (object), `'U'` (unicode), or `'S'` (bytes string).  No configuration required.

---

## 6. Configuration reference

### 6.1 WellMetadata

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `well_name` | str | `"UNKNOWN"` | Well identifier |
| `well_id` | str | `""` | UWI / API number |
| `company` | str | `"UNKNOWN"` | Operator / company |
| `field_name` | str | `"WILDCAT"` | Field name |
| `country` | str | `""` | ISO country code |
| `null_value` | float | `-999.25` | File-declared null sentinel |

Create from a dict (database row, API payload):

```python
WellMetadata.from_dict({"well_name": "A-01", "company": "Operator", "null_value": -9999.0})
```

### 6.2 DLISFileConfig / OriginConfig

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `producer_name` | str | `""` | Organisation writing the file — **always set this** |
| `product_name` | str | `"dlis-builder"` | Software / application name |
| `version` | str | `"1.0.0"` | Application version string |
| `file_type` | str | `"LAS CONVERSION"` | RP 66 V1 file type label; common values: `"CALIBRATED"`, `"PROCESSED"`, `"WELL-LOG"` |
| `frame_name` | str | `"MAIN-FRAME"` | Name of the primary DLIS Frame |
| `file_set_name` | str \| None | `None` | DLIS FILE-SET-NAME for multi-file sets |
| `file_set_number` | int \| None | `None` | Integer FILE-SET-NUMBER |
| `file_number` | int \| None | `None` | Integer FILE-NUMBER within set |

### 6.3 ChannelDef

| Parameter | Type | Required | Notes |
|---|---|---|---|
| `name` | str | ✓ | DLIS channel mnemonic (max 256 chars, ASCII printable) |
| `data` | ndarray | ✓ | Shape `(N,)` for scalar/string; `(N, M)` for array/image |
| `unit` | str | | Physical unit string (normalised at write time); use `""` for categorical/unitless channels |
| `long_name` | str | | Descriptive label for viewers; defaults to `name` |
| `dimension` | list[int] | | `[M]` for array channels; inferred from `data.shape` if omitted |
| `resolution` | Resolution | | `Resolution.HIGH` (float64) or `Resolution.LOW` (float32); only applies to array channels |

The first channel added is always the depth (index) channel.

**String / categorical channels** — pass a numpy array with `dtype=object`, `dtype='U…'`, or `dtype='S…'`.  The library detects these automatically via `ChannelDef.is_string` and encodes them as `int32` category codes (DLIS frame data cannot store text directly).  A companion DLIS Parameter named `LABELS_<channel_name>` is written with the lookup table in `"0=<null>,1=Label1,2=Label2,…"` format.

```python
types = np.array(["Sedimentary", "Igneous", None, "Sedimentary"], dtype=object)
ChannelDef(name="LITH", unit="", data=types)   # encoded automatically
```

### 6.4 ParameterDef

Parameters are written as named DLIS Parameter objects alongside curve data.

```python
# Numeric parameter — value stored as float
ParameterDef.numeric("BHT",  85.0,   unit="degC", description="Bottom Hole Temperature")

# Text / categorical parameter — value stored as str
ParameterDef.text("TOOL", "FMI-HD", description="Imaging tool")

# Direct construction — value may be str, float, or int
ParameterDef(name="DEPTH_DATUM", value=0.0, unit="m")
```

`ParameterDef.value` accepts `str`, `float`, or `int`.  The writer dispatches on type and writes the appropriate DLIS representation without guessing.

### 6.5 FrameDef

Represents a single DLIS Frame — a group of channels sharing the same depth axis.

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `name` | str | `"FRAME"` | Frame mnemonic written to the DLIS file |
| `channels` | list[ChannelDef] | `[]` | Channels in this frame; first element must be the index channel |

`FrameDef` is only needed when constructing `WellDataset` directly.  When using `DLISBuilder`, frames are created automatically by `build(group_by_depth=True)`.

```python
from dlis_builder.models import FrameDef

frame = FrameDef(
    name="MAIN-FRAME",
    channels=[dept_ch, gr_ch, rhob_ch],
)
```

---

## 7. Null / absent value handling

The library follows a **NaN-as-internal-sentinel** strategy.  Source null values are converted to `NaN` on read and written back as the DLIS absent value on write.

### LAS null masking

On every LAS read the following values are masked to `NaN`, regardless of the `NULL.` header value declared in the file:

| Value | Source / convention |
|---|---|
| File `NULL.` value | Whatever is declared in the `~Well` section |
| `-999.25` | CWLS / SPWLA standard |
| `-999.0` | Common legacy variant |
| `-9999.0` | Baker Hughes / Halliburton legacy |
| `-285.43` | Baker Hughes / Halliburton tool-family legacy |

Masking uses an absolute tolerance of `0.005` to handle floating-point representation drift in legacy acquisition systems while avoiding masking of real measurements close to the null.

### DLIS absent value

When writing, `NaN` values in channel data are replaced with the `null_value` from `WellMetadata` (default `-999.25`).  For DLIS consumers configured for `-9999`, set:

```python
WellMetadata(null_value=-9999.0)
```

---

## 8. Validation

Validation runs **automatically** every time `build()` or `convert()` is called.  You can also invoke validators explicitly for early feedback:

```python
from dlis_builder.utils.validation import (
    validate_metadata,
    validate_dataset,
    validate_channel_names,
    validate_csv_config,
    DLISMetadataWarning,
)
import warnings

# Promote metadata warnings to errors (useful in CI / strict mode)
warnings.filterwarnings("error", category=DLISMetadataWarning)

# Validate a dataset before building
validate_dataset(my_dataset)   # raises ValueError on critical problems
```

**What is validated:**

| Check | Scope | Behaviour |
|---|---|---|
| No channels | dataset | `ValueError` |
| NaN in index (depth) channel | per frame | `ValueError` |
| Channel length mismatch | per frame | `ValueError` |
| `dimension` inconsistent with `data.shape` | per frame | `ValueError` |
| Duplicate channel names | per frame | `ValueError` |
| `well_name` / `company` still `"UNKNOWN"` | dataset | `DLISMetadataWarning` |
| `null_value` is positive | dataset | `DLISMetadataWarning` |
| Channel name > 256 chars or non-ASCII | dataset | `ValueError` |
| `depth_column` missing from CSV | CSV only | `ValueError` |

When `dataset.frames` is non-empty (multi-frame mode), all per-frame checks run independently for each frame — channels in different frames are allowed to have different row counts.

---

## 9. ⚠️ Developer pitfalls

### 9.1 `dliswriter` API

The library uses [dliswriter](https://github.com/well-log/dliswriter) under the hood.  Do **not** use the following — they do not exist in dliswriter ≥ 1.2:

```python
# ❌ WRONG — these do not exist
channel.representation_code = ...
frame.set_dataset(...)

# ✅ CORRECT
channel = lf.add_channel(name, data=array, cast_dtype=np.float32)
```

### 9.2 First channel must be the index

DLIS frames require the first channel to be the depth (or time) index.  Always add the depth channel first.

```python
builder.add_channel(ChannelDef(name="DEPT", ...))   # ← index channel first
builder.add_channel(ChannelDef(name="GR",   ...))
```

### 9.3 lasio `HeaderItem.__bool__` always returns `False`

When reading LAS `~Well` sections with `lasio`, every `HeaderItem` evaluates as falsy even when it has a string value.  The library handles this internally:

```python
# ❌ WRONG — always False even when item has a value
if item and item.value:
    ...

# ✅ CORRECT
if item is not None and item.value is not None:
    ...
```

### 9.4 `unit_row` sentinel value

| Value | Meaning |
|---|---|
| `None` (default) | Auto-detect — scan the file |
| `-1` | Disabled — no unit row, don't scan |
| `1` | Unit row is the first row after the header (0-based in file) |

Passing `unit_row=0` means "skip the header row as a unit row", which is almost certainly wrong.

### 9.5 Memory for large image files

LAS / CSV files with large image arrays can occupy several hundred MB when loaded as numpy arrays.  For long depths with wide borehole-image channels:

- `convert_batch` keeps `max_workers` low (2–4) to prevent concurrent conversions from exhausting RAM.
- The writer avoids redundant array copies — if the source array already has the target dtype, no copy is made until the null-replace step.
- Each batch worker calls `gc.collect()` after each conversion to release arrays promptly.

```python
# Safe for files with many depth samples and wide image arrays
LASConverter(image_resolution="low").convert_batch("/data/", max_workers=2)
```

### 9.6 DLIS channel name constraints

DLIS channel names must be ≤ 256 characters and ASCII-printable.  Non-ASCII characters (accented letters, brackets, hyphens) in LAS mnemonics or CSV headers are replaced with `_` via `sanitize_channel_name()`.  Use `validate_channel_names()` to verify output names if exact round-trip matters.

### 9.7 `DLISFileConfig is OriginConfig`

`DLISFileConfig` is a module-level alias (`DLISFileConfig = OriginConfig`), not a subclass.  `isinstance(cfg, OriginConfig)` and `isinstance(cfg, DLISFileConfig)` are both `True` for any instance.

### 9.8 `from_dict()` ignores unknown keys silently

Both `WellMetadata.from_dict()` and `OriginConfig.from_dict()` discard keys that don't match dataclass fields (tolerant reader for API payloads).  Typos like `"weel_name"` are silently dropped — inspect with `repr()` if in doubt.

### 9.9 `ParameterDef.value` type

`ParameterDef.value` is `Union[str, float, int]`.  Use `ParameterDef.numeric()` for numeric values — it stores the actual `float` rather than a string representation, which avoids a redundant text→float parse at write time.

### 9.10 Do not cast string channels to float

Do not call `.astype(float)` or `np.asarray(values, dtype=float)` on categorical data before creating a `ChannelDef`.  Pass the raw values as `dtype=object` and let the library handle encoding:

```python
# ❌ WRONG — raises ValueError: could not convert string to float: 'Sedimentary'
ChannelDef(name="LITH", unit="", data=np.asarray(labels, dtype=float))

# ✅ CORRECT
ChannelDef(name="LITH", unit="", data=np.asarray(labels, dtype=object))
```

### 9.11 Multi-frame index channel requirement

When `group_by_depth=True` (the default), each depth group must contain a recognisable index channel.  A channel is recognised as the index if its name matches a known depth mnemonic (`DEPT`, `DEPTH`, `MD`, `TDEP`, `TVD`, `TVDSS`, `TVDMD`) **or** its unit is a length unit (`m`, `ft`, `in`, `cm`, `mm`, `km`).  If no index can be found for a group, the builder falls back to single-frame validation and raises a `"same number of depth samples"` error.

```python
# ✅ Recognised — name matches mnemonic
ChannelDef(name="DEPT",     unit="ft",  data=depth_array)

# ✅ Recognised — unit is a length unit
ChannelDef(name="DEPTH_FMI", unit="ft", data=image_depth_array)

# ❌ NOT recognised — unknown name and non-length unit; builder cannot auto-group
ChannelDef(name="INDEX",    unit="",   data=depth_array)
```

---

## 10. Dependencies

### Required (always installed)

| Package | Purpose |
|---|---|
| `numpy` | Array representation and dtype handling |
| `dliswriter` | Low-level DLIS binary writing |

### Optional

| Extra | Package(s) | Purpose |
|---|---|---|
| `[las]` | `lasio ≥ 0.32` | Parse LAS 1.2 / 2.0 / 3.0 files |
| `[csv]` | `pandas ≥ 2.0` | Parse CSV / TSV files |
| `[verify]` | `dlisio ≥ 0.3` | Read back and validate written DLIS files |

### Dev / test extras (not installed in production)

```bash
pip install "dlis-builder[dev]"
# installs: pytest, pytest-cov, ruff, mypy, lasio, pandas, dlisio
```

---

*Full format documentation (LAS structure, DLIS binary layout, unit normalisation tables) is in the top-level [README.md](../README.md).*

