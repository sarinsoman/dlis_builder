"""
dlis_builder.__main__
-----------------------
CLI entry point so the package can be invoked as:

    python -m dlis_builder <args>
    dlis-builder <args>         (after pip install)
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dlis-builder",
        description="Convert LAS or CSV well log files to DLIS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  dlis-builder well.las\n"
            "  dlis-builder well.las -o output.dlis --resolution high\n"
            "  dlis-builder data.csv -o output.dlis --format csv "
            "                --depth-column DEPTH --unit DEPTH:m --unit GR:gAPI\n"
            "  dlis-builder /data/las_files/ -o /data/dlis/ --recursive\n"
        ),
    )
    p.add_argument("input", help="Input LAS / CSV file or directory")
    p.add_argument("-o", "--output", default=None, metavar="PATH",
                   help="Output DLIS file or directory (default: <input>.dlis)")
    p.add_argument("--format", choices=["las", "csv"], default=None,
                   help="Input format (auto-detected from extension if omitted)")
    p.add_argument("--producer", default="",
                   help="Producer name in DLIS Origin")
    p.add_argument("--product", default="dlis-builder",
                   help="Product name in DLIS Origin")
    p.add_argument("--well", default=None, metavar="NAME",
                   help="Override well name")
    p.add_argument("--company", default=None, metavar="NAME",
                   help="Override company name")
    p.add_argument(
        "--array-map", dest="array_map", action="append", default=[],
        metavar="NAME:COL1,COL2,...",
        help="Group CSV/LAS columns into an array channel.  Repeatable.",
    )
    p.add_argument("--depth-column", dest="depth_column", default=None,
                   help="CSV depth/index column name (default: first column)")
    p.add_argument(
        "--unit", dest="units", action="append", default=[],
        metavar="COL:UNIT",
        help="Assign unit to a CSV column.  Repeatable e.g. --unit GR:gAPI",
    )
    p.add_argument("-r", "--recursive", action="store_true",
                   help="Recurse subdirectories (batch mode)")
    p.add_argument(
        "--resolution", choices=["low", "high"], default="low",
        help="Array channel precision: low=float32 (default), high=float64",
    )
    p.add_argument("--flatten-arrays", dest="flatten_arrays", action="store_true",
                   help="Write array elements as separate scalar channels")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Debug-level logging")
    p.add_argument("-q", "--quiet", action="store_true",
                   help="Errors only")
    return p


def _parse_array_map(raw: list[str]) -> dict:
    result: dict = {}
    for item in raw:
        if ":" not in item:
            continue
        name, cols_str = item.split(":", 1)
        cols = [c.strip() for c in cols_str.split(",") if c.strip()]
        if len(cols) >= 2:
            result[name.strip()] = cols
    return result


def _parse_units(raw: list[str]) -> dict:
    result: dict = {}
    for item in raw:
        if ":" not in item:
            continue
        col, unit = item.split(":", 1)
        result[col.strip()] = unit.strip()
    return result


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    level = logging.DEBUG if args.verbose else (logging.ERROR if args.quiet else logging.INFO)
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    # Detect format
    inp = Path(args.input)
    fmt = args.format
    if fmt is None:
        if inp.is_dir():
            fmt = "las"   # default batch mode: look for .las
        elif inp.suffix.lower() == ".csv":
            fmt = "csv"
        else:
            fmt = "las"

    array_map = _parse_array_map(args.array_map) or None

    from dlis_builder.models import OriginConfig, WellMetadata  # noqa: PLC0415

    origin = OriginConfig(producer_name=args.producer, product_name=args.product)
    meta_overrides: dict = {}
    if args.well:
        meta_overrides["well_name"] = args.well
    if args.company:
        meta_overrides["company"] = args.company

    if fmt == "las":
        from dlis_builder.converters import LASConverter  # noqa: PLC0415
        conv = LASConverter(image_resolution=args.resolution)
        if inp.is_dir():
            results = conv.convert_batch(
                str(inp), output_dir=args.output, recursive=args.recursive,
                array_map=array_map, flatten_arrays=args.flatten_arrays,
            )
            failed = [r for r in results if not r["success"]]
            if failed:
                for r in failed:
                    print(f"FAIL: {r['las_path']}: {r['error']}", file=sys.stderr)
                return 1
            if not args.quiet:
                print(f"Batch complete: {len(results)} file(s) converted.")
            return 0
        else:
            meta = WellMetadata(**meta_overrides) if meta_overrides else None
            dlis_path = conv.convert(
                str(inp), args.output or str(inp.with_suffix(".dlis")),
                metadata=meta, origin=origin,
                array_map=array_map, flatten_arrays=args.flatten_arrays,
            )

    else:  # csv
        from dlis_builder.converters import CSVConverter  # noqa: PLC0415
        units = _parse_units(args.units)
        conv_csv = CSVConverter(
            depth_column=args.depth_column,
            column_units=units or None,
            array_columns=array_map,
            image_resolution=args.resolution,
        )
        if inp.is_dir():
            meta = WellMetadata(**meta_overrides) if meta_overrides else None
            results = conv_csv.convert_batch(
                str(inp), output_dir=args.output,
                recursive=args.recursive, metadata=meta,
                flatten_arrays=args.flatten_arrays,
            )
            failed = [r for r in results if not r["success"]]
            if failed:
                for r in failed:
                    print(f"FAIL: {r['csv_path']}: {r['error']}", file=sys.stderr)
                return 1
            if not args.quiet:
                print(f"Batch complete: {len(results)} file(s) converted.")
            return 0
        else:
            meta = WellMetadata(**meta_overrides) if meta_overrides else WellMetadata()
            dlis_path = conv_csv.convert(
                str(inp), args.output or str(inp.with_suffix(".dlis")),
                metadata=meta, origin=origin, flatten_arrays=args.flatten_arrays,
            )

    if not args.quiet:
        in_size  = inp.stat().st_size
        out_size = Path(dlis_path).stat().st_size
        print(f"\n{'─'*60}")
        print(f"  Input  : {args.input} ({in_size:,} bytes)")
        print(f"  Output : {dlis_path} ({out_size:,} bytes)")
        print(f"{'─'*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
