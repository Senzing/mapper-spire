# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mapper-spire converts Spire `enhanced_vessel_master.csv` maritime vessel data into Senzing-compatible JSON format for entity resolution. The output can be loaded into Senzing to match vessel data against other sources.

## Commands

### Run the Mapper

```bash
python3 src/spire_mapper.py -i <input_csv> -o <output_json> [-l <log_file>]
```

### Install Dependencies

```bash
python -m pip install --group all .
```

### Lint

```bash
pylint $(git ls-files '*.py' ':!:docs/source/*')
```

## Architecture

The codebase consists of a single mapper script ([spire_mapper.py](src/spire_mapper.py)) with a `mapper` class that:

1. Reads CSV input from Spire's `enhanced_vessel_master.csv`
2. Maps vessel fields to Senzing entity format (IMO, MMSI, vessel name, call sign, etc.)
3. Creates relationship records for group owners and beneficial owners (extracts organizations as separate records with `REL_POINTER`/`REL_ANCHOR` relationships)
4. Outputs JSON-lines format ready for Senzing ingestion

Key Senzing attributes mapped: `DATA_SOURCE`, `RECORD_ID`, `RECORD_TYPE` (VESSEL or ORGANIZATION), `IMO_NUMBER`, `MMSI_NUMBER`, `VESSEL_NAME_ORG`, `CALL_SIGN`.

## Python Version

Requires Python 3.10+. CI tests against Python 3.10, 3.11, 3.12, and 3.13.
