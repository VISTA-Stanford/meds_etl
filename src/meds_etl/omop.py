from __future__ import annotations

import argparse
import itertools
import json
import multiprocessing
import os
import pickle
import random
import shutil
import subprocess
import tempfile
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Tuple

import jsonschema
import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

import meds_etl
import meds_etl.unsorted
import meds_etl.utils

RANDOM_SEED: int = 3422342
# OMOP constants
CUSTOMER_CONCEPT_ID_START: int = 2_000_000_000
DEFAULT_VISIT_CONCEPT_ID: int = 8
DEFAULT_NOTE_CONCEPT_ID: int = 46235038
DEFAULT_VISIT_DETAIL_CONCEPT_ID: int = 4203722
DEFAULT_IMAGE_CONCEPT_ID: int = 4180938


OMOP_TIME_FORMATS: Iterable[str] = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d")


def split_list(ls: List[Any], parts: int) -> List[List[Any]]:
    per_list = (len(ls) + parts - 1) // parts
    result = []

    for i in range(parts):
        sublist = ls[i * per_list : (i + 1) * per_list]
        if len(sublist) > 0:
            result.append(sublist)

    return result


def get_table_files(path_to_src_omop_dir: str, table_name: str, table_details={}) -> Tuple[List[str], List[str]]:
    """Retrieve all .csv/.csv.gz/.parquet files for the OMOP table given by `table_name` in `path_to_src_omop_dir`

    Because OMOP tables can be quite large for datasets comprising millions
    of subjects, those tables are often split into compressed shards. So
    the `measurements` "table" might actually be a folder containing files
    `000000000000.csv.gz` up to `000000000152.csv.gz`. This function
    takes a path corresponding to an OMOP table with its standard name (e.g.,
    `condition_occurrence`, `measurement`, `observation`) and returns two list
    of paths.

    The first list contains all the csv files. The second list contains all parquet files.
    """
    if table_details.get("file_suffix"):
        table_name += "_" + table_details["file_suffix"]

    path_to_table: str = os.path.join(path_to_src_omop_dir, table_name)

    if os.path.exists(path_to_table) and os.path.isdir(path_to_table):
        csv_files = []
        parquet_files = []

        for a in os.listdir(path_to_table):
            fname = os.path.join(path_to_table, a)
            if a.endswith(".csv") or a.endswith(".csv.gz"):
                csv_files.append(fname)
            elif a.endswith(".parquet"):
                parquet_files.append(fname)

        return csv_files, parquet_files
    elif os.path.exists(path_to_table + ".csv"):
        return [path_to_table + ".csv"], []
    elif os.path.exists(path_to_table + ".csv.gz"):
        return [path_to_table + ".csv.gz"], []
    elif os.path.exists(path_to_table + ".parquet"):
        return [], [path_to_table + ".parquet"]
    else:
        return [], []


def read_polars_df(fname: str) -> pl.DataFrame:
    """Read a file that might be a CSV or Parquet file"""
    if fname.endswith(".csv"):
        # Don't try to infer schema because it can cause errors with columns that look like ints but aren't
        return pl.read_csv(fname, infer_schema=False)
    elif fname.endswith(".parquet"):
        return pl.read_parquet(fname)
    else:
        raise RuntimeError("Found file of unknown type " + fname + " expected parquet or csv")


def load_file(path_to_decompressed_dir: str, fname: str) -> Any:
    """Load a file from disk, unzip into temporary decompressed directory if needed

    Args:
        path_to_decompressed_dir (str): Path where (temporary) decompressed file should be stored
        fname (str): Path to the (input) file that should be loaded from disk

    Returns:
        Opened file object
    """
    if fname.endswith(".gz"):
        file = tempfile.NamedTemporaryFile(dir=path_to_decompressed_dir, suffix=".csv")
        subprocess.run(["gunzip", "-c", fname], stdout=file)
        return file
    else:
        # If the file isn't compressed, we don't write anything to `path_to_decompressed_dir`
        return open(fname)


def cast_to_datetime(schema: Any, column: str, move_to_end_of_day: bool = False):
    if column not in schema.names():
        return pl.lit(None, dtype=pl.Datetime(time_unit="us"))
    if schema[column] == pl.Utf8():
        if not move_to_end_of_day:
            return meds_etl.utils.parse_time(pl.col(column), OMOP_TIME_FORMATS)
        else:
            # Try to cast time to a datetime but if only the date is available, then use
            # that date with a timestamp of 23:59:59
            time = pl.col(column)
            time = pl.coalesce(
                time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="us"),
                time.str.to_datetime("%Y-%m-%d", strict=False, time_unit="us").dt.offset_by("1d").dt.offset_by("-1s"),
            )
            return time
    elif schema[column] == pl.Date():
        time = pl.col(column).cast(pl.Datetime(time_unit="us"))
        if move_to_end_of_day:
            time = time.dt.offset_by("1d").dt.offset_by("-1s")
        return time
    elif isinstance(schema[column], pl.Datetime):
        return pl.col(column).cast(pl.Datetime(time_unit="us"))
    else:
        raise RuntimeError("Unknown how to handle date type? " + schema[column] + " " + column)


def write_event_data(
    path_to_MEDS_unsorted_dir: str,
    get_batch: Any,
    table_name: str,
    table_config: Mapping[str, Any],
    concept_id_map: Mapping[int, str],
    concept_name_map: Mapping[int, str],
    primary_key: str = "person_id",
    is_canonical: bool = False,
    canonical_code: str = None,
) -> None:
    """Write event data from the given table to event files in MEDS Unsorted format

    Args:
        path_to_MEDS_unsorted_dir: Directory to write MEDS Unsorted output files
        get_batch: Callable that returns a LazyFrame for the batch
        table_name: Name of the OMOP table being processed
        table_config: Dictionary with table configuration (time_field, code_field, etc.)
        concept_id_map: Dictionary mapping concept IDs to concept codes
        concept_name_map: Dictionary mapping concept IDs to concept names
        primary_key: Name of the primary key column (default: "person_id")
        is_canonical: Whether this is a canonical event (BIRTH/DEATH)
        canonical_code: If canonical, the code to use (e.g., "MEDS_BIRTH")
    """
    batch = get_batch()

    batch = batch.rename({c: c.lower() for c in batch.collect_schema().names()})
    schema = batch.collect_schema()

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Determine what to use for the `subject_id` column in MEDS Unsorted  #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    # Use the configured primary_key column as subject_id
    subject_id = pl.col(primary_key.lower()).cast(pl.Int64)

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Determine what to use for the `time`                          #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    # Build timestamp from config
    time_field = table_config.get("time_field", "").lower()
    time_fallbacks = [f.lower() for f in table_config.get("time_fallbacks", [])]

    # Special handling for birth: construct from year/month/day if needed
    if is_canonical and canonical_code == meds.birth_code:
        if "year_of_birth" in schema.names():
            time = pl.coalesce(
                (
                    cast_to_datetime(schema, time_field)
                    if time_field and time_field in schema.names()
                    else pl.lit(None, dtype=pl.Datetime(time_unit="us"))
                ),
                pl.datetime(
                    pl.col("year_of_birth"),
                    pl.coalesce(
                        pl.col("month_of_birth") if "month_of_birth" in schema.names() else pl.lit(1), pl.lit(1)
                    ),
                    pl.coalesce(pl.col("day_of_birth") if "day_of_birth" in schema.names() else pl.lit(1), pl.lit(1)),
                    time_unit="us",
                ),
            )
        else:
            time = (
                cast_to_datetime(schema, time_field)
                if time_field in schema.names()
                else pl.lit(None, dtype=pl.Datetime(time_unit="us"))
            )
    else:
        options = []
        if time_field and time_field in schema.names():
            options.append(cast_to_datetime(schema, time_field, move_to_end_of_day=True))

        for fallback in time_fallbacks:
            if fallback in schema.names():
                options.append(cast_to_datetime(schema, fallback, move_to_end_of_day=True))

        if not options:
            raise ValueError(
                f"Could not find a valid time column for table '{table_name}'. "
                f"Config specified: time_field={table_config.get('time_field')}, "
                f"fallbacks={table_config.get('time_fallbacks', [])}. "
                f"Available columns: {schema.names()}"
            )

        time = pl.coalesce(options) if len(options) > 1 else options[0]

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Determine what to use for the `code` column                   #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    if is_canonical and canonical_code:
        # Use the canonical code (MEDS_BIRTH, MEDS_DEATH, etc.)
        code = pl.lit(canonical_code, dtype=pl.Utf8)
    else:
        # Check code_type to determine how to extract the code
        code_type = table_config.get("code_type", "source_value").lower()

        if code_type == "source_value":
            # Use the source_value field directly (no mapping)
            code_field = table_config.get("code_field", "").lower()
            if not code_field or code_field not in schema.names():
                raise ValueError(
                    f"code_field '{code_field}' not found in schema for table '{table_name}'. "
                    f"Available columns: {schema.names()}"
                )
            code = pl.col(code_field).cast(pl.Utf8)

        elif code_type == "concept_id":
            # Map concept_id to concept_code using the OMOP concept hierarchy
            concept_id_field = table_config.get("concept_id_field", table_name + "_concept_id").lower()

            if concept_id_field not in schema.names():
                raise ValueError(
                    f"concept_id_field '{concept_id_field}' not found in schema for table '{table_name}'. "
                    f"Available columns: {schema.names()}"
                )

            concept_id = pl.col(concept_id_field).cast(pl.Int64)

            # Try to get source_concept_id if available (preferred)
            source_concept_id_field = table_config.get("source_concept_id_field")
            if source_concept_id_field:
                source_concept_id_field = source_concept_id_field.lower()
                if source_concept_id_field in schema.names():
                    source_concept_id = pl.col(source_concept_id_field).cast(pl.Int64)
                else:
                    source_concept_id = pl.lit(0, dtype=pl.Int64)
            else:
                # Try standard pattern: replace _concept_id with _source_concept_id
                inferred_source = concept_id_field.replace("_concept_id", "_source_concept_id")
                if inferred_source in schema.names():
                    source_concept_id = pl.col(inferred_source).cast(pl.Int64)
                else:
                    source_concept_id = pl.lit(0, dtype=pl.Int64)

            # Fallback concept ID if both source and standard are 0
            fallback_concept_id = pl.lit(table_config.get("fallback_concept_id", None), dtype=pl.Int64)

            # Prefer source_concept_id, then concept_id, then fallback
            final_concept_id = (
                pl.when(source_concept_id != 0)
                .then(source_concept_id)
                .when(concept_id != 0)
                .then(concept_id)
                .otherwise(fallback_concept_id)
            )

            # Map concept IDs to concept codes using the concept_id_map
            code = final_concept_id.replace_strict(concept_id_map, return_dtype=pl.Utf8(), default=None)

        else:
            raise ValueError(
                f"Unknown code_type '{code_type}' for table '{table_name}'. " f"Must be 'source_value' or 'concept_id'."
            )

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Determine what to use for the `value`                       #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    # Extract numeric value if configured
    numeric_value_field = table_config.get("numeric_value_field", "").lower()
    text_value_field = table_config.get("text_value_field", "").lower()

    value = pl.lit(None, dtype=str)

    if text_value_field and text_value_field in schema.names():
        value = pl.col(text_value_field)

    if numeric_value_field and numeric_value_field in schema.names():
        value = pl.coalesce(pl.col(numeric_value_field), value)

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Determine the metadata columns                            #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    metadata = {
        "table": pl.lit(table_name, dtype=str),
    }

    # Add end timestamp if configured
    time_end_field = table_config.get("time_end_field", "").lower()
    if time_end_field and time_end_field in schema.names():
        end = cast_to_datetime(schema, time_end_field, move_to_end_of_day=True)
        metadata["end"] = end

    # Add metadata fields from config
    for meta_spec in table_config.get("metadata", []):
        meta_name = meta_spec.get("name", "").lower()
        meta_type = meta_spec.get("type", "string").lower()

        if meta_name in schema.names():
            # Cast to appropriate type
            if meta_type in ["int", "integer", "int64"]:
                metadata[meta_name] = pl.col(meta_name).cast(pl.Int64)
            elif meta_type in ["float", "float64", "double"]:
                metadata[meta_name] = pl.col(meta_name).cast(pl.Float64)
            elif meta_type in ["string", "str", "text"]:
                metadata[meta_name] = pl.col(meta_name).cast(pl.Utf8)
            else:
                metadata[meta_name] = pl.col(meta_name)

    batch = batch.filter(code.is_not_null())

    columns = {
        "subject_id": subject_id,
        "time": time,
        "code": code,
    }

    n, t = meds_etl.utils.convert_generic_value_to_specific(value)

    columns["numeric_value"] = n
    columns["text_value"] = t

    # Add metadata columns to the set of MEDS dataframe columns
    for k, v in metadata.items():
        columns[k] = v.alias(k)

    event_data = batch.select(**columns)
    # Write this part of the MEDS Unsorted file to disk
    fname = os.path.join(path_to_MEDS_unsorted_dir, f'{table_name.replace("/", "_")}_{uuid.uuid4()}.parquet')
    try:
        event_data.collect(streaming=True).write_parquet(fname, compression="zstd", compression_level=1)
    except pl.exceptions.InvalidOperationError as e:
        print(table_name)
        print(e)
        print(event_data.explain(streaming=True))
        raise e


def process_table_csv(args):
    (
        table_file,
        table_name,
        table_config,
        concept_id_map_data,
        concept_name_map_data,
        path_to_MEDS_unsorted_dir,
        path_to_decompressed_dir,
        primary_key,
        is_canonical,
        canonical_code,
        verbose,
    ) = args
    """
    This function is designed to be called through parallel processing utilities
    such as `pool.imap_unordered(process_table, [args1, args2, ..., argsN])

    Args:
        args (tuple): A tuple with the following elements:
            table_file (str): Path to the raw source table file (can be compressed)
            table_name (str): Name of the source table. Used for table-specific preprocessing
            table_config (Dict): Configuration dictionary for this table
            ...
    """
    concept_id_map = pickle.loads(concept_id_map_data)  # 0.25 GB for STARR-OMOP
    concept_name_map = pickle.loads(concept_name_map_data)  # 0.5GB for STARR-OMOP
    if verbose:
        print("Working on ", table_file, table_name, table_config)

    # Load the source table, decompress if needed
    with load_file(path_to_decompressed_dir, table_file) as temp_f:
        # Read it in batches so we don't max out RAM
        table = pl.read_csv_batched(
            temp_f.name,  # The decompressed source table
            infer_schema_length=0,  # Don't try to automatically infer schema
            batch_size=1_000_000,  # Read in 1M rows at a time
        )

        batch_index = 0

        while True:  # We read until there are no more batches
            batch_index += 1
            batch = table.next_batches(1)  # Returns a list of length 1 containing a table for the batch
            if batch is None:
                break

            batch = batch[0]  # (Because `table.next_batches` returns a list)

            batch = batch.lazy()

            write_event_data(
                path_to_MEDS_unsorted_dir,
                lambda: batch.lazy(),
                table_name,
                table_config,
                concept_id_map,
                concept_name_map,
                primary_key,
                is_canonical,
                canonical_code,
            )


def process_table_parquet(args):
    (
        table_files,
        table_name,
        table_config,
        concept_id_map_data,
        concept_name_map_data,
        path_to_MEDS_unsorted_dir,
        primary_key,
        is_canonical,
        canonical_code,
        verbose,
    ) = args
    """
    This function is designed to be called through parallel processing utilities
    such as `pool.imap_unordered(process_table, [args1, args2, ..., argsN])

    Args:
        args (tuple): A tuple with the following elements:
            table_files (List[str]): Paths to the raw source table files
            table_name (str): Name of the source table
            table_config (Dict): Configuration dictionary for this table
            ...
    """
    concept_id_map = pickle.loads(concept_id_map_data)  # 0.25 GB for STARR-OMOP
    concept_name_map = pickle.loads(concept_name_map_data)  # 0.5GB for STARR-OMOP
    if verbose:
        print("Working on ", table_files, table_name, table_config)

    # Load the source table, decompress if needed
    write_event_data(
        path_to_MEDS_unsorted_dir,
        lambda: pl.scan_parquet(table_files),
        table_name,
        table_config,
        concept_id_map,
        concept_name_map,
        primary_key,
        is_canonical,
        canonical_code,
    )


def load_config(config_path: str) -> Dict[str, Any]:
    """Load ETL configuration from file

    Args:
        config_path: Path to configuration JSON file

    Returns:
        Configuration dictionary with primary_key, canonical_events, and tables
    """
    if not config_path or not os.path.exists(config_path):
        raise ValueError(f"Configuration file required but not found: {config_path}")

    print(f"Loading ETL configuration from {config_path}")
    with open(config_path, "r") as f:
        config = json.load(f)

    # Validate required fields
    if "primary_key" not in config:
        raise ValueError("Configuration must include 'primary_key' field")

    if "tables" not in config:
        raise ValueError("Configuration must include 'tables' field (can be empty dict)")

    if "canonical_events" not in config:
        config["canonical_events"] = {}

    return config


def extract_metadata(path_to_src_omop_dir: str, path_to_decompressed_dir: str, verbose: int = 0) -> Tuple:
    concept_id_map: Dict[int, str] = {}  # [key] concept_id -> [value] concept_code
    concept_name_map: Dict[int, str] = {}  # [key] concept_id -> [value] concept_name
    code_metadata: Dict[str, Any] = {}  # [key] concept_code -> [value] metadata

    # Read in the OMOP `CONCEPT` table from disk
    # (see https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html#concepts)
    # and use it to generate metadata file as well as populate maps
    # from (concept ID -> concept code) and (concept ID -> concept name)
    print("Generating metadata from OMOP `concept` table")
    for concept_file in tqdm(
        itertools.chain(*get_table_files(path_to_src_omop_dir, "concept")),
        total=len(get_table_files(path_to_src_omop_dir, "concept")[0])
        + len(get_table_files(path_to_src_omop_dir, "concept")[1]),
        desc="Generating metadata from OMOP `concept` table",
    ):
        # Note: Concept table is often split into gzipped shards by default
        if verbose:
            print(concept_file)
        with load_file(path_to_decompressed_dir, concept_file) as f:
            # Read the contents of the `concept` table shard
            # `load_file` will unzip the file into `path_to_decompressed_dir` if needed
            concept = read_polars_df(f.name)

            concept_id = pl.col("concept_id").cast(pl.Int64)
            code = pl.col("vocabulary_id") + "/" + pl.col("concept_code")

            # Convert the table into a dictionary
            result = concept.select(concept_id=concept_id, code=code, name=pl.col("concept_name"))

            result = result.to_dict(as_series=False)

            # Update our running dictionary with the concepts we read in from
            # the concept table shard
            concept_id_map |= dict(zip(result["concept_id"], result["code"]))
            concept_name_map |= dict(zip(result["concept_id"], result["name"]))

            # Assuming custom concepts have concept_id > 2000000000 we create a
            # record for them in `code_metadata` with no parent codes. Such a
            # custom code could be eg `STANFORD_RACE/Black or African American`
            # with `concept_id` 2000039197
            custom_concepts = (
                concept.filter(concept_id > CUSTOMER_CONCEPT_ID_START)
                .select(concept_id=concept_id, code=code, description=pl.col("concept_name"))
                .to_dict()
            )
            for i in range(len(custom_concepts["code"])):
                code_metadata[custom_concepts["code"][i]] = {
                    "code": custom_concepts["code"][i],
                    "description": custom_concepts["description"][i],
                    "parent_codes": [],
                }

    # Include map from custom concepts to normalized (ie standard ontology)
    # parent concepts, where possible, in the code_metadata dictionary
    for concept_relationship_file in tqdm(
        itertools.chain(*get_table_files(path_to_src_omop_dir, "concept_relationship")),
        total=len(get_table_files(path_to_src_omop_dir, "concept_relationship")[0])
        + len(get_table_files(path_to_src_omop_dir, "concept_relationship")[1]),
        desc="Generating metadata from OMOP `concept_relationship` table",
    ):
        with load_file(path_to_decompressed_dir, concept_relationship_file) as f:
            # This table has `concept_id_1`, `concept_id_2`, `relationship_id` columns
            concept_relationship = read_polars_df(f.name)

            concept_id_1 = pl.col("concept_id_1").cast(pl.Int64)
            concept_id_2 = pl.col("concept_id_2").cast(pl.Int64)

            custom_relationships = (
                concept_relationship.filter(
                    concept_id_1 > CUSTOMER_CONCEPT_ID_START,
                    pl.col("relationship_id") == "Maps to",
                    concept_id_1 != concept_id_2,
                )
                .select(concept_id_1=concept_id_1, concept_id_2=concept_id_2)
                .to_dict(as_series=False)
            )

            for concept_id_1, concept_id_2 in zip(
                custom_relationships["concept_id_1"], custom_relationships["concept_id_2"]
            ):
                if concept_id_1 in concept_id_map and concept_id_2 in concept_id_map:
                    code_metadata[concept_id_map[concept_id_1]]["parent_codes"].append(concept_id_map[concept_id_2])

    # Extract dataset metadata e.g., the CDM source name and its release date
    datasets: List[str] = []
    dataset_versions: List[str] = []
    for cdm_source_file in tqdm(
        itertools.chain(*get_table_files(path_to_src_omop_dir, "cdm_source")),
        total=len(get_table_files(path_to_src_omop_dir, "cdm_source")[0])
        + len(get_table_files(path_to_src_omop_dir, "cdm_source")[1]),
        desc="Extracting dataset metadata",
    ):
        with load_file(path_to_decompressed_dir, cdm_source_file) as f:
            cdm_source = read_polars_df(f.name)
            cdm_source = cdm_source.rename({c: c.lower() for c in cdm_source.columns})
            cdm_source = cdm_source.to_dict(as_series=False)

            datasets.extend(cdm_source["cdm_source_name"])
            dataset_versions.extend(cdm_source["cdm_release_date"])

    metadata = {
        "dataset_name": "|".join(datasets),  # eg 'Epic Clarity SHC|Epic Clarity LPCH'
        "dataset_version": "|".join(str(a) for a in dataset_versions),  # eg '2024-02-01|2024-02-01'
        "etl_name": "meds_etl.omop",
        "etl_version": meds_etl.__version__,
        "meds_version": meds.__version__,
    }
    # At this point metadata['code_metadata']['STANFORD_MEAS/AMOXICILLIN/CLAVULANATE']
    # should give a dictionary like
    # {'description': 'AMOXICILLIN/CLAVULANATE', 'parent_codes': ['LOINC/18862-3']}
    # where LOINC Code '18862-3' is a standard concept representing a lab test
    # measurement determining microorganism susceptibility to Amoxicillin+clavulanic acid

    jsonschema.validate(instance=metadata, schema=meds.dataset_metadata_schema)

    return metadata, code_metadata, concept_id_map, concept_name_map


def main():
    parser = argparse.ArgumentParser(prog="meds_etl_omop", description="Performs an ETL from OMOP v5 to MEDS")
    parser.add_argument(
        "path_to_src_omop_dir",
        type=str,
        help="Path to the OMOP source directory, e.g. "
        "`~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_2023_11_19` "
        " for STARR-OMOP full or "
        "`~/Downloads/data/som-rit-phi-starr-prod.starr_omop_cdm5_confidential_1pcent_2024_02_09`",
    )
    parser.add_argument("path_to_dest_meds_dir", type=str, help="Path to where the output MEDS files will be stored")
    parser.add_argument(
        "config",
        type=str,
        help="Path to the ETL configuration JSON file.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=100,
        help="Number of shards to use for converting MEDS from the unsorted format "
        "to MEDS (subjects are distributed approximately uniformly at "
        "random across shards and collation/joining of OMOP tables is "
        "performed on a shard-by-shard basis).",
    )
    parser.add_argument("--num_proc", type=int, default=1, help="Number of vCPUs to use for performing the MEDS ETL")
    parser.add_argument(
        "--backend",
        type=str,
        default="polars",
        help="The backend to use when converting from MEDS Unsorted to MEDS in the ETL. See the README for a discussion on possible backends.",
    )
    parser.add_argument("--verbose", type=int, default=0)
    parser.add_argument(
        "--continue_job",
        dest="continue_job",
        action="store_true",
        help="If set, the job continues from a previous run, starting after the "
        "conversion to MEDS Unsorted but before converting from MEDS Unsorted to MEDS.",
    )
    parser.add_argument(
        "--force_refresh",
        action="store_true",
        help="If set, this will overwrite all previous MEDS data in the output dir.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.path_to_src_omop_dir):
        raise ValueError(f'The source OMOP folder ("{args.path_to_src_omop_dir}") does not seem to exist?')

    # Create the directory where final MEDS files will go, if doesn't already exist
    if args.force_refresh:
        if os.path.exists(args.path_to_dest_meds_dir):
            if args.verbose:
                print(f"Deleting and recreating {args.path_to_dest_meds_dir}")
            shutil.rmtree(args.path_to_dest_meds_dir)
    os.makedirs(args.path_to_dest_meds_dir, exist_ok=True)

    # Within the target directory, create temporary subfolder for holding files
    # that need to be decompressed as part of the ETL (via eg `load_file`)
    path_to_decompressed_dir = os.path.join(args.path_to_dest_meds_dir, "decompressed")

    # Create temporary folder for storing MEDS Unsorted data within target directory
    path_to_temp_dir = os.path.join(args.path_to_dest_meds_dir, "temp")
    path_to_MEDS_unsorted_dir = os.path.join(path_to_temp_dir, "unsorted_data")

    if not args.continue_job or not (os.path.exists(path_to_MEDS_unsorted_dir) and os.path.exists(path_to_temp_dir)):
        if os.path.exists(path_to_temp_dir):
            if args.verbose:
                print(f"Deleting and recreating {path_to_temp_dir}")
            shutil.rmtree(path_to_temp_dir)
        os.mkdir(path_to_temp_dir)

        if os.path.exists(path_to_MEDS_unsorted_dir):
            if args.verbose:
                print(f"Deleting and recreating {path_to_MEDS_unsorted_dir}")
            shutil.rmtree(path_to_MEDS_unsorted_dir)
        os.mkdir(path_to_MEDS_unsorted_dir)

        if os.path.exists(path_to_decompressed_dir):
            if args.verbose:
                print(f"Deleting and recreating {path_to_decompressed_dir}")
            shutil.rmtree(path_to_decompressed_dir)
        os.mkdir(path_to_decompressed_dir)

        # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Generate metadata.json from OMOP concept table  #
        # # # # # # # # # # # # # # # # # # # # # # # # # #

        metadata, code_metadata, concept_id_map, concept_name_map = extract_metadata(
            path_to_src_omop_dir=args.path_to_src_omop_dir,
            path_to_decompressed_dir=path_to_decompressed_dir,
            verbose=args.verbose,
        )

        os.mkdir(os.path.join(path_to_temp_dir, "metadata"))

        # Save the extracted metadata file to disk...
        # We save one copy in the MEDS Unsorted data directory
        with open(os.path.join(path_to_temp_dir, "metadata", "dataset.json"), "w") as f:
            json.dump(metadata, f)

        table = pa.Table.from_pylist(code_metadata.values(), meds.code_metadata_schema())
        pq.write_table(table, os.path.join(path_to_temp_dir, "metadata", "codes.parquet"))
        # And we save another copy in the final/target MEDS directory
        shutil.copytree(
            os.path.join(path_to_temp_dir, "metadata"), os.path.join(args.path_to_dest_meds_dir, "metadata")
        )

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        # Convert all "measurements" to MEDS Unsorted, write to disk  #
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        # Load ETL configuration
        config = load_config(args.config)
        primary_key = config["primary_key"]

        # Prepare concept_id_map and concept_name_map for parallel processing
        concept_id_map_data = pickle.dumps(concept_id_map)
        concept_name_map_data = pickle.dumps(concept_name_map)

        # Create tasks for all tables
        all_csv_tasks = []
        all_parquet_tasks = []

        # First, process canonical events (BIRTH, DEATH, etc.)
        for event_name, event_config in config.get("canonical_events", {}).items():
            table_name = event_config["table"]
            # Get canonical code from config, with fallback to defaults
            if "code" in event_config:
                canonical_code = event_config["code"]
            else:
                # Fallback to default codes
                canonical_code = (
                    meds.birth_code
                    if event_name == "birth"
                    else meds.death_code if event_name == "death" else f"MEDS_{event_name.upper()}"
                )

            csv_table_files, parquet_table_files = get_table_files(
                path_to_src_omop_dir=args.path_to_src_omop_dir,
                table_name=table_name,
                table_details={},
            )

            all_csv_tasks.extend(
                (
                    table_file,
                    table_name,
                    event_config,
                    concept_id_map_data,
                    concept_name_map_data,
                    path_to_MEDS_unsorted_dir,
                    path_to_decompressed_dir,
                    primary_key,
                    True,  # is_canonical
                    canonical_code,
                    args.verbose,
                )
                for table_file in csv_table_files
            )

            all_parquet_tasks.extend(
                (
                    table_files,
                    table_name,
                    event_config,
                    concept_id_map_data,
                    concept_name_map_data,
                    path_to_MEDS_unsorted_dir,
                    primary_key,
                    True,  # is_canonical
                    canonical_code,
                    args.verbose,
                )
                for table_files in split_list(parquet_table_files, args.num_shards)
            )

        # Then, process regular tables
        for table_name, table_config in config.get("tables", {}).items():
            csv_table_files, parquet_table_files = get_table_files(
                path_to_src_omop_dir=args.path_to_src_omop_dir,
                table_name=table_name,
                table_details={},
            )

            all_csv_tasks.extend(
                (
                    table_file,
                    table_name,
                    table_config,
                    concept_id_map_data,
                    concept_name_map_data,
                    path_to_MEDS_unsorted_dir,
                    path_to_decompressed_dir,
                    primary_key,
                    False,  # is_canonical
                    None,
                    args.verbose,
                )
                for table_file in csv_table_files
            )

            all_parquet_tasks.extend(
                (
                    table_files,
                    table_name,
                    table_config,
                    concept_id_map_data,
                    concept_name_map_data,
                    path_to_MEDS_unsorted_dir,
                    primary_key,
                    False,  # is_canonical
                    None,
                    args.verbose,
                )
                for table_files in split_list(parquet_table_files, args.num_shards)
            )

        rng = random.Random(RANDOM_SEED)
        rng.shuffle(all_csv_tasks)
        rng.shuffle(all_parquet_tasks)

        print("Decompressing OMOP tables, mapping to MEDS Unsorted format, writing to disk...")
        if args.num_proc > 1:
            os.environ["POLARS_MAX_THREADS"] = "1"
            with multiprocessing.get_context("spawn").Pool(args.num_proc) as pool:
                # Wrap all tasks with tqdm for a progress bar
                total_tasks = len(all_csv_tasks)
                with tqdm(total=total_tasks, desc="Mapping CSV OMOP tables -> MEDS format") as pbar:
                    for _ in pool.imap_unordered(process_table_csv, all_csv_tasks):
                        pbar.update()

                total_tasks = len(all_parquet_tasks)
                with tqdm(total=total_tasks, desc="Mapping Parquet OMOP tables -> MEDS format") as pbar:
                    for _ in pool.imap_unordered(process_table_parquet, all_parquet_tasks):
                        pbar.update()

        else:
            # Useful for debugging `process_table` without multiprocessing
            for task in tqdm(all_csv_tasks):
                process_table_csv(task)

            for task in tqdm(all_parquet_tasks):
                process_table_parquet(task)

        # TODO: Do we need to do this more often so as to reduce maximum disk storage?
        shutil.rmtree(path_to_decompressed_dir)

        print("Finished converting dataset to MEDS Unsorted.")

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # Collate measurements into timelines for each subject, by shard
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

    print("Converting from MEDS Unsorted to MEDS...")
    meds_etl.unsorted.sort(
        source_unsorted_path=path_to_temp_dir,
        target_meds_path=os.path.join(args.path_to_dest_meds_dir, "result"),
        num_shards=args.num_shards,
        num_proc=args.num_proc,
        backend=args.backend,
    )
    print("...finished converting MEDS Unsorted to MEDS")
    shutil.move(
        src=os.path.join(args.path_to_dest_meds_dir, "result", "data"),
        dst=os.path.join(args.path_to_dest_meds_dir, "data"),
    )
    shutil.rmtree(path=os.path.join(args.path_to_dest_meds_dir, "result"))

    print(f"Deleting temporary directory `{path_to_temp_dir}`")
    shutil.rmtree(path_to_temp_dir)


if __name__ == "__main__":
    main()
