import json
from collections import defaultdict

from prefect import task
from prefect.logging import get_run_logger
from sqlalchemy import text

from prefect_mls_data.tools.dbutils import engine

ENGINE = engine()

CHILD_TABLES = [
    'participants', 'weather', 'lineups', 'performance',
    'events', 'timeline', 'comments', 'trends',
    'statistics', 'metadata', 'formations', 'coordinates',
    'xg', 'scores',
]

def clean_row_formatting(rows: list) -> list:
    ## Convert Pydantic Model to dict
    rows = [dict(x) for x in rows]
    ## Convert object types into string
    rows = [{k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()} for row in rows]
    return rows


def _extract_rows(game_data: dict) -> tuple[int, list[tuple[str, list]]]:
    """Extract (table_name, rows) pairs from a single game_data dict. Pure in-memory, no DB."""
    fixture = game_data['fixture']
    fixture_id = fixture.fixture_id

    all_rows = [
        ('fixtures', [fixture]),
        ('participants', game_data['participants'].participants),
        ('weather', [game_data['weather']]),
        ('lineups', game_data['lineups'].lineup),
        ('performance', [d for pp in game_data['performance'].performance for d in pp.details]),
        ('events', game_data['events'].events),
        ('timeline', game_data['timeline'].timeline),
        ('comments', game_data['comments'].comments),
        ('trends', game_data['trends'].trends),
        ('statistics', game_data['statistics'].statistics),
        ('metadata', game_data['metadata'].metadata),
        ('formations', game_data['formations'].formations),
        ('coordinates', game_data['coordinates'].coordinates),
        ('xg', game_data['xg'].xg),
        ('scores', game_data['scores'].scores),
    ]

    all_rows = [(name, [r for r in rows if r is not None]) for name, rows in all_rows]
    all_rows = [(name, rows) for name, rows in all_rows if rows]

    return fixture_id, all_rows


@task(name="extract-game-rows")
def extract_game_rows(game_data: dict) -> tuple[int, list[tuple[str, list]]]:
    """Extract rows from a single game into (fixture_id, [(table, rows), ...]). No DB access."""
    logger = get_run_logger()
    fixture_id, all_rows = _extract_rows(game_data)
    logger.info(f"Extracted rows for fixture_id={fixture_id}: {game_data['fixture'].game_name}")
    return fixture_id, all_rows


@task(name="merge-all-game-rows")
def merge_all_game_rows(extracted: list[tuple[int, list[tuple[str, list]]]]) -> tuple[list[int], list[tuple[str, list]]]:
    """Merge extracted rows from all games into single lists per table."""
    logger = get_run_logger()
    fixture_ids = []
    merged = defaultdict(list)

    for fixture_id, all_rows in extracted:
        fixture_ids.append(fixture_id)
        for table_name, rows in all_rows:
            merged[table_name].extend(rows)

    # Maintain consistent table ordering: fixtures first, then child tables
    table_order = ['fixtures'] + CHILD_TABLES
    merged_rows = [(t, merged[t]) for t in table_order if t in merged]

    logger.info(f"Merged {len(fixture_ids)} games across {len(merged_rows)} tables")
    for table_name, rows in merged_rows:
        logger.info(f"  {table_name}: {len(rows)} rows")

    return fixture_ids, merged_rows


@task(name="batch-delete-fixtures")
def batch_delete_fixture_data(fixture_ids: list[int]) -> None:
    """Delete all data for the given fixture IDs in a single transaction."""
    logger = get_run_logger()
    if not fixture_ids:
        logger.info("No fixture IDs to delete")
        return

    ids_str = ', '.join(str(fid) for fid in fixture_ids)
    with ENGINE.begin() as conn:
        for name in CHILD_TABLES:
            result = conn.execute(text(f"DELETE FROM raw.{name} WHERE fixture_id IN ({ids_str})"))
            logger.info(f"Deleted {result.rowcount} rows from {name}")

        result = conn.execute(text(f"DELETE FROM raw.fixtures WHERE fixture_id IN ({ids_str})"))
        logger.info(f"Deleted {result.rowcount} rows from fixtures")

    logger.info(f"Batch delete committed for {len(fixture_ids)} fixtures")


@task(name="batch-write-fixtures", retries=3)
def batch_write_fixture_data(merged_rows: list[tuple[str, list]]) -> None:
    """Write all merged rows across all games in a single transaction."""
    logger = get_run_logger()
    with ENGINE.begin() as conn:
        for table_name, rows in merged_rows:
            if not rows:
                continue
            logger.info(f"Writing {len(rows)} rows to {table_name}")
            rows = clean_row_formatting(rows)
            cols = list(rows[0].keys())
            statement = text(f"INSERT INTO raw.{table_name} ({', '.join(cols)}) VALUES ({', '.join(f':{c}' for c in cols)})")
            conn.execute(statement, rows)
            logger.info(f"Inserted {len(rows)} rows into {table_name}")

    logger.info("Batch write transaction committed")
