from datetime import datetime
import json

from prefect import flow, task
from prefect.logging import get_run_logger
from psycopg.types.json import Json
from sqlalchemy import text

from prefect_mls_data.tools.dbutils import engine
from prefect_mls_data.tools.sportmonk import (
    FixtureDetails, FixtureParticipants, FixtureWeather, FixtureLineups, FixturePerformance,
    EventList, Timeline, Comments, Trends, Statistics,
    Metadata, Formations, Coordinates, FixtureXG, Scores)

ENGINE = engine()

CHILD_TABLES = [
    'participants', 'weather', 'lineups', 'performance',
    'events', 'timeline', 'comments', 'trends',
    'statistics', 'metadata', 'formations', 'coordinates',
    'xg', 'scores',
]

def clean_row_formatting(rows: list) -> list:
    ## Convert Pytdantic Model to dict
    rows = [dict(x) for x in rows]
    ## Convert object types into string
    rows = [{k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()} for row in rows]
    return rows

@task(name="delete-fixture-data", log_prints=True)
def delete_fixture_data(fixture_id: int) -> None:
    logger = get_run_logger()

    with ENGINE.begin() as conn:
        for name in CHILD_TABLES:
            result = conn.execute(text(f"DELETE FROM raw.{name} WHERE fixture_id = {fixture_id}"))
            logger.info(f"Deleted {result.rowcount} rows from {name}")

        result = conn.execute(text(f"DELETE FROM raw.fixtures WHERE fixture_id = {fixture_id}"))
        logger.info(f"Deleted {result.rowcount} rows from fixtures")
    logger.info(f"Delete transaction committed for fixture_id={fixture_id}")


@task(name="write-fixture-data", log_prints=True)
def write_fixture_data(all_rows: list[tuple[str, list[dict]]]) -> None:
    logger = get_run_logger()
    with ENGINE.begin() as conn:
        for table_name, rows in all_rows:
            if not rows:
                logger.info(f"No rows for {table_name}")
                return
            rows = clean_row_formatting(rows)
            cols = list(rows[0].keys())
            statement = text(f"INSERT INTO raw.{table_name} ({', '.join(cols)}) VALUES ({', '.join(f':{c}' for c in cols)})")
            conn.execute(statement, rows)
            logger.info(f"Inserted {len(rows)} rows into {table_name}")

    logger.info("Write transaction committed")


@flow(name="persist-game-to-database", log_prints=True)
def persist_game(
    fixture: FixtureDetails,
    participants: FixtureParticipants,
    weather: FixtureWeather,
    lineups: FixtureLineups,
    performance: FixturePerformance,
    events: EventList,
    timeline: Timeline,
    comments: Comments,
    trends: Trends,
    statistics: Statistics,
    metadata: Metadata,
    formations: Formations,
    coordinates: Coordinates,
    xg: FixtureXG,
    scores: Scores,
) -> None:
    logger = get_run_logger()
    fixture_id = fixture.fixture_id
    logger.info(f"Persisting fixture_id={fixture_id}: {fixture.game_name}")

    # Build (table_name, rows) pairs — parent first, then children
    all_rows = [
        ('fixtures', [fixture]),
        ('participants', participants.participants),
        ('weather', [weather]),
        ('lineups', lineups.lineup),
        ('performance', [d for pp in performance.performance for d in pp.details]),
        ('events', events.events),
        ('timeline', timeline.timeline),
        ('comments', comments.comments),
        ('trends', trends.trends),
        ('statistics', statistics.statistics),
        ('metadata', metadata.metadata),
        ('formations', formations.formations),
        ('coordinates', coordinates.coordinates),
        ('xg', xg.xg),
        ('scores', scores.scores),
    ]

    delete_fixture_data(fixture_id)
    write_fixture_data(all_rows)