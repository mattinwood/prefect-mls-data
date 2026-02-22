from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.futures import wait
from prefect_mls_data.tools import sportmonk
from prefect_mls_data.dbflows import (
    extract_game_rows, merge_all_game_rows,
    batch_delete_fixture_data, batch_write_fixture_data,
)


def build_game_models(game: dict) -> dict:
    """Convert raw API response dict into all 15 Pydantic models."""
    return {
        'fixture': sportmonk.FixtureDetails(**game),
        'participants': sportmonk.FixtureParticipants(
            participants=[sportmonk.ParticipantDetails(**p, fixture_id=game['id']) for p in game['participants']]
        ),
        'weather': sportmonk.FixtureWeather(**game['weatherreport']) if game.get('weatherreport') else None,
        'lineups': sportmonk.FixtureLineups(lineup=game['lineups']),
        'performance': sportmonk.FixturePerformance(performance=game['lineups']),
        'events': sportmonk.EventList(events=game['events']),
        'timeline': sportmonk.Timeline(timeline=game['timeline']),
        'comments': sportmonk.Comments(comments=game['comments']),
        'trends': sportmonk.Trends(trends=game['trends']),
        'statistics': sportmonk.Statistics(statistics=game['statistics']),
        'metadata': sportmonk.Metadata(metadata=game['metadata']),
        'formations': sportmonk.Formations(formations=game['formations']),
        'coordinates': sportmonk.Coordinates(coordinates=game['ballcoordinates']),
        'xg': sportmonk.FixtureXG(xg=game['xgfixture']),
        'scores': sportmonk.Scores(scores=game['scores']),
    }


@task(name='retrieve-fixture-ids', retries=3)
def retrieve_fixture_ids(date_range_start=None, date_range_end=None):
    return sportmonk.get_fixture_ids(date_range_start, date_range_end)


@task(name='download-single-game', retries=3)
def download_single_game(game_id: int) -> dict:
    logger = get_run_logger()
    sportmonk.set_logger(logger)

    response = sportmonk.get_fixture_details(fixture_id=game_id)
    assert len(response['data']) == 1, f'Expected 1 fixture, got {len(response["data"])}'
    game = response['data'][0]

    logger.info(f'Downloaded game {game_id}: {game['name']}')
    models = build_game_models(game)
    logger.info(f'Build {len(models)} for game {game_id}: {models["fixture"].game_name}')
    return models


@flow(name='download-individual-game-by-id')
def download_game(game_id: int = 19353071):
    """Standalone flow for downloading and persisting a single game."""
    data = download_single_game(game_id)
    fixture_id, all_rows = extract_game_rows(data)
    batch_delete_fixture_data([fixture_id])
    batch_write_fixture_data(all_rows)


@flow(name='download-all-games', log_prints=True)
def download_all_games(date_range_start=None, date_range_end=None):
    """Download all games in a date range. Two-batch parallel execution."""
    logger = get_run_logger()
    sportmonk.set_logger(logger)
    ids = retrieve_fixture_ids(date_range_start, date_range_end)
    if ids:
        logger.info(f'Downloading {len(ids)} games')

        # Batch 1: Download all games in parallel
        download_futures = [download_single_game.submit(game_id) for game_id in ids]
        wait(download_futures)

        # Batch 2: Extract rows from each game in parallel (no DB access)
        extract_futures = [extract_game_rows.submit(f.result()) for f in download_futures]
        wait(extract_futures)

        # Batch 3: Merge all extracted rows by table, then delete and write in single transactions
        fixture_ids, merged_rows = merge_all_game_rows([f.result() for f in extract_futures])
        batch_delete_fixture_data(fixture_ids)
        batch_write_fixture_data(merged_rows)
