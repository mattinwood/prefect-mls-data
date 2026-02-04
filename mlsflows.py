from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.futures import wait
from prefect_mls_data.tools import sportmonk
from pydantic import BaseModel
from datetime import datetime
from time import sleep


@task(name='download-single-fixture', log_prints=True)
def request_fixture(game_id: int) -> dict:
    response = sportmonk.get_fixture_details(fixture_id=game_id)
    assert len(response['data']) == 1, f'Expected 1 fixture, got {len(response["data"])}'
    sleep(3)
    return response['data'][0]


@task(name='fixture-class')
def fixture_class(fixture: dict) -> BaseModel:
    data = sportmonk.FixtureDetails(**fixture)
    sleep(3)
    return data

@task(name='participant-class')
def participant_class(fixture: dict) -> BaseModel:
    data = sportmonk.FixtureParticipants(
        participants=[sportmonk.ParticipantDetails(**p, fixture_id=fixture['id']) for p in fixture['participants']]
    )
    sleep(3)
    return data

@task(name='weather-class')
def weather_class(fixture: dict) -> BaseModel:
    data = sportmonk.FixtureWeather(**fixture['weatherreport'])
    sleep(3)
    return data

@task(name='lineup-class')
def lineup_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.FixtureLineups(lineup=fixture['lineups'])

@task(name='performance-class')
def performance_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.FixturePerformance(performance=fixture['lineups'])

@task(name='events-class')
def events_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.EventList(events=fixture['events'])

@task(name='timeline-class')
def timeline_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Timeline(timeline=fixture['timeline'])

@task(name='comment-class')
def comment_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Comments(comments=fixture['comments'])

@task(name='trends-class')
def trends_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Trends(trends=fixture['trends'])

@task(name='statistics-class')
def statistics_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Statistics(statistics=fixture['statistics'])

@task(name='metadata-class')
def metadata_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Metadata(metadata=fixture['metadata'])

@task(name='formation-class')
def formation_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Formations(formations=fixture['formations'])

@task(name='coordinate-class')
def coordinate_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Coordinates(coordinates=fixture['ballcoordinates'])

@task(name='xg-class')
def xg_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.FixtureXG(xg=fixture['xgfixture'])

@task(name='score-class')
def score_class(fixture: dict) -> BaseModel:
    sleep(3)
    return sportmonk.Scores(scores=fixture['scores'])

@task(name='dummy')
def dummy_step(game_id='Unknown ID', game_name='Unknown Game'):
    sleep(3)
    get_run_logger().info(f'Successfully extracted data for game {game_id}: {game_name}')
    pass

@flow(name='download-individual-game-by-id', log_prints=True)
def download_game(game_id: int = 19353071):
    ### Overwrite the logger in the sportmonks module to the Prefect one
    logger = get_run_logger()
    sportmonk.set_logger(logger)

    ### Retrieve the fixture data response
    game = request_fixture(game_id)

    ### Create Pydantic models for each of the fixture subcategories
    fixture = fixture_class.submit(game)
    participant = participant_class.submit(game)
    weather = weather_class.submit(game)
    lineup = lineup_class.submit(game)
    performance = performance_class.submit(game)
    events = events_class.submit(game)
    timeline = timeline_class.submit(game)
    comments = comment_class.submit(game)
    trends = trends_class.submit(game)
    metadata = metadata_class.submit(game)
    statistics = statistics_class.submit(game)
    formations = formation_class.submit(game)
    coordinates = coordinate_class.submit(game)
    xg = xg_class.submit(game)
    scores = score_class.submit(game)

    s = dummy_step(wait_for=[
        fixture, participant, weather, lineup, performance,
        events, timeline, comments, trends, statistics,
        metadata, formations, coordinates, xg, scores
    ], game_id=game_id, game_name=fixture.result().game_name)

    #TODO: Add more Round details
