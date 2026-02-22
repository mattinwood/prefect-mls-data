import asyncio
import configparser
from pydantic import BaseModel, Field, AliasPath, model_validator
import httpx
import sys
from typing import Dict, NewType, Optional
from prefect_mls_data.tools.logconfig import get_logger
from datetime import datetime
from decimal import Decimal

LOGGER = get_logger()
def set_logger(logger):
    global LOGGER
    LOGGER = logger

CONFIG = configparser.ConfigParser()
CONFIG.read('/data/prefect_mls_data/tools/config.ini')
TOKEN = CONFIG['SPORTSMONKS']['API_KEY']
BASE_URL = f'https://api.sportmonks.com/v3/football/'
ISODate = NewType('ISODate', str)

def gen_url() -> str:
    url = BASE_URL + product + f'{endpoint}?api_token={TOKEN}'
    url += '&timezone=America/Chicago'
    return url


def paginated_results(
        client: httpx.Client,
        endpoint: str,
        includes: list[str] = None,
        filters: Dict[str, str] = None,
        selects: list[str] = None,
        pagination: str = '') -> dict:
    """
    Fetches and combines data from all pages of a paginated API endpoint.
    This function sends GET requests to the input URL, and if the response contains
    a link to the next page, it sends another request to fetch the data from
    the next page. This process is repeated until all pages have been fetched.
    The function returns a dictionary that includes data from all pages, as well
    as API subscription details, API rate limit details, and the time zone.

    Parameters:
    url (str): The URL of the API endpoint.

    Returns:
    dict: A dictionary that includes 'data' which is a list of dictionaries each
    representing an entity from the fetched pages. Other keys in the dictionary
    include 'subscription', 'rate_limit' and 'timezone'.
    """
    all_records = []
    params = {
        'timezone': 'America/Chicago',
        'page': 1
    }
    url = BASE_URL + endpoint
    if includes:
        params['includes'] = ';'.join(includes)
    if filters:
        params['filters'] = ';'.join(filters)
    if selects:
        params['selects'] = ';'.join(selects)

    while True:
        LOGGER.info(f'Pulling page {len(all_records) + 1} of results')
        LOGGER.debug(f'Request URL: {httpx.URL(url, params=params | {'api_token': TOKEN})}')
        LOGGER.info(f'Safe Request URL: {httpx.URL(url, params=params)}')
        response = client.get(url, params=params | {'api_token': TOKEN})

        data = response.json()
        if 'data' not in data.keys():
            LOGGER.warning(f'{data["message"]}')
            return None

        LOGGER.info(f'Returned {len(data['data']) if type(data['data']) == list else 1} records')
        all_records.append(data.get('data', []))

        has_more = data.get('pagination', {}).get('has_more', False)
        LOGGER.debug(f'Has more pages: {has_more}')

        ratelimit = data['rate_limit']['remaining']

        if ratelimit <= 50:
            LOGGER.critical(f'{ratelimit} requests remaining; terminating program')
            sys.exit(1)
        elif ratelimit <= 500:
            LOGGER.warning(f'{ratelimit} requests remaining')

        if has_more:
            params['page'] += 1

        elif not has_more:
            LOGGER.info('No more pages to fetch')
            break

    output = {
        'data': all_records,
        'subscription': data.get('subscription'),
        'rate_limit': data.get('rate_limit'),
        'timezone': data.get('timezone'),
    }

    LOGGER.info(f'Total records returned: {len(output['data'])}')

    return output


def get_fixture_details(fixture_id: int, includes: list=None, filters: list=None) -> dict:
    if includes is None:
        includes = [
            'participants',
            'season',
            'stage.groups',  # Group Name includes Conference, two for inter-conference. Nothing for playoffs
            'venue',
            'weatherReport',
            'lineups.player', 'lineups.position', 'lineups.detailedPosition', 'lineups.type', 'lineups.details.type',
            'events.type', 'events.subtype',
            'timeline.participant',
            'comments',  # interesting combo with the timeline?
            'trends.participant', 'trends.type', 'trends.period', # Type is recommended to avoid...?
            'statistics.participant', 'statistics.type',  # Type is recommended to avoid...?
            'metadata.Type',  # HAS ATTENDANCE!!!
            'sidelined.player', 'sidelined.type',  # Type is recommended to avoid...?
            'referees.referee',
            'formations.participant',
            'ballCoordinates.period',
            'xGFixture.type', 'xGFixture.participant',
            'aggregate', 'scores.participant'
        ]
        filters = ['scoreTypes:1525']
    with httpx.Client(timeout=30) as client:
        fixture = paginated_results(
            client,
            f'fixtures/{fixture_id}',
            includes=includes,
            filters=filters
        )

    return fixture


def get_fixture_ids(date_range_start=None, date_range_end=None):
    if not all([date_range_start, date_range_end]):
        from datetime import date
        from dateutil.relativedelta import relativedelta
        date_range_start = (date.today() - relativedelta(days=14)).isoformat()
        print(f'Date Range Start: {date_range_start}')
        date_range_end = date.today().isoformat()
        print(f'Date Range Start: {date_range_end}')
    with httpx.Client() as client:
        response = paginated_results(
            client,
            f'fixtures/between/{date_range_start}/{date_range_end}',
            selects= ['id'],
            filters = ['fixtureLeagues:779', 'fixtureStates:5']
        )
    if response is None:
        return None
    else:
        return [fixture['id'] for sublist in response['data'] for fixture in sublist]


### Pydantic Data Classes
class FixtureDetails(BaseModel):
    fixture_id: int = Field(alias='id')
    season: str = Field(alias=AliasPath('season', 'name'))
    stage: str = Field(alias=AliasPath('stage', 'name'))
    leg: str
    game_name: str = Field(alias='name')
    results: str = Field(alias='result_info')
    kickoff: datetime = Field(alias='starting_at')

    @model_validator(mode='after')
    def isooutput(self):
        if type(self.kickoff) == datetime:
            self.kickoff = self.kickoff.isoformat()
        return self


class ParticipantDetails(BaseModel):
    participant_id: int = Field(alias='id')
    fixture_id: int
    participant_name: str = Field(alias='name')
    participant_logo_url: str = Field(alias='image_path')
    home_away: str = Field(alias=AliasPath('meta', 'location'))


class FixtureParticipants(BaseModel):
    participants: list[ParticipantDetails]


class FixtureWeather(BaseModel):
    weather_id: int = Field(alias='id')
    fixture_id: int

    morning_temperature: Decimal = Field(alias=AliasPath('temperature', 'morning'), decimal_places=2)
    day_temperature: Decimal = Field(alias=AliasPath('temperature', 'day'), decimal_places=2)
    evening_temperature: Decimal = Field(alias=AliasPath('temperature', 'evening'), decimal_places=2)
    night_temperature: Decimal = Field(alias=AliasPath('temperature', 'night'), decimal_places=2)

    morning_feels_like: Decimal = Field(alias=AliasPath('feels_like', 'morning'), decimal_places=2)
    day_feels_like: Decimal = Field(alias=AliasPath('feels_like', 'day'), decimal_places=2)
    evening_feels_like: Decimal = Field(alias=AliasPath('feels_like', 'evening'), decimal_places=2)
    night_feels_like: Decimal = Field(alias=AliasPath('feels_like', 'night'), decimal_places=2)

    wind_speed: Decimal = Field(alias=AliasPath('wind', 'speed'), decimal_places=2)
    wind_direction: int = Field(alias=AliasPath('wind', 'direction'))


class LineupDetails(BaseModel):
    lineup_id: int = Field(alias='id')
    fixture_id: int
    player_name: str
    player_id: int|None
    formation_field: str|None
    formation_position: int|None
    position: str|None = Field(alias=AliasPath('position', 'name'), default=None)
    detailed_position: str|None = Field(alias=AliasPath('detailedposition', 'name'), default=None)
    starting_lineup: str = Field(alias=AliasPath('type', 'name'))

    @model_validator(mode='after')
    def starter(self):
        if self.starting_lineup == 'Lineup':
            self.formation_field = 'Starter'
        return self


class FixtureLineups(BaseModel):
    lineup: list[LineupDetails]


class PerformanceDetails(BaseModel):
    performance_id: int = Field(alias='id')
    fixture_id: int
    lineup_id: int|None
    player_id: int|None
    statistic: str = Field(alias=AliasPath('type', 'name'))
    value: Decimal|int|bool = Field(alias=AliasPath('data', 'value'))

    @model_validator(mode='after')
    def coerce_string(self):
        self.value = str(self.value)
        return self


class PlayerPerformance(BaseModel):
    details: list[PerformanceDetails]


class FixturePerformance(BaseModel):
    performance: list[PlayerPerformance]

class EventDetails(BaseModel):
    event_id: int = Field(alias='id')
    fixture_id: int
    participant_id: int
    player_id: int|None
    related_player_id: int|None
    minute: int
    extra_minute: int|None
    info: str|None
    addition: str|None
    injured: bool|None
    on_bench: bool|None
    event_type: str = Field(alias=AliasPath('type', 'name'))
    subtype: str|None = Field(alias=AliasPath('subtype', 'name'), default=None)
    sort_order: int


class EventList(BaseModel):
    events: list[EventDetails]

class TimelineDetails(BaseModel):
    event_id: int = Field(alias='id')
    fixture_id: int
    participant_id: int
    player_id: int|None
    related_player_id: int|None
    minute: int | None
    extra_minute: int | None
    info: str | None
    addition: str | None
    injured: bool | None
    on_bench: bool | None
    event_type: str = Field(alias=AliasPath('type', 'name'), default=None)
    subtype: str | None = Field(alias=AliasPath('subtype', 'name'), default=None)
    sort_order: int

class Timeline(BaseModel):
    timeline: list[TimelineDetails]

class Comment(BaseModel):
    comment_id: int = Field(alias='id')
    fixture_id: int
    comment: str
    minute: int | None
    extra_minute: int | None
    is_goal: bool
    is_important: bool
    sort_order: int = Field(alias='order')

class Comments(BaseModel):
    comments: list[Comment]

class Trend(BaseModel):
    trend_id: int = Field(alias='id')
    fixture_id: int
    participant: str = Field(alias=AliasPath('participant', 'name'))
    minute: int|None
    period: int|None = Field(alias=AliasPath('period','type_id'))
    trend_type: str = Field(alias=AliasPath('type', 'name'))
    value: Decimal

class Trends(BaseModel):
    trends: list[Trend]

class Statistic(BaseModel):
    statistic_id: int = Field(alias='id')
    fixture_id: int
    participant: str = Field(alias=AliasPath('participant', 'name'))
    statistic: str = Field(alias=AliasPath('type', 'name'))
    value: int|None = Field(alias=AliasPath('data', 'value'))

    @model_validator(mode='after')
    def coerce_nulls(self):
        if not self.value:
            self.value = 0
        self.value = str(self.value)
        return self

class Statistics(BaseModel):
    statistics: list[Statistic]

class Metadatum(BaseModel):
    metadata_id: int = Field(alias='id')
    metadata_name: str = Field(alias=AliasPath('type', 'name'))
    fixture_id: int = Field(alias='metadatable_id')
    value_type: str
    values: str | dict | None

class Metadata(BaseModel):
    metadata: list[Metadatum]

class Formation(BaseModel):
    formation_id: int = Field(alias='id')
    fixture_id: int
    participant_id: int
    participant_name: str = Field(alias=AliasPath('participant', 'name'))
    formation: str
    home_away: str = Field(alias='location')

class Formations(BaseModel):
    formations: list[Formation]

class Coordinate(BaseModel):
    coordinate_id: int = Field(alias='id')
    fixture_id: int
    period: int|None = Field(alias=AliasPath('period','type_id'))
    timer: str
    x: float
    y: float

class Coordinates(BaseModel):
    coordinates: list[Coordinate]

class ParticipantXG(BaseModel):
    xg_id: int = Field(alias='id')
    fixture_id: int
    participant_name: str = Field(alias=AliasPath('participant', 'name'))
    home_away: str = Field(alias='location')
    xg_type: str = Field(alias=AliasPath('type', 'name'))
    xg_value: float = Field(alias=AliasPath('data', 'value'))

class FixtureXG(BaseModel):
    xg: list[ParticipantXG]

class Score(BaseModel):
    score_id: int = Field(alias='id')
    fixture_id: int
    participant_name: str = Field(alias=AliasPath('participant', 'name'))
    home_away: str = Field(alias=AliasPath('score', 'participant'))
    score: int = Field(alias=AliasPath('score', 'goals'))

class Scores(BaseModel):
    scores: list[Score]