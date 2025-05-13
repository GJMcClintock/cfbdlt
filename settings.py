START_YEAR = 1869
END_YEAR = 2025
BASE_URL = 'https://apinext.collegefootballdata.com'

ADD_YEAR = ['teams','rosters']

# (table name, endpoint, merge key, start year)
SEASON_ENDPOINTS = [
    ('game_media','games/media',('id'),2003),
    ('game_weather','games/weather',('id'),2001),
    ('team_records','records',('year','team_id'),1876),
    ('drives','drives',('id'),2001),
    ('teams','teams',('year','id'),1876),
    ('rosters','roster',('year','id'),2004),
    ('team_talent','talent',('year','team'),2015),
    ('coaches','coaches',('first_name','last_name','year','school'),1888),
    ('transfers','player/portal',('season','first_name','last_name','origin','transfer_date'),2021),
    ('player_usage','player/usage',('season','id'),2013),
    ('player_returning_production','player/returning',('season','team'),2014),
    ('rankings','rankings',('season','season_type','week'),1936),
    ('recruits','recruiting/players',('id'),2000),
    ('team_recruiting','recruiting/teams',('year','team'),2000),
    ('ratings_sp','ratings/sp',('year','team'),1970),
    ('ratings_srs','ratings/srs',('year','team'),1897),
    ('ratings_elo','ratings/elo',('year','team'),1897),
    ('ratings_fpi','ratings/fpi',('year','team'),2005),
    ('player_season_stats','stats/player/season',('player_id','season','category','stat_type'),2004),
    ('team_season_stats','stats/season',('season','team','stat_name'),2004),
    ('season_advanced_stats','stats/season/advanced',('season','team'),2001),
    ('draft_picks','draft/picks',('year','overall'),1966),
    ('adj_team_season_stats','wepa/team/season',('year','team_id'),2008),
    ('adj_player_passing','wepa/players/passing',('year','athlete_id'),2013),
    ('adj_player_rushing','wepa/players/rushing',('year','athlete_id'),2013),
    ('adj_player_kicking','wepa/players/kicking',('year','athlete_id'),2016),
    ('games','games',('id'),1869),
    ('season_calendar','calendar',('start_date'),2002)
]

STATIC_ENDPOINTS = [
    ('play_types','plays/types'),
    ('play_stat_types','plays/stats/types'),
    ('conferences','conferences'),
    ('venues','venues'),
    ('ppa_kicking','metrics/fg/ep'),
    ('stat_categories','stats/categories'),
    ('draft_teams','draft/teams'),
    ('draft_positions','draft/positions')
]