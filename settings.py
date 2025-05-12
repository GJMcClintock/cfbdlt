START_YEAR = 1869
END_YEAR = 2024
BASE_URL = 'https://apinext.collegefootballdata.com'

ADD_YEAR = ['teams','rosters']

# (table name, endpoint, merge key, parent transformer)
SEASON_ENDPOINTS = [
    ('season_calendar','calendar',('start_date')),
    ('games','games',('id')),
    ('game_media','games/media',('id')),
    ('game_weather','games/weather',('id')),
    ('team_records','records',('year','team_id')),
    ('drives','drives',('id')),
    ('teams','teams',('year','id')),
    ('rosters','roster',('year','id')),
    ('team_talent','talent',('year','team')),
    ('coaches','coaches',('first_name','last_name','year','school')),
    ('transfers','player/portal',('season','first_name','last_name','origin','transfer_date')),
    ('player_usage','player/usage',('season','id')),
    ('player_returning_production','player/returning',('season','team')),
    ('rankings','rankings',('season','season_type','week')),
    ('recruits','recruiting/players',('id')),
    ('team_recruiting','recruiting/teams',('year','team')),
    ('ratings_sp','ratings/sp',('year','team')),
    ('ratings_srs','ratings/srs',('year','team')),
    ('ratings_elo','ratings/elo',('year','team')),
    ('ratings_fpi','ratings/fpi',('year','team')),
    ('player_season_stats','stats/player/season',('player_id','season','category','stat_type')),
    ('team_season_stats','stats/season',('season','team','stat_name')),
    ('season_advanced_stats','stats/season/advanced',('season','team')),
    ('draft_picks','draft/picks',('year','overall')),
    ('adj_team_season_stats','wepa/team/season',('year','team_id')),
    ('adj_player_passing','wepa/players/passing',('year','athlete_id')),
    ('adj_player_rushing','wepa/players/rushing',('year','athlete_id')),
    ('adj_player_kicking','wepa/players/kicking',('year','athlete_id'))
]

STATIC_ENDPOINTS = [
    ('play_types','plays/types'),
    ('play_stat_types','plays/stats/types'),
    ('conferences','conferences'),
    ('vanues','venues'),
    ('ppa_kicking','metrics/fg/ep'),
    ('stat_categories','stats/categories'),
    ('draft_teams','draft/teams'),
    ('draft_positions','draft/positions')
]