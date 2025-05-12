START_YEAR = 2022
END_YEAR = 2024
BASE_URL = 'https://apinext.collegefootballdata.com'

SEASON_ENDPOINTS = [
    ('games','games',None,None),
    ('game_media','games/media',None,None),
    ('game_weather','games/weather',None,None),
    ('team_records','records',None,None),
    ('season_calendar','calendar',None,None),
    ('drives','drives',None,None),
    ('teams','teams',None,None),
    ('rosters','roster',None,None),
    ('team_talent','talent',None,None),
    ('coaches','coaches',None,None),
    ('transfers','player/portal',None,None),
    ('player_usage','player/usage',None,None),
    ('player_returning_production','player/returning',None,None),
    ('rankings','rankings',None,None),
    ('recruits','recruiting/players',None,None),
    ('team_recruiting','recruiting/teams',None,None),
    ('ratings_sp','ratings/sp',None,None),
    ('ratings_srs','ratings/srs',None,None),
    ('ratings_elo','ratings/elo',None,None),
    ('ratings_fpi','ratings/fpi',None,None),
    ('player_season_stats','stats/player/season',None,None),
    ('team_season_stats','stats/season',None,None),
    ('season_advanced_stats','stats/season/advanced',None,None),
    ('draft_picks','draft/picks',None,None),
    ('adj_team_season_stats','wepa/team/season',None,None),
    ('adj_player_passing','wepa/players/passing',None,None),
    ('adj_player_rushing','wepa/players/rushing',None,None),
    ('adj_player_kicking','wepa/players/kicking',None,None)
]