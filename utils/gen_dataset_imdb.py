import pandas as pd

ratings = pd.read_csv('https://datasets.imdbws.com/title.ratings.tsv.gz', delimiter='\t')

title_basics = pd.read_csv('https://datasets.imdbws.com/title.basics.tsv.gz',
                           delimiter='\t', na_values=['\\N'],
                           dtype={'startYear': 'Int64'})

episodes = pd.read_csv('https://datasets.imdbws.com/title.episode.tsv.gz',
                       delimiter='\t', na_values=['\\N'],
                       dtype={'seasonNumber': 'Int64', 'episodeNumber': 'Int64'})

ratings_pop = ratings[ratings.numVotes > 200000]
series_pop = (
    title_basics
    .query("titleType == 'tvSeries' and startYear >= 2000")
    .merge(ratings_pop, on='tconst')
    .filter(['tconst', 'primaryTitle'])
)
series_pop

episodes_pop = (
    series_pop
    .merge(episodes, left_on='tconst', right_on='parentTconst',
           suffixes=['Series', ''])
    .merge(title_basics[['tconst', 'primaryTitle', 'startYear', 'genres', 'runtimeMinutes']],
           on='tconst')
    .merge(ratings, on='tconst')
    .rename(columns={
        'tconstSeries': 'id_serie',
        'primaryTitle_x': 'nome_serie',
        'tconst': 'id_episodio',
        'primaryTitle_y': 'nome_episodio',
        'seasonNumber': 'temporada',
        'episodeNumber':  'num_episodio',
        'averageRating': 'avaliacao_media',
        'numVotes': 'num_votos',
        'startYear': 'ano',
        'genres': 'generos',
        'runtimeMinutes': 'duracao_minutos'
    })
    .filter(['id_serie', 'nome_serie', 'id_episodio', 'temporada',
             'num_episodio', 'nome_episodio', 'ano', 'generos', 'duracao_minutos',
             'avaliacao_media', 'num_votos'])
    .sort_values(by=['nome_serie', 'temporada', 'num_episodio'])
)

episodes_pop.to_csv('../dados/avaliacao_episodios_series_top.csv', index=False)
