REGISTER Q4_formatGenre.jar;
movies = LOAD 'movies.dat' USING PigStorage(':') AS (MovieID:int,Title:chararray,Genre:chararray);
format_movies = FOREACH movies GENERATE Title , CONCAT(FORMAT_GENRE(Genre) , '$NetId');
STORE format_movies INTO 'Q4.res';