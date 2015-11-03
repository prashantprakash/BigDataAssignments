movies = LOAD 'movies.dat' USING PigStorage(':') AS (MovieID:int,Title:chararray,Genre:chararray);
ratings = LOAD 'ratings.dat' USING PigStorage(':') AS (UserID:int,MovieID:int,Rating:double,Timestamp:chararray);
movies_ratings = COGROUP movies by MovieID INNER , ratings by MovieID INNER;
result = LIMIT movies_ratings 5 + (int)'$X';
STORE result INTO 'Q2.res';