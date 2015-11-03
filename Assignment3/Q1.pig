movies = LOAD 'movies.dat' USING PigStorage(':') AS (MovieID:int,Title:chararray,Genre:chararray);
ratings = LOAD 'ratings.dat' USING PigStorage(':') AS (UserID:int,MovieID:int,Rating:double,Timestamp:chararray);
users = LOAD 'users.dat' USING PigStorage(':') AS (UserID:int,Gender:chararray,Age:int,Occupation:int,Zipcode:chararray);

filteredusers= FILTER users BY Age >=20 and Age<=40 AND Gender=='M';
selectedmovies = FILTER movies BY Genre MATCHES '.*Comedy.*' AND Genre MATCHES '.*Drama*.';

/*
* get the lowest rated Comedy and Drama  movies
*/

join_selectedmovies_ratings = JOIN selectedmovies by MovieID , ratings by MovieID;
group_join_selectedmovies_ratings = GROUP join_selectedmovies_ratings by ratings::MovieID;
avg_group_join_selectedmovies_ratings = FOREACH group_join_selectedmovies_ratings GENERATE group as MovieID , AVG(join_selectedmovies_ratings.ratings::Rating) as averageRating;
sorted = ORDER avg_group_join_selectedmovies_ratings BY averageRating ASC;
lowestrating = LIMIT sorted 1;

/*
* list all userids
*/

join_lr_ratings = JOIN lowestrating by MovieID , ratings by MovieID;
join_lr_ratings_filteredusers = JOIN join_lr_ratings by ratings::UserID , filteredusers by UserID;
temp = FOREACH join_lr_ratings_filteredusers GENERATE filteredusers::UserID as userID;
result = DISTINCT temp;

join_result_users = JOIN result by userID , users by UserID;
userzip = FILTER join_result_users by (users::Zipcode MATCHES CONCAT('$X','.*'));
STORE userzip INTO 'Q1.res';

