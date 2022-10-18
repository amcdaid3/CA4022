-- THIS FILE CONTAINS CODE FOR CLEANING DATA AND RUNNING QUERIES IN PIG.

-- Start by cleaning the MovieLens data
--Cleaning movies.csv file
movies = LOAD 'movies.csv' USING PigStorage(',') as (movieId:chararray, title:chararray, genres:chararray);

--remove the headers
movie = FILTER movies BY (movieId != 'movieId');

--Removing delimiters "" in the title
replace = FOREACH movie GENERATE movieId as movieId, REPLACE(title, '""','') as title, genres as genres;

--Seperating title from the year and seperating the genres
movies_cleaned = FOREACH replace GENERATE movieId as movieId,FLATTEN(SUBSTRING(title, 0, (int)SIZE(title)-7)) as title,FLATTEN(SUBSTRING(title, (int)SIZE(title)-5,(int)SIZE(title)-1)) as year, FLATTEN(STRSPLIT(genres, '\\|',0)) as genres;

--Store into csv in cleaned_csv folder
STORE movies_cleaned INTO 'movies_cleaned' USING PigStorage(',');

--Cleaning ratings.csv file
--Not including timestamp column as is not needed for the queries
ratings = LOAD 'ratings.csv' USING PigStorage(',') as (userId:chararray, movieId:chararray, rating:chararray);

--Remove headers from ratings
ratings_cleaned = FILTER ratings BY (userId != 'userId');

--file does not appear to need any more cleaning 
STORE ratings_cleaned INTO 'ratings_cleaned' USING PigStorage(','); 

--tags.csv and links.csv might not be needed for this assignment queries will leave until needed.

--JOINING MOVIES AND RATINGS DATA TOGETHER FOR THE COMPLEX HIVE QUERY.
--movies_cleaned and ratings_cleaned
--Join into one table
join_movies_ratings = JOIN movies_cleaned BY movieId, ratings_cleaned BY movieId;

movies_ratings = FOREACH join_movies_ratings GENERATE movies_cleaned::movieId as movieId, movies_cleaned::title as title, movies_cleaned::year as year, movies_cleaned::genres as genres, ratings_cleaned::rating as rating;

STORE movies_ratings INTO 'joined_movies_ratings' using PigStorage(',');

-- PIG QUERIES LOCALLY
-- QUERY 1
--Need to use movies_cleaned and ratings_cleaned.
--Need to get movieId and the rating from ratings_cleaned data
movie_ratings = FOREACH ratings_cleaned GENERATE (long)movieId as movieId, (float)rating as rating;

--Need to get movieId and title only from movies_cleaned data
movie_title = FOREACH movies_cleaned GENERATE (long)movieId AS movieId, title;

--Group the ratings using movieId
ratings_grouped = GROUP movie_ratings BY movieId;

--for each movieId count the ratings
ratings_count = FOREACH ratings_grouped GENERATE group AS movieId, (int)COUNT(movie_ratings.movieId) as rating_count;

--join the ratings_count data with the movies_cleaned data using movieId
joined_movies_ratings = JOIN ratings_count BY movieId RIGHT, movie_title BY movieId;

--Generate data with movieId, title, rating count
movies_ratings_count = FOREACH joined_movies_ratings GENERATE movie_title::movieId AS movieId, movie_title::title AS title, ratings_count::rating_count as rating_count;

--Order generated data into descending order and print first line.
ordered_ratings = ORDER movies_ratings_count BY rating_count DESC;

--Limit to show top line when Dump
top_rating = LIMIT ordered_ratings 1;

STORE top_rating INTO 'pig_query_1';

--QUERY 2
--Can use the same data as query above:
----movie_ratings -> movieId and rating
----movie_title -> movieId and title
----ratings_grouped -> group ratings using movieId 

movie_ratings = FOREACH ratings_cleaned GENERATE (long)movieId as movieId, (float)rating as rating;
movie_title = FOREACH movies_cleaned GENERATE (long)movieId as movieId, title;
ratings_grouped = GROUP movie_ratings BY movieId;

--USE THE AVERAGE OF ALL RATINGS FOR EACH MOVIE TITLE TO FIND THE ONES WITH ONLY FIVE STAR REVIEWS;
ratings_average = FOREACH ratings_grouped GENERATE group AS movieId,(float)AVG(movie_ratings.rating) as average_rating;

--TO RETURN TITLE WITH THESE 5 STAR OUTCOMES
average_movies_join = JOIN ratings_average BY movieId RIGHT, movie_title BY movieId;
average_movies = FOREACH average_movies_join GENERATE movie_title::movieId AS movieId, movie_title::title AS title, ratings_average::average_rating as average_rating;

--FILTER THE DATA TO RETURN MOVIES WITH AVERAGE REVIEW SCORE OF 5
five_star_movies = FILTER average_movies BY average_rating == 5.0;

STORE five_star_movies INTO 'pig_query_2';

-- QUERY 3
--ONLY NEED TO LOOK AT movie_ratings. DONT NEED TITLES.

user_ratings = FOREACH ratings_cleaned GENERATE (long)userId as userId, (float)rating as rating;

--GROUP THE DATA BY USERID
users_grouped = GROUP user_ratings BY userId;

--CALCULATE THE AVERAGE SUM OF RATINGS FOR EACH USERID
user_average = FOREACH users_grouped GENERATE group AS userId, (float)AVG(user_ratings.rating) AS average_rating;

--ORDER THE DATA DESCENDING BASED ON THE USER AVERAGE RATING
ordered_user = ORDER user_average BY average_rating DESC;

--Limit to show top line when Dump
top_rating_user = LIMIT ordered_user 1;

STORE ordered_user INTO 'pig_query_3';
