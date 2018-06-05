# Movie Recommender System

## Overview
In this project, I built a movie recommender system based on Item Collaborative Filtering using Apache Hadoop MapReduce in Java 

## Dataset
The datasets can be downloaded from below:
[MovieLens 100k dataset](https://grouplens.org/datasets/movielens/100k/)  
[MovieLens 1m dataset](https://grouplens.org/datasets/movielens/1m/)

The first dataset consists of:
	* 100,000 ratings (1-5) from 943 users on 1682 movies. 
	* Each user has rated at least 20 movies. 
  * Simple demographic info for the users (age, gender, occupation, zip)

The second dataset consists of:
  * 1,000,209 anonymous ratings of approximately 3,900 movies made by 6,040 MovieLens users.
Detailed information can be found on above links.

### Data preprocessing
Change the rating data format into the format of user,movie,rating

## Approach
Here I have followed the following steps to build the recommender system.

1) Divide the data by users.
2) Build item (movie) co-occurrence matrix.
3) Build movie rating matrix.
4) Multiply co-occurrence matrix unit with corresponding rating matrix unit. 
5) Merge the multiplication results for each movie per user and normalize the sum-up result to get the rating predict.
5) Generate topK recommendation list for each user.

## To Execute the program:
1) Install Hadoop
2) The raw data input format: user,movie,rating. 
3) Put the raw dataset file into Hadoop hdfs
4) Compile all the source codes and build a JAR
5) Run the algorithm using Hadoop with seven arguments 
(e.g. hadoop jar recommender.jar Driver args1 args2 args3 args4 args5 args6 args7(optional)
 - args1: the directory of a raw dataset file -- rawInput
 - args2: the directory of re-format data that divided by user -- userMovieListOutputDir
 - args3: the directory of co-occurrence matrix -- coOccurrenceMatrixDir
 - args4: the directory of unit-multiplication result -- multiplicationDir
 - args5: the directory of overall movie predicates for users -- overallRatingDir
 - args6: the directory of topK recommendation list per user --topKDir
 - args7: k for top k recommendation list calculation -- k
 6) The predicted rating results are saved to the overallRatingDir and the topK recommendation lists for users is saved to the 
 topKDir.
