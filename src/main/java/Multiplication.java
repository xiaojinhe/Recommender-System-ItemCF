import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Multiplication uses two mappers to parse the input file, co-occurrence matrix and raw data file, into key-value
 * pairs, and uses a reducer to do the matrix unit cell multiplication to get the
 * subrating unit for each user:movieA, and generate the key-value pairs (user:movieA, subrating,movieB:movieA's
 * relation).
 *
 * In order to skip the calculation for the movies that each user has watched and rated, we use a setup method in the
 * reducer to collect all the users and their rated movie lists as key-value pair in hashmap. When go through each movie
 * to generate subrating for user:movie, we can skip the ones that the user has rated.
 *
 * Note: co-occurrence matrix * rating matrix per user = predicate rating matrix per user
 * To get the predicate for one movie (row index in co-occurrence matrix), we need to multiply all the relations that
 * the movie with other movies with the ratings for other movies respectively (each unit cell in the row represents the
 * movieA's relation with other movies, e.g. movieA:movieB's relation * movieB's rating) and sum them up and normalize
 * the sum-up rating with the total relation that used to calculate the sum-up rating(next job).
 *
 * Since for each movieA, we need to get all its relation with movieBs and multiply by movieB, so we can use movieB as
 * mapper's ouput key. Then, we can do the unit-multiplication in the reducer to get the subrating. After that, we
 * also need to set the reducer's output key to user:movieA. Because, we need to do the sum up all subrating and
 * normalization in the next job for each movieA for each user.
 *
 */
public class Multiplication {
	/**
	 * CoOccurrenceMapper takes co-occurrence matrix file as input.
	 * Input format for co-occurrence matrix: movieA:movieB \t relation
	 *
	 * outputKey: movieB(co-occurrence matrix's one column)
	 * outputValue: movieA(co-occurrence matrix's one row)=relation
	 *
	 */
	public static class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//output: key: movieB, value: movieA=relation
			String[] movieRelation = value.toString().trim().split("\t");
			String[] movies = movieRelation[0].split(":");
			context.write(new Text(movies[1]), new Text(movies[0] + "=" + movieRelation[1]));
		}
	}

	/**
	 * RatingMapper takes the raw data file (each line: user,movie,rating) and parse it into the key-value pairs
	 * outputKey: movieB
	 * outputValue: user:rating
	 *
	 */
	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] userMovieRatings = value.toString().trim().split(",");
			String user = userMovieRatings[0];
			String movie = userMovieRatings[1];
			String rating = userMovieRatings[2];
			context.write(new Text(movie), new Text(user + ":" + rating));
		}
	}

	/**
	 * MultiplicationReducer first sets up a hashmap that contains all the users with their rated movie lists for later
	 * looking up by parsing the re-format data file (format: user \t movie1:rating1,movie2:rating2,movie3:rating3...).
	 *
	 * The reducer method collects the data (movieA=relation..., userA:rating...) for each movieB, and do the unit-
	 * multiplication for each user:movie (movie:movieB relation * movieB's rating from the user),
	 * if the movie has not been rated by the user yet, and write out the result in the format of key-value pairs.
	 *
	 * outputKey: user:movieA
	 * outputValue: subrating,relation(movieA:movieB, for the normalization in the next job)
	 *
	 */
	public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

		private Map<String, Set<String>> userSeenMovie = new HashMap<String, Set<String>>();

		@Override
		public void setup(Context context) throws IOException {
			Configuration configuration = context.getConfiguration();
			String path = configuration.get("path", "");
			FileSystem fileSystem = FileSystem.get(configuration);

			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
					fileSystem.open(new Path(path + "/part-r-00000"))));

			String line = bufferedReader.readLine();

			while (line != null) {
				String[] userMovieRating = line.trim().split("\t");
				String user = userMovieRating[0];
				String[] movies = userMovieRating[1].split(",");
				Set<String> set = new HashSet<String>();
				for (String movie : movies) {
					set.add(movie.split(":")[0]);
				}
				userSeenMovie.put(user, set);
				line = bufferedReader.readLine();
			}

			bufferedReader.close();
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie and save them in the hashmaps, then do the multiplication
			//outputKey: user:movieA
			//outputValue: subRatingUnit,movieA:movieB'relation
			Map<String, Integer> movieRelation = new HashMap<String, Integer>();
			Map<String, Double> userRatings = new HashMap<String, Double>();
			for (Text value : values) {
				String val = value.toString().trim();
				if (val.contains("=")) {
					String[] movieAndRelation = val.split("=");
					movieRelation.put(movieAndRelation[0], Integer.parseInt(movieAndRelation[1]));
				} else {
					String[] userMovieRating = val.split(":");
					userRatings.put(userMovieRating[0], Double.parseDouble(userMovieRating[1]));
				}
			}

			for (Map.Entry<String, Integer> movieRelationEntry : movieRelation.entrySet()) {
				String movieA = movieRelationEntry.getKey();
				int relation = movieRelationEntry.getValue();
				for (Map.Entry<String, Double> userRatingsEntry : userRatings.entrySet()) {
					String user = userRatingsEntry.getKey();
					//check whether the user has rated movieA, if not, do the unit-multiplication
					if (!userSeenMovie.get(user).contains(movieA)) {
						double rating = userRatingsEntry.getValue();
						double subRating = relation * rating;
						context.write(new Text(user + ":" + movieA), new Text(subRating + "," + relation));
					}
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("path", args[3]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CoOccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CoOccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoOccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
