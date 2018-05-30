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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieA:movieB \t relation
			//output: key: movieB, value: movieA=relation
			String[] movieRelation = value.toString().trim().split("\t");
			String[] movies = movieRelation[0].split(":");
			context.write(new Text(movies[1]), new Text(movies[0] + "=" + movieRelation[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: user,movie,rating
			//outputKey: movie
			//outputValue: userID,movie:rating
			String[] userMovieRatings = value.toString().trim().split(",");
			String user = userMovieRatings[0];
			String movie = userMovieRatings[1];
			String rating = userMovieRatings[2];
			context.write(new Text(movie), new Text(user + ":" + rating));
		}
	}

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
			//collect the data for each movie, then do the multiplication
			//outputKey: user:movie
			//outputValue: subRatingUnit,movieB:movieA'relation
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
					if (!userSeenMovie.get(user).contains(movieA)) {
						double rating = userRatingsEntry.getValue();
						double subRating = relation * rating;
						context.write(new Text(user + ":" + movieA), new Text(subRating + "," + relation));
					}
				}
			}

			/*for (Map.Entry<String, Integer> movieRelationEntry : movieRelation.entrySet()) {
				String movieA = movieRelationEntry.getKey();
				int relation = movieRelationEntry.getValue();
				for (Map.Entry<String, Double> userRatingsEntry : userRatings.entrySet()) {
					String userID = userRatingsEntry.getKey();
					double rating = userRatingsEntry.getValue();
					double subRating = relation * rating;
					context.write(new Text(userID + ":" + movieA), new Text(subRating + "," + relation));
				}
			}*/
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("path", args[3]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
