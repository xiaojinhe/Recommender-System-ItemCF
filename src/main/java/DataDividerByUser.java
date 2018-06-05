import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * DataDividerByUser takes the input format: user,movie,rating, and re-format it into the output format:
 * user \t movie1:rating1,movie2:rating2,movie3:rating3...
 */
public class DataDividerByUser {

	/**
	 * DataDividerMapper takes the input format: user,movie,rating, and divide the data by user.
	 * InputValue: one line from input file
	 * OutputKey: userID
	 * OutputValue: movie:rating
	 */
	public static class DataDividerMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] userMovieRating = value.toString().trim().split(",");
			if (userMovieRating.length < 2) {
				return;
			}
			String userID = userMovieRating[0];
			context.write(new Text(userID), new Text(userMovieRating[1] + ":" + userMovieRating[2]));
		}
	}

	/**
	 * DataDividerReducer will merge all the movies and ratings for each user, and generate the re-format data
	 * into a output file.
	 * InputKey: userID; InputValue: all the movie-rating pairs for the userID
	 * OutputKey: userID; OutputValue: movie1:rating1,movie2:rating2,movie3:rating3...
	 */
	public static class DataDividerReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//merge data for one user
			StringBuilder movieAndRatings = new StringBuilder();
			for (Text value : values) {
				movieAndRatings.append(value).append(",");
			}
			context.write(key, new Text(movieAndRatings.deleteCharAt(movieAndRatings.length() - 1).toString()));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDividerByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
