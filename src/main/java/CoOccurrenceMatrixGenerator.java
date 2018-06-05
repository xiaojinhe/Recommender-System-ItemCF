import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * CoOccurrenceMatrixGenerator takes input file with the format: userid \t movie1:rating1,movie2:rating2...,
 * and generate co-occurrence matrix representing with every unit cell of the matrix
 * (format: movieA:movieB \t co-occurrence times).
 */
public class CoOccurrenceMatrixGenerator {
	/**
	 * MatrixGeneratorMapper takes the input file and generate the co-occurrence relation of all the movies rated by the
	 * user. If two movies rated by one user, the two movies will have 1 co-occurrence relation (similarity).
	 * We loop through all the movies rated by the user and generate one-to-one relation for all the movies,
	 * including the movie and itself's relation.
	 *
	 * inputValue format: userid \t movie1:rating1,movie2:rating2...
	 * outputKey: movieA:movieB
	 * outputValue: 1
	 *
	 */
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//calculate each user rating list: <movieA, movieB>
			String[] movieRatings = value.toString().trim().split("\t")[1].split(",");
			for (String movieRatingA : movieRatings) {
				String movieA = movieRatingA.split(":")[0];
				for (String movieRatingB : movieRatings) {
					String movieB = movieRatingB.split(":")[0];
					context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
				}
			}
		}
	}

	/**
	 * MatrixGeneratorReducer summarizes the co-occurrence relation for each two movies, and generates the
	 * key-value pair for each two movies and co-occurrence times.
	 * The reducer can also set the threshold to filter the co-occurrence relation of two movies that less than the
	 * threshold out from the output.
	 *
	 * inputKey: movieA:movieB
	 * inputValue: iterable<1, 1, 1,...>
	 * outputKey: movieA:movieB
	 * outputValue: co-occurrence relation
	 *
	 */
	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		int threshold = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key movieA:movieB value = iterable<1, 1, 1>
			//calculate each two movies have been watched by how many people
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			if (sum > threshold) {
				context.write(key, new IntWritable(sum));
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
