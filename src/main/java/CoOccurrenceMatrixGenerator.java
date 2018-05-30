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
import java.util.ArrayList;
import java.util.List;

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = userid \t movie1:rating,movie2:rating...
			//key = movieA:movieB value = 1
			//calculate each user rating list: <movieA, movieB>
			String[] movieRatings = value.toString().trim().split("\t")[1].split(",");
			/*List<String> movies = new ArrayList<String>();
			for (String movieRating : movieRatings) {
				movies.add(movieRating.split(":")[0]);
			}*/
			for (String movieRatingA : movieRatings) {
				String movieA = movieRatingA.split(":")[0];
				for (String movieRatingB : movieRatings) {
					String movieB = movieRatingB.split(":")[0];
					context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
				}
			}
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		int threshold = 25;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key movieA:movieB value = iterable<1, 1, 1>
			//calculate each two movies have been watched by how many people
			//outputKey: movieA:movieB
			//outputValue: co-occurrence times

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
