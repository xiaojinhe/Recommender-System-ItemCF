import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by Michelle on 11/12/16.
 */
public class Sum {

    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input format: user:movieA \t subRatingUnit,movieB:movieA's relation
            String[] keyAndValue = value.toString().trim().split("\t");
            context.write(new Text(keyAndValue[0]), new Text(keyAndValue[1]));
        }
    }

    public static class UserRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: user,movie,rating
            String[] userMovieRatings = value.toString().trim().split(",");
            String user = userMovieRatings[0];
            String movie = userMovieRatings[1];
            context.write(new Text(user + ":" + movie), new Text("seen"));
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {

        double threshold = 3.2;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key: user:movieA value: subRatingUnit,movieB:movieA's relation or seen
            //calculate the sum and normalize it to get the predicate

            int coOccurrenceNum = 0;
            double coOccurrenceSum = 0;
            double predicate;
            for (Text value : values) {
                if (value.toString().trim().equals("seen")) {
                    return;
                }
                String[] unitAndRelation = value.toString().trim().split(",");
                double subRatingUnit = Double.parseDouble(unitAndRelation[0]);
                int relation = Integer.parseInt(unitAndRelation[1]);
                coOccurrenceSum += subRatingUnit;
                coOccurrenceNum += relation;
            }

            DecimalFormat df = new DecimalFormat("#.000");
            predicate = Double.valueOf(df.format(coOccurrenceSum / coOccurrenceNum));

            String[] userAndMovie = key.toString().split(":");
            if (predicate > threshold) {
                context.write(new Text (userAndMovie[0]), new Text(userAndMovie[1] + ":" + predicate));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        ChainMapper.addMapper(job, SumMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, UserRatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
        job.setMapperClass(SumMapper.class);
        job.setMapperClass(UserRatingMapper.class);

        job.setReducerClass(SumReducer.class);

        job.setJarByClass(Sum.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SumMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserRatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
