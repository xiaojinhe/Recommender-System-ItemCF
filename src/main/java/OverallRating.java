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
import java.text.DecimalFormat;

/**
 * OverallRating takes the previous Multiplication job's output and sums up the subratings for the user:movie and
 * normalizes the sum-up ratings with total relation to get the final overall predicate of the movie for the user.
 *
 * input format: user:movieA \t subRatingUnit,movieB:movieA's relation
 * output format: user \t movie:predicate
 *
 */
public class OverallRating {

    /**
     * SubRatingMapper takes the subrating as input and parse it into key-value pairs
     *
     * outputKey: user:movieA
     * outputValue: subRating,movieA:movieB's relation
     */
    public static class SubRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyAndValue = value.toString().trim().split("\t");
            context.write(new Text(keyAndValue[0]), new Text(keyAndValue[1]));
        }
    }

    /**
     * OverallRatingReducer sums up the subRatings, calculates the total relation, and normalizes the sum-up rating
     * with total relation to get the predicate.
     *
     * inputKey: user:movieA
     * intputValue: subrating1,movieA:movieB's relation1, subrating2,movieA:movieC's relation2, ...
     *
     * outputKey: user
     * outputValue: movie:predicate
     *
     * Note: a threshold can be set to filter the predicates for user:movie that are lower than the threshold out
     * the output result.
     *
     */
    public static class OverallRatingReducer extends Reducer<Text, Text, Text, Text> {

        double threshold = 2.5;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int coOccurrenceNum = 0;
            double coOccurrenceSum = 0;
            double predicate;
            for (Text value : values) {
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

        job.setMapperClass(SubRatingMapper.class);
        job.setReducerClass(OverallRatingReducer.class);

        job.setJarByClass(OverallRating.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
