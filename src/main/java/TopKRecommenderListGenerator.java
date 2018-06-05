import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * TopKRecommenderListGenerator takes the movie predicate results for users as input, and computes the top k movie
 * predicates for each user.
 *
 * input format: userID \t movie:predicate
 * output format: outputKey: user; outputValue: top k movie predicates
 */
public class TopKRecommenderListGenerator {

    /**
     * TopKRecommenderListMapper parses the movie predicates for users into key-value pairs.
     * outputKey: user
     * outputValue: movie:predicate
     */
    public static class TopKRecommenderListMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input format: user \t movie:predicate
            String[] tokens = value.toString().trim().split("\t");
            context.write(new Text(tokens[0]), new Text(tokens[1]));
        }
    }

    /**
     * TopKRecommenderListReducer takes in the k value from the argument (command line) or use the default value of 5.
     * Each input value (movie1:predicate1) is wrapped into PredicateData object. The reducer uses a PriorityQueue to
     * compute the top k movie predicates for each user.
     */
    public static class TopKRecommenderListReducer extends Reducer<Text, Text, Text, Text> {

        private int k;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //input format: key: userID, values: movie1:predicate1, movie2:predicate2...
            PriorityQueue<PredicateData> pq = new PriorityQueue<PredicateData>();

            for (Text value : values) {
                String[] movieAndPredicate = value.toString().trim().split(":");
                double predicate = Double.parseDouble(movieAndPredicate[1]);
                pq.offer(new PredicateData(movieAndPredicate[0], predicate));

                if (pq.size() > k) {
                    pq.poll();
                }
            }

            while (!pq.isEmpty()) {
                PredicateData predicateData = pq.poll();
                context.write(key, new Text(predicateData.getMovie() + ":" + predicateData.getPredicate()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("k", args[2]);

        Job job = Job.getInstance(conf);
        job.setMapperClass(TopKRecommenderListMapper.class);
        job.setReducerClass(TopKRecommenderListReducer.class);

        job.setJarByClass(TopKRecommenderListGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
