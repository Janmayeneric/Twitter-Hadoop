import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * <p>
 * The method will produce an output file in which each line contains location 
 * and the number of tweets related to here  alphabetic order
 * </p>
 * @author xh20
 *
 */
public class Query3 {
	
	public void operate(String input_path, String output_path) throws IOException {
		// (From WordCount.java) Setup new Job and Configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Count");
				
		// (From WordCount.java)Specify input and output paths
		FileInputFormat.setInputPaths(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
				
		// (From WordCount.java)Set our own TwitterMapper as the mapper
		job.setMapperClass(LocationMapper.class);

		// (From WordCount.java)Specify output types produced by mapper (words with count of 1)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
				
		// (From WordCount.java)The output of the reducer is a map from unique words to their total counts.
		job.setReducerClass(TwitterReducer.class);
				
		// (From WordCount.java)Specify the output types produced by reducer (words with total counts)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
				
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}


}
