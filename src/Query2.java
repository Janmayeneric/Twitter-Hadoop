import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Basically it uses the code from WordCount.java in example code
 * Except some changing on the class name for specific program
 * it is a query class that is called by the main program
 * @author xh20
 *
 */
public class Query2 {

	/**
	 * <p>
	 * The method will produce an output file in which line contains
	 * a combination of screen_name and location followed by the total
	 * number of tweets in all input files in number order by their counting number
	 * </p>
	 * @param input_path
	 * @param output_path
	 * @throws IOException
	 */
	public void operate(String input_path, String output_path) throws IOException {
		// (From WordCount.java) Setup new Job and Configuration
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "userCount");
		
		// (From WordCount.java)Specify input and output paths
		FileInputFormat.setInputPaths(job2, new Path(input_path));
		FileOutputFormat.setOutputPath(job2, new Path(output_path));
		
		// (From WordCount.java)Set our own TwitterMapper as the mapper
		job2.setMapperClass(TwitterCountMapper.class);

		// (From WordCount.java)Specify output types produced by mapper (words with count of 1)
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
				
		// (From WordCount.java)The output of the reducer is a map from unique words to their total counts.
		job2.setReducerClass(TwitterCountReducer.class);
				
		// (From WordCount.java)Specify the output types produced by reducer (words with total counts)
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		
		try {
			job2.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}
}
