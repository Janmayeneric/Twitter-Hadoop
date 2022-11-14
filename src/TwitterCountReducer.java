import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * extended mapper class
 * The reducer take the map from unique words/sentences with count and shuffle them in order
 * @author xh20
 *
 */
public class TwitterCountReducer extends Reducer<LongWritable, Text, LongWritable , Text>{


	/**
	 * <p>
	 * The key is the counting number 
	 * The value are the screen_name and location
	 * </p>
	 */
	public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
			
		for (Text value : values) {
			long count = key.get();
			
			// count is originally negative number for descending order , change back to positive for the output demonstration
			count = -1 * count;
			output.write(new LongWritable(count), value);
		}
	}
}
