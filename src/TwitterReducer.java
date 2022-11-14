import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * <p>
 * extended mapper class
 * The reducer take the map from unique words/sentences to their total count(CountWordsReducer)
 * </p>
 * @author xh20
 *
 */
public class TwitterReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	

	/**
	 * <p>
	 * The key is the word.(CountWordsReducer.java)
	 * (CountWordsReducer.java) The values are all the counts associated with that word (commonly one copy of '1' for each occurrence).
	 * </p>
	 */
	public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {
		int sum =0;
		for(LongWritable value : values) {
			long l = value.get();
			sum += l;
		}
		output.write(key, new LongWritable(sum));
	}
}
