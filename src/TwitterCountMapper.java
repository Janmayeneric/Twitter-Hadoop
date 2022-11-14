import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * extended mapper class
 * The mapper read a line from the temporary file that create by Query1 class
 * mapper scan the line, and get the count and screen_name and location separately
 * then assign the count as key and screen_name and location as value
 * @author xh20
 */
public class TwitterCountMapper extends Mapper<LongWritable, Text,LongWritable, Text>{

	/**
	 * <p>
	 * the key is the character offset within the file of the start of the line, ignored.(ScanWordsMapper.java)
	 * The value is a line from the file.(ScanWordsMapper.java)
	 * </p>
	 */
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		
		String line = value.toString();
		
		
		Boolean found = false;
			
		//default value set for the current scanning position, it search from last index to the front
		int pos = line.length() - 1;
			
		//assign the start position of the integer(exclusive)
		while (!found) {
			if (line.charAt(pos) == ')') {
				found = true;
			} else {
				
				// or back the position for 1 unit for another scan
				pos --;
			}
		
			// get the counting number
			int count = Integer.parseInt(line.substring(pos + 1, line.length()).trim());
			
			//assign the negative number, for descending order
			count = count * -1;
			String content = line.substring(0,pos + 1).trim();
			output.write(new LongWritable(count), new Text(content));
		}
	}
}
	
	

