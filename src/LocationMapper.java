import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue.ValueType;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * extended mapper class
 * The mapper read a line from the input file containing JSON object
 * mapper get the JSON object and process it , then map it and assign a default key to it (1)
 * @author xh20
 *
 */
public class LocationMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	
	
	//several constant use for comparator and assignment 
	public static final String IS_NULL = "\n\r";
	public static final String LINE = "\n";
	public static final String LINE2 = "\r";
	public static final String SPACE = " ";
	
	/**
	 * <p>
	 *  The LongWritable key is the character offset within the file of the start of the line, ignored.(ScanWordsMapper.java)
	 *  The value is a line from the file.(ScanWordsMapper.java)
	 * </p>
	 */
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		
		
		String line = value.toString();
			String strLocation = this.getLocation(line);
			if (!strLocation.equals(IS_NULL)) {
				output.write(new Text(strLocation), new LongWritable(1));
			}
	}
	
	
	
	/**
	 * function developed from TwitterMapper
	 * function takes the reading line from the file and output the location in string form
	 * @param line the read line from file
	 * @return location 
	 */
	private String getLocation(String line) {
		JsonReader reader = Json.createReader(new StringReader(line));
		JsonObject tweetObject = reader.readObject();
		
		// check whether the object contains the certain keys
		if (!tweetObject.containsKey("user")) {
			return IS_NULL;
		}
		
		// get object by keyword "user"
		JsonObject userObject = tweetObject.getJsonObject("user");
		
		if (!userObject.containsKey("location")) {
			return IS_NULL;
		}
		
		if (userObject.get("location").getValueType() == ValueType.NULL) {
			return IS_NULL;
		}
		
		StringBuilder sb = new StringBuilder(userObject.getString("location"));
		sb = this.lineReplace(sb);
		return sb.toString();
	}
	
	/**
	 * replace spurious line feed and carriage return with a space character
	 * @param sb1 StringBuilder with the string might have spurious line feed and carriage
	 * @return adjusted StringBuilder that escape the spurious line feed and carriage returns 
	 */
	private StringBuilder lineReplace(StringBuilder sb1) {
		if (sb1.indexOf(LINE) > -1) {
			int index = 0;
			while (index < sb1.length()) {
				if (sb1.substring(index, index + 1).toString().equals(LINE) | sb1.substring(index, index + 1).toString().equals(LINE2)) {
					if (index > 0) {
						if (sb1.charAt(index - 1) != '\\' | sb1.charAt(index - 1) != '/') {
							sb1.replace(index, index + 1, SPACE);
						}
					}else {
						sb1.replace(index, index + 1, SPACE);
					}
				}
				index ++;
			}
		}
		return sb1;
	}
}
