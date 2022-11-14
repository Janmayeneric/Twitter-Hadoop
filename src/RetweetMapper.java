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
 * <p>
 * extended mapper class
 * The mapper read a line from the input file containing JSON object
 * mapper get the JSON object and process it , then map it and assign a default key to it (1)
 * </p>
 * @author xh20
 *
 */
public class RetweetMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	////several constant use for comparator and assignment 
	public static final String IS_NULL = "\n\r";
	public static final String LINE = "\n";
	public static final String LINE2 = "\r";
	public static final String SPACE = " ";
	public static final String TEXT_FIELD = "text";
	public static final String LOCATION_FIELD = "location";
	public static final String USER_FIELD = "user";
	public static final String RETWEETED_STATUS = "retweeted_status";
	
	
	/**
	 * <p>
	 * The LongWritable key is the character offset within the file of the start of the line, ignored.(ScanWordsMapper.java)
	 * The value is a line from the file.(ScanWordsMapper.java)
	 * </p>
	 */
	public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
		
		String line = value.toString();
		JsonObject tweetJo = this.getTweetJo(line);
			
		// check if the retweeted object is existed
		if (tweetJo.containsKey(RETWEETED_STATUS)) {
			JsonObject reTweetJo = tweetJo.getJsonObject(RETWEETED_STATUS);
			String retweet = this.getRetweet(reTweetJo);
			if (!retweet.equals(IS_NULL)) {
				output.write(new Text(retweet), new LongWritable(1));
			}
		}
	}
	
	/**
	 *  get the JsonObject from the line
	 *  warning: this JsonObject is the tweetObject not the userObject
	 * @param strline
	 * @return
	 */
	private JsonObject getTweetJo(String strline) {
		//get the JsonObject from the line
		JsonReader reader = Json.createReader(new StringReader(strline));
		JsonObject tweetObject = reader.readObject();
		reader.close();
		return tweetObject;
	}
	
	/**
	 * function take the JsonObject and get the tweet and location
	 * then function combine them together by StringBuilder
	 * @param jo Tweet object
	 * @return tweet and location related to its
	 */
	private String getRetweet(JsonObject jo) {
		
		// check whether the object contains the certain keys
		if (!jo.containsKey(USER_FIELD)) {
			return IS_NULL;
		}
		JsonObject userObject = jo.getJsonObject(USER_FIELD);
		if (!(jo.containsKey(TEXT_FIELD) & userObject.containsKey(LOCATION_FIELD))) {
			return IS_NULL;
		}
		
		
		// (From Hint 2) check whether the object contains the certain attribute
		if (jo.get(TEXT_FIELD).getValueType() == ValueType.NULL) {
			return IS_NULL;
		}
		if (userObject.get(LOCATION_FIELD).getValueType() == ValueType.NULL) {
			return IS_NULL;
		}
		
		// aggregate the screen_name and location by StringBuilder
		StringBuilder sb = new StringBuilder(jo.getString(TEXT_FIELD));
		sb.append(" (");
		sb.append(userObject.getString(LOCATION_FIELD));
		sb= this.lineReplace(sb);
		sb.append(")");
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
