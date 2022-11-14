import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
/**
 * <p>
 * provide a menu for satisfying different queries(Extension1.java)
 * the job configuration is put into the different Query class (Query1 Query2) (Extension1.java)
 * </p>
 * @author xh20
 *
 */
public class Extension2 {
	//meaningless default value for set number
	public static final int DEFAULT_NUM = - 233;
	
	//temporary file use by query 2, this dictionary will be deleted after finishing
	public static final String TEMP_PATH = "temp";
	
	//the name of the temporary file name is inspired from the practical work
	public static final String TEMP_FILE = "temp/part-r-00000";
	
	
	
	/**
	 * <p>
	 * user friendly console interface for user to choose the different queries
	 * </p>
	 * @param args args Only two argument 1: input path 2: output dictionary
	 * @throws IOException
	 */
	public static void main(String[]  args) throws IOException {
		
		//Check the input form
		if(args.length < 2) {
			System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_directory>");
			System.exit(1);
		}
		
		String input_path = args[0];
		String output_path = args[1];
		
		
		boolean isPass = false;
		
		int choice = DEFAULT_NUM ;
		
		
			while (!isPass) {
				System.out.println("Options:");
				System.out.println("1, Alphabetical Order");
				System.out.println("2, Number of Tweets by users");
				System.out.println("3: Sort out the hottest place on Tweeter");
				System.out.println("4, Find the Hottest Tweet");
				
				
				
				// try catch block to check the input
				// continue the loop if the input is invalid (e.g string space)
				try {
					
					// provide the console input solution on ide
					BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
					String input = reader.readLine();
					
					
					
					choice = Integer.parseInt(input);
					if (choice > 0 & choice <= 5) {
						isPass = true;
					} else {
						System.out.println("Invalid Input (enter between 1 and 3)");
					}
				
				} catch (NumberFormatException e) {
					isPass = false;
					System.out.println("Invalid Input (enter INTEGER between 1 and 2)");
				}
			}
		
		
		switch (choice) {
		case 1:
			Query1 query1 = new Query1();
			query1.operate(input_path, output_path);
			break;
		case 2:
			Query1 query2_1 = new Query1();
			query2_1.operate(input_path, TEMP_PATH);
			Query2 query2_2 = new Query2();
			query2_2.operate(TEMP_FILE, output_path);
			break;
		case 3:
			Query3 query3_1 = new Query3();
			query3_1.operate(input_path, TEMP_PATH);
			Query4 query3_2 = new Query4();
			query3_2.operate(TEMP_FILE, output_path);
			break;
		case 4:
			Query5 query4 = new Query5();
			query4.operate(input_path, TEMP_PATH);
			Query6 query4_1 = new Query6();
			query4_1.operate(TEMP_FILE, output_path);
			break;
		}
	}

}
