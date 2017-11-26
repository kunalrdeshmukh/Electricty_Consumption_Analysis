package youtube;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MRreducer  extends Reducer <Text,Text,Text,Text> {
    public static String IFS=",";
    public static String OFS=",";
   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
    // TODO 1: initialize variables
	   /** Key : Week_of_the_year;
	    * Values:
		* 1 Time;
		* 2 Global_active_power;
		* 3 Global_reactive_power;
		* 4 Voltage;
	 	* 5 Global_intensity;
		* 6 Sub_metering_1;
		* 7 Sub_metering_2;
		* 8 Sub_metering_3   */
	// TODO 2: loop through values to find most viewed, most liked, and most disliked video
	   for (Text value: values) {
		  
		   }
	// TODO 3: write the key-value pair to the context exactly as defined in lab write-up
	
	 }
}
