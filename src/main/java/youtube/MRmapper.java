package youtube;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.text.*;
import java.util.Calendar;
import java.util.Date;


public class MRmapper  extends Mapper <LongWritable,Text,Text,Text> {
    static String IFS=",";
    static String OFS=",";
    static int NF=11;
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException {
        
        /** 
		0 Date;
		1 Time;
		2 Global_active_power;
		3 Global_reactive_power;
		4 Voltage;
	 	5 Global_intensity;
		6 Sub_metering_1;
		7 Sub_metering_2;
		8 Sub_metering_3
        */
    	// TODO 2: convert value to string
    	String[] tokens = (value.toString()).split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        // TODO 3: count num fields, increment bad record counter, and return if bad

    	if (tokens[0].equals("Date")) { return;} 
    	else {
    		String format = "ddMMyyyy";
    		SimpleDateFormat df = new SimpleDateFormat(format);
    		int week = 0;
    		try {
				Date date = df.parse(tokens[0]);
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				week = cal.get(Calendar.WEEK_OF_YEAR);
			} catch (ParseException e) {
				e.printStackTrace();
			}
    		context.write(new Text(Integer.toString(week)), new Text(tokens[1] + "," +
        			tokens[2]+","+tokens[3]+","+tokens[4]+","+tokens[5]+","+tokens[6]+","+tokens[7]+","+tokens[8]));
    	}}    	
		 }

