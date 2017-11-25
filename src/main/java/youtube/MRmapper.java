package youtube;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

public class MRmapper  extends Mapper <LongWritable,Text,Text,Text> {
    static String IFS=",";
    static String OFS=",";
    static int NF=11;
    static int bad_record = 0;
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException {
        
        /** USvideos.csv
        video_id0
        title1
        channel_title2
        category_id3
        tags4
        views5
        likes6
        dislikes7
        comment_total8  ||   videoid , views likes dislikes link
        thumbnail_link9
        date10
        */
    	// TODO 2: convert value to string
    	String[] tokens = (value.toString()).split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        // TODO 3: count num fields, increment bad record counter, and return if bad
    	if (tokens.length != 11) {
    		bad_record++;
    		return; 
    	}
        // TODO 3: count num fields, increment bad record counter, and return if bad

        // TODO 4: pull out fields of interest
        
        // TODO 5: construct key and composite value 
        
        // TODO 6: write key value pair to context
    	else {
    		if (tokens[0].equals("video_id")) { return;} else {
    		context.write(new Text(tokens[3]), new Text(tokens[0] + "," +
        			tokens[5]+","+tokens[6]+","+tokens[7]+","+tokens[9]));
    	}}    	
		 }}

