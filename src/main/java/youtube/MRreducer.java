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
	   long views = 0;
	   long likes = 0;
	   long dislikes = 0;
	   String likes_video_id = null;
	   String likes_thumbnail_link = null;
	   String dislikes_video_id = null;
	   String dislikes_thumbnail_link = null;
	   String views_video_id = null;
	   String views_thumbnail_link = null;
	   String category_id = key.toString();
	   long max_likes = 0;  
	   long max_dislikes = 0;
	   long max_views = 0;
	   String compositeString;
	   String[] compositeStringArray;
	// TODO 2: loop through values to find most viewed, most liked, and most disliked video
	   for (Text value: values) {
		   compositeString = value.toString();
		   compositeStringArray = compositeString.split(",");
		   views = new Long(compositeStringArray[1]).longValue();
		   likes = new Long(compositeStringArray[2]).longValue();
		   dislikes = new Long(compositeStringArray[3]).longValue();
		   
		   if(views > max_views) {
			   max_views = views;
			   views_video_id = new Text(compositeStringArray[0]).toString();
			   views_thumbnail_link = new Text(compositeStringArray[4]).toString();
		   }
		   if(likes > max_likes) {
			   max_likes = likes;
			   likes_video_id = new Text(compositeStringArray[0]).toString();
			   likes_thumbnail_link = new Text(compositeStringArray[4]).toString();
		   }
		   if(dislikes > max_dislikes) {
			   max_dislikes = dislikes;
			   dislikes_video_id = new Text(compositeStringArray[0]).toString();
			   dislikes_thumbnail_link = new Text(compositeStringArray[4]).toString();
		   }
		   }
	// TODO 3: write the key-value pair to the context exactly as defined in lab write-up
	   String str = "category_id: "+category_id;
	   Text KEY = new Text(str);
	   str = "\nmost views: "+views_video_id+", "+views_thumbnail_link+'\n'+"most likes: "+likes_video_id
			   +", "+likes_thumbnail_link+'\n'+"most dislikes: "+dislikes_video_id+", "+dislikes_thumbnail_link;
	   Text VALUE = new Text(str);
	   context.write(KEY,VALUE);		
	 }
}
