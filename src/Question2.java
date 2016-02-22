
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class Question2 {

	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
	    private Text word = new Text();
	   
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Configuration conf = context.getConfiguration();//get the parameter
	    	String firstFriend = conf.get("firstFriend");
	        String secondFriend = conf.get("secondFriend");
	        String [] line = value.toString().split("\t");
	        
	        if(line[0].equals(firstFriend)){
	           String lists[] = line[1].split("\\),");
	        	for(String list:lists){
	        		String FriendAndList [] = list.split("\\(");
	        		if(FriendAndList[0].substring(0, FriendAndList[0].length() - 1).equals(secondFriend)){
	        			String parts[] = FriendAndList[1].split(":");
	        			for(String part:parts){
	        				String result = getList(part);
	        				if(result != null){
	        					context.write(new LongWritable(Long.parseLong(firstFriend)), new Text("," + secondFriend + result));
	        				}
	        			}
	        			
	        		}
	        	}
	        }
	    
	       // context.write(new LongWritable(Long.parseLong(firstNumber)), new Text("1234"));
	        }
	    
	    public String getList(String st){
	    	String result = null;
	    	if(st.length() >= 2 && st.charAt(1) == '['){
	    		result = st.substring(1,st.length());
	    	}
	    	return result;
	    }
	    }
	
	
	
	
	public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {

		
	    private Text text = new Text();
	    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	StringBuffer sb = new StringBuffer();
	    	
	    		String tx ="";
	        	for (Text val : values) {
	        		tx = tx + val.toString();
	        	}
	        	context.write(key, new Text(tx));
	    
	              
	              }  
	}
	
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		conf.set("firstFriend", args[0]);//parse the parameter to firstfriend
        conf.set("secondFriend", args[1]);
        Job job = new Job(conf, "Question1");
        job.setJarByClass(Question2.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
       
        //Job job1 = new Job(conf);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem outFs = new Path(args[1]).getFileSystem(conf);
        outFs.delete(new Path(args[1]), true);

        FileInputFormat.addInputPath(job, new Path("/yxg140730/input/resultFromQusOne"));
        FileOutputFormat.setOutputPath(job, new Path("/yxg140730_out"));

        job.waitForCompletion(true);
	}
}
