
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.*;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class Question3 {
	public static class Map1 extends Mapper<LongWritable, Text, LongWritable, Text> {
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
	
    public static class Reduce1 extends Reducer<LongWritable, Text, LongWritable, Text> {

		
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
    
    public static class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {
    	//public static final Log log = LogFactory.getLog(Map2.class);
    	List<String> idList = new ArrayList<String>();
		//Distributed cache does not work on the cluster, pls use thiscode for the setup phase instead.		
//		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			//myCenterList = new ArrayList<>();
			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("intermediatePath");
			
			
			Path part=new Path("hdfs://cshadoop1"+myfilepath);//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		            
		            //do what you want with the line read
		        	String st[] = line.split("\\[|\\]");//the '['  ']' the sequence is not fixed
		        	String ids[] = st[1].split(",");
		        	for(String id:ids){
		        		if(id.substring(0,1).equals(" ")){
		        			id = id.substring(1,id.length());
		        		}
		        		idList.add(id);
		        	}
		            line=br.readLine();
		        }
		       
		    }
	     
	    }
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			String line[] = value.toString().split(",");
			String userId = line[0];
//            if(line.length == 10){
				if(idList.contains(userId)){
					context.write(new LongWritable(111), new Text(line[1] + ":" + line[6]));
				}

		}

    }
    public static class Reduce2 extends Reducer<LongWritable, Text, LongWritable, Text>{
    	public void reduce(LongWritable key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
    		//StringBuffer sb = new StringBuffer();
    		Configuration conf = context.getConfiguration();//get the parameter
	    	String firstFriend = conf.get("firstFriend");
	        String secondFriend = conf.get("secondFriend");
    		String tx = secondFriend + " [";
        	for (Text val : values) {
        		tx = tx + val.toString() + ",";
        	}
        	tx = tx.substring(0,tx.length() - 1) + "]";
        	context.write(new LongWritable(Long.parseLong(firstFriend)), new Text( tx));
    	}
    }
    
    
    public static void main(String args[]) throws Exception{
    	 final String OUTPUT_PATH = "intermediate_output";
    	
		Configuration conf = new Configuration();//we need only one configure for two mapreduce
		conf.set("firstFriend", args[0]);//parse the parameter to firstfriend
        conf.set("secondFriend", args[1]);
        conf.set("intermediatePath", "/yxg140730/intermediate_output");//the parameter should be set at the begining
        Job job = new Job(conf, "Job1");
        job.setJarByClass(Question3.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
       
        /*
         * job1
         */
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem outFs = new Path("/yxg140730/input/result.txt").getFileSystem(conf);
        //outFs.delete(new Path("/yxg140730/input/result.txt"), true);

        FileInputFormat.addInputPath(job, new Path("/yxg140730/input/resultFromQusOne"));
        FileOutputFormat.setOutputPath(job, new Path("/yxg140730/intermediate_output"));

        job.waitForCompletion(true);
        
        /*
         * job2
         */
        //Configuration conf2 = new Configuration();
        Job job2 = new Job(conf, "Job2");
        job2.setJarByClass(Question3.class);
        
        //conf2.set("firstFriend", args[0]);//parse the parameter to firstfriend
        //conf2.set("secondFriend", args[1]);
        //conf.set("intermediatePath", "/yxg140730/intermediate_output");
        
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        TextInputFormat.addInputPath(job2, new Path("/yxg140730/input1/userdata.txt"));
        TextOutputFormat.setOutputPath(job2, new Path("/yxg140730_out"));
        
        job2.waitForCompletion(true);
	}
}
