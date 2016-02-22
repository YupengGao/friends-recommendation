import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.io.*;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
/*
 * method one: we make pair of <ID , avgAge> by in memory join and reverse to <avgAge, ID>, and hadoop will sort the record by key
 * then get the top 20 record
 * 
 * method two: extract the column based on which we would like to find Top K records and insert that value as key into TreeMap 
 * and entire row as value.
 */




public class Question4 {
/*
 * step 1 make the in memory join. put userdata in memory  
 */
   static public class Map extends Mapper<LongWritable, Text, Text, Text>{
	     
//	   TreeMap<String, String> ToRecordMap = new TreeMap<String , String>(new Comparator<String>(){
//	    	 public int compare(String st1, String st2)
//	         {
//               return  st2.compareTo(st1);
//	    		
//	         } 
//	     });
	    	double diff = 0.0000000001;
	    	HashMap<String,String> map = new HashMap<String, String>();
			
	    	public void setup(Context context) throws IOException, InterruptedException {
				super.setup(context);
				Configuration conf = context.getConfiguration();
				String myfilepath = conf.get("input1");
				
				
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
			        	String st[] = line.split(",");//the '['  ']' the sequence is not fixed
			        	String time[] = st[9].split("/");
			        	//LocalDate start = LocalDate.of(Integer.parseInt(time[2]), Integer.parseInt(time[0]), Integer.parseInt(time[1]));
				   		//LocalDate end = LocalDate.of(2016, 2, 17); // use for age-calculation: LocalDate.now()
				   		//int years = (int)ChronoUnit.YEARS.between(start, end);
			        	int years = 2016 - Integer.parseInt(time[2]);
			        	if(Integer.parseInt(time[0]) < 2){
			        		years--;
			        	}else if((Integer.parseInt(time[0]) == 2) && (Integer.parseInt(time[1]) < 20)){
			        		years--;
			        	}
			        	map.put(st[0], Integer.toString(years));
			            line=br.readLine();
			        }
			       
			    }
		     
			}
			
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				/////////why map read the userdata not the soc    
				String line[] = value.toString().split("\t");
			        int sum = 0;
                    int count = 0;
			        if (line.length == 2) {
			          String directFriend[] = line[1].split(",");
					          for(String friend:directFriend){
					        	  if(map.containsKey(friend)){
					        		  sum  += Integer.parseInt(map.get(friend));
					        		  count++;
					        	  }
					          }
					  double average = 0;
					  if(count != 0){
						  average = sum * (1.000)/count;
					  }
					   DecimalFormat df=new DecimalFormat(".##########");
						 
					   String avgStr=df.format(average + diff);
					   diff = diff + 0.0000000001;
			          context.write( new Text(line[0]),new Text(avgStr));
			          //ToRecordMap.put(avgStr, line[0]);
			          //Iterator<Map.Entry<String , String>> iter = ToRecordMap.entrySet().iterator();
			          //TreeMap.Entry<String , String> entry = null;
			          //Set set = ToRecordMap.entrySet();
			          //Iterator iter = set.iterator();
//			          while(ToRecordMap.size()>10){
//			        	  //Entry<String,Integer>  entry = iter.next(); 
//			        	  String lastkey = ToRecordMap.lastKey();
//			        	  ToRecordMap.remove(lastkey);         
//			            }
						

			        }
			        //context.write(new LongWritable(Long.parseLong(line[0])), new Text("asd"));
			}
			
//			protected void cleanup(Context context) throws IOException, InterruptedException {
//				
//				for (String key : ToRecordMap.keySet()) 
//				{
////				   //context.write(new LongWritable(Long.parseLong(ToRecordMap.get(key))), new Text(key));
//					context.write(new Text(key), new Text(ToRecordMap.get(key)));
////					context.write(new LongWritable(Long.parseLong("11")), new Text(Integer.toString(ToRecordMap.size())));
//				}
////				for(int i = 0 ; i < 9 ; i++){
//				 //context.write(new LongWritable(Long.parseLong("166")), new Text("22"));
////				}
//            }    
			
   }
   
   public static class Reduce extends Reducer<Text, Text, Text, Text>{
   	
	   TreeMap<MyKey, String> treeMap = new TreeMap<MyKey , String>(new Comparator<MyKey>(){
	    	 public int compare(MyKey st1, MyKey st2)
	         {
               return  st2.compareTo(st1);
	    		
	         } 
	     });
	   public void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
   		//<id ,avg>
   		String txKey = key.toString();
   		String stValue = "";
       	for (Text val : values) {
       		stValue = stValue + val.toString();
       	}
       	treeMap.put(new MyKey(Double.parseDouble(stValue),Integer.parseInt(txKey)), txKey);
        while(treeMap.size()>20){
      	  //Entry<String,Integer>  entry = iter.next(); 
      	   MyKey lastkey = treeMap.lastKey();
      	   treeMap.remove(lastkey);         
          }

   	}
   	
   	protected void cleanup(Context context) throws IOException, InterruptedException {
		//<avg, id>
		for (MyKey key : treeMap.keySet()) 
		{
			//context.write(new Text(treeMap.get(key)), new Text(key));
			context.write(new Text(treeMap.get(key)), new Text(Double.toString(key.getValue())));
		}

    }    
   }
   
   public static class MyKey implements Comparable<MyKey>{
       private Double value;
       private int id;
   
       public MyKey(double value,int num){
           this.value = value;
           this.id = num;
       }
       
      public double getValue() {
          return value;
      }
  
      public void setValue(double value) {
          this.value = value;
      }
  
      @Override
      public int compareTo(MyKey o) {
          return value.compareTo(o.getValue());
      }
      
      
  }
   /*
    * intermediatePath
    */
   static public class Map1 extends Mapper<LongWritable, Text, Text, Text>{
	     
	    	

			
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
               String lines[] = value.toString().split("\t");
               context.write(new Text(lines[0]), new Text(lines[1]));
			}

			
   }
   /*
    * userdata
    */
   public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from userdata
			
			String lines[] = value.toString().split(",");
			context.write(new Text(lines[0]), new Text(lines[3] + ", " + lines[4] + ", " +lines[5] + ", "));
       }
	
		
	}
   
   public static class Reduce2 extends Reducer<Text,Text,Text,Text> {
	   TreeMap<String, String> tree = new TreeMap<String , String>(new Comparator<String>(){
	    	 public int compare(String st1, String st2)
	         {
             return  st2.compareTo(st1);
	    		
	         } 
	     });
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {

           String st = "";
           int count = 0;
			for (Text val : values) {
				
				count++;
				if(count == 1){
					st = st + val.toString();
				}
				else if(count == 2){ //tree<age---id+address>
					st = st + val.toString().substring(0, 2);
					tree.put(val.toString(), key.toString() +", "+ st );
				}
           
			}
//			if(count > 1){
//				context.write(new Text(key.toString()),new Text(st));
//				
//			}
			
    }		    
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//<avg, id>
			for (String key : tree.keySet()) 
			{
				//context.write(new Text(treeMap.get(key)), new Text(key));
				context.write(null, new Text(tree.get(key)));
			}

	    }  
	}


   public static void main(String args[]) throws Exception{
  	 //final String OUTPUT_PATH = "intermediate_output";
  	
	  Configuration conf = new Configuration();//we need only one configure for two mapreduce
	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args

	  //conf.set("firstFriend", args[0]);//parse the parameter to firstfriend
      //conf.set("secondFriend", args[1]);
      conf.set("input1", "/yxg140730/input1/userdata.txt");//the parameter should be set at the begining
      //conf.set("intermediatePath", "/yxg140730/intermediate");
      Job job = new Job(conf, "Question4");
      job.setJarByClass(Question4.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      
     
      /*
       * job1
       */
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileSystem outFs = new Path("/yxg140730/input/soc-LiveJournal1Adj.txt").getFileSystem(conf);
      //outFs.delete(new Path("/yxg140730/input/result.txt"), true);

      FileInputFormat.addInputPath(job, new Path("/yxg140730/input/soc-LiveJournal1Adj.txt"));
      FileOutputFormat.setOutputPath(job, new Path("/yxg140730/intermediate"));

      job.waitForCompletion(true);
      
      /*
       * job2
       */
      //Configuration conf2 = new Configuration();
      Job job2 = new Job(conf, "Job2");
      job2.setJarByClass(Question4.class);
      /*
       * reduce side join
       */
      MultipleInputs.addInputPath(job2, new Path("/yxg140730/intermediate/part-r-00000"), TextInputFormat.class,Map1.class );

	  MultipleInputs.addInputPath(job2, new Path("/yxg140730/input1/userdata.txt"),TextInputFormat.class,Map2.class );

      //conf2.set("firstFriend", args[0]);//parse the parameter to firstfriend
      //conf2.set("secondFriend", args[1]);
      //conf.set("intermediatePath", "/yxg140730/intermediate_output");
      
      //job2.setMapperClass(Map2.class);
      job2.setReducerClass(Reduce2.class);
      
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      
      //job2.setInputFormatClass(TextInputFormat.class);
      //job2.setOutputFormatClass(TextOutputFormat.class);
      //MultipleInputs.addInputPath(job, orderInput, TextInputFormat.class, OrderMapper.class);

      //TextInputFormat.addInputPath(job2, new Path("/yxg140730/input/userdata.txt"));
      TextOutputFormat.setOutputPath(job2, new Path("/yxg140730_out"));
      
      job2.waitForCompletion(true);
   }
}
