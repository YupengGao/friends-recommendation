import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 





public class MapperMapSideJoinDCacheTextFile extends Mapper<LongWritable, Text, Text, Text>{
	HashMap<String, String> DepartmentMap = new HashMap<String, String>();

    private Text m_id = new Text();
    private Text m_others = new Text();
    
	Text keyUser = new Text();
	Text suggTuple = new Text();
	Text existingFriend = new Text();
	String [] userRow,friendList;
	String otherFriends;
	
	int i,j;

	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		//read data to memory on the mapper.
		Configuration conf = context.getConfiguration();
		String myfilepath = conf.get("myfilepath");
		//e.g /user/hue/input/
		Path part=new Path("hdfs://cshadoop1"+myfilepath);//Location of file in HDFS
		
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(part);
	    for (FileStatus status : fss) {
	        Path pt = status.getPath();
	        
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String line;
	        line=br.readLine();
	        while (line != null){
	            System.out.println(line);
	            //do what you want with the line read
	            String strLineReadChanged = line.replaceFirst(",", "@");
				String[] deptFieldArray = strLineReadChanged.split("@");
				String[] detail = deptFieldArray[1].toString().trim().split(",");
				String nz = detail[0].toString()+":"+detail[5].toString();
				DepartmentMap.put(deptFieldArray[0].trim().toString(), nz.trim());
				line=br.readLine();
	            
	        }
	       
	    }
	}

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
		
		Configuration conf = context.getConfiguration();
    	String user1 = conf.get("user1");
    	String user2 = conf.get("user2");
    	String user12 = user1.compareTo(user2) < 0 ? user1+","+user2 : user2+","+user1;
    	userRow = value.toString().split("\\s");
		if (userRow.length==1){
			userRow = null;
			return;
		}
		//friendList = null;
		friendList = userRow[1].split(",");
		
		
		
		for (String friend : friendList) 
		{
			String id = userRow[0].compareTo(friend) < 0 ? userRow[0]+","+friend : friend+","+userRow[0];
            m_id.set(id);
            if(m_id.toString().equals(user12))
            { 
            	String nameZip = "";
            	for (int i =0;i<friendList.length;i++) {
            		String tempString="";
            		
        			
        			try {
        				tempString = DepartmentMap.get(friendList[i].toString());
        				
        			} finally {
        				tempString = ((tempString.equals(null) || tempString
        						.equals("")) ? "NOT-FOUND" : tempString);
        			}
        			nameZip += tempString+",";
        		}
        		m_others.set(nameZip.substring(0,nameZip.length()-1));
            	context.write(m_id, m_others);
            }
		}
		
	}



}