import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerMapSideJoinDCacheTextFile extends Reducer<Text, Text, Text, Text> {
private Text m_result = new Text();
	
	// Calculates intersection of two given Strings, i.e. friends lists
	private String intersection(String s1, String s2) {
	    HashSet<String> h1 = new HashSet<String>();
	    HashSet<String> h2 = new HashSet<String>();
	    
	    String[] n1 = s1.split(",");
	    String[] n2 = s2.split(",");
	    for(int i = 0; i < n1.length; i++) {
	        h1.add(n1[i]);
	    }
	    for(int i = 0; i < n2.length; i++) {
	        h2.add(n2[i]);
	    }
	
	    h1.retainAll(h2);
	    String[] res = h1.toArray(new String[0]);
	    String intersect = new String("");
	    for (int i = 0; i < res.length; i++) {
	        intersect += res[i]+",";
	    }
	    
	    return intersect.substring(0,intersect.length()-1);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
	    // Prepare a 2-String-Array to hold the values, i.e. the friends lists of
	    // our current friends pair.
	    String[] combined = new String[2];
	    int cur = 0;
	    for(Text value : values) {
	        combined[cur++] = value.toString();
	    }
	
	    // Calculate the intersection of these lists and write result in the form (UserAUserB, MutualFriends).
	    m_result.set(intersection(combined[0], combined[1]));
	    context.write(key, m_result);
	}
}
