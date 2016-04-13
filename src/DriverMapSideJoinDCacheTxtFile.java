import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


@SuppressWarnings("deprecation")
public class DriverMapSideJoinDCacheTxtFile extends Configured implements Tool {

	  @SuppressWarnings("deprecation")
	@Override
		public int run(String[] args) throws Exception {

			Job job = new Job(getConf());
			Configuration conf = job.getConfiguration();
	        conf.set("user1", args[0]);
	        conf.set("user2", args[1]);
	        conf.set("myfilepath", "/yxg140730/input/userdata.txt");
			
			job.setJobName("Map-side join with text lookup file in DCache");
			//DistributedCache.addCacheFile(new Path("/yxh144130/cache/userdata.txt").toUri(),conf);
			//DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+ otherArgs[1]), conf);     

			job.setJarByClass(DriverMapSideJoinDCacheTxtFile.class);
			job.setMapperClass(MapperMapSideJoinDCacheTextFile.class);
			job.setReducerClass(ReducerMapSideJoinDCacheTextFile.class);
			job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, new Path("/yxg140730/input/soc-LiveJournal1Adj.txt"));
			FileOutputFormat.setOutputPath(job, new Path("/yxg140730/output"));

			//job.setNumReduceTasks(0);
			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		}

		public static void main(String[] args) throws Exception {
			int exitCode = ToolRunner.run(new Configuration(), new DriverMapSideJoinDCacheTxtFile(), args);
			System.exit(exitCode);
		}
	}
