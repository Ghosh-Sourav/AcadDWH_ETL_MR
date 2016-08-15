package in.ac.iitkgp.acaddwh;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;

public class TestMR {
	public static class ETMapper extends Mapper<Text, Text, Text, Text> {
		private Text attribute = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			
			StringBuffer sb = new StringBuffer(); 
			while (itr.hasMoreTokens()) {
				sb.append("_<"+itr.nextToken()+">_");
			}
			attribute.set(sb.toString());
			context.write(key, attribute);
		}
	}
/*
	public static class ETReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String translations = "";
			for (Text val : values) {
				translations += "|" + val.toString();
			}
			result.set(translations);
			context.write(key, result);
		}
	}
*/
	public static int executeJob() throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "testMR");
		job.setJarByClass(TestMR.class);
		job.setMapperClass(ETMapper.class);
		//job.setReducerClass(ETReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(HadoopNodeInfo.getPathInHdfs() + "20160815213022114_INSA_dim_departments.csv"));
		FileOutputFormat.setOutputPath(job, new Path(HadoopNodeInfo.getPathInHdfs() + "output_20160815213022114_INSA_dim_departments.csv"));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
