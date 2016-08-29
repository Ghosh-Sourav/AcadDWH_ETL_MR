package in.ac.iitkgp.acaddwh.service.etl;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class GenericETMapper extends Mapper<Text, Text, Text, Text> {
	private long taskStartTime = 0;
	private long taskEndTime = 0;
	private long taskTimeTaken = 0;

	public long getTaskTimeTaken() {
		return taskTimeTaken;
	}


	public void setup(Context context) throws IOException, InterruptedException {
		taskStartTime = new Date().getTime();
		System.out.println("TASK START TIME = " + taskStartTime);
		super.setup(context);
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		taskEndTime = new Date().getTime();
		System.out.println("TASK END TIME = " + taskEndTime);
		taskTimeTaken = (taskEndTime - taskStartTime);
		System.out.println("TOTAL TIME = " + (taskEndTime - taskStartTime) + " ns");
		
		String jobName = conf.get("jobName");
		ETLHelper.addMaxTimeTakenInfo(jobName, taskTimeTaken);
		
		super.cleanup(context);
	}
}
