package in.ac.iitkgp.acaddwh.service.etl.fact;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import in.ac.iitkgp.acaddwh.bean.fact.TeachingQuality;
import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.config.NameNodeInfo;
import in.ac.iitkgp.acaddwh.dao.fact.TeachingQualityDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractAndTransformException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class TeachingQualityETL implements ETLService<TeachingQuality> {

	public static class ETMapper extends Mapper<Text, Text, Text, Text> {
		private Text attributes = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String instituteCode = conf.get("instituteCode");

			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			TeachingQuality teachingQuality = new TeachingQuality();

			/* Extract: BEGIN */
			teachingQuality.setCourseKey(key.toString());
			teachingQuality.setTimeKey(itr.nextToken());
			teachingQuality.setTeacherKey(itr.nextToken());
			teachingQuality.setEvalAreaKey(itr.nextToken());
			teachingQuality.setNoOfEvaluation(Integer.parseInt(itr.nextToken()));
			teachingQuality.setAvgTeachingQuality(Float.parseFloat(itr.nextToken()));
			/* Extract: END */

			/* Transform: BEGIN */
			teachingQuality.setInstituteKey(instituteCode);
			teachingQuality.setCourseKey(instituteCode + '_' + teachingQuality.getCourseKey());
			teachingQuality.setTimeKey(instituteCode + '_' + teachingQuality.getTimeKey());
			teachingQuality.setTeacherKey(instituteCode + '_' + teachingQuality.getTeacherKey());
			teachingQuality.setEvalAreaKey(instituteCode + '_' + teachingQuality.getEvalAreaKey());
			/* Transform: END */

			attributes.set(teachingQuality.getPrintableLineWithoutKeyAndNewLine());
			context.write(new Text(teachingQuality.getInstituteKey()), attributes);
		}
	}

	public boolean extractAndTransform(String shortFileName, String instituteCode, String absoluteLogFileName)
			throws ExtractAndTransformException {
		try {
			Configuration conf = new Configuration();
			conf.set("key.value.separator.in.input.line", ",");
			conf.set("mapred.textoutputformat.separator", ",");
			conf.set("mapred.min.split.size", HadoopNodeInfo.getSplitSize()+"");
			conf.set("mapred.max.split.size", HadoopNodeInfo.getSplitSize()+"");
			conf.set("dfs.block.size", HadoopNodeInfo.getDfsBlockSize()+"");			
			conf.set("instituteCode", instituteCode);

			Job job = new Job(conf, "extractAndTransform_" + shortFileName);
			job.setMapperClass(ETMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(job,
					new Path(NameNodeInfo.getUrl() + HadoopNodeInfo.getPathInHdfs() + shortFileName));
			FileOutputFormat.setOutputPath(job, new Path(NameNodeInfo.getUrl() + HadoopNodeInfo.getPathInHdfs()
					+ "outputDir_" + shortFileName.replace(".", "_")));
			return job.waitForCompletion(true);

		} catch (Exception e) {
			e.printStackTrace();
			throw (new ExtractAndTransformException());
		}
	}

	public void load(String hdfsFilePath, String absoluteLogFileName) throws LoadException {
		StringBuffer logString = new StringBuffer();

		Connection con = HiveConnection.getSaveConnection();
		TeachingQualityDAO teachingQualityDAO = new TeachingQualityDAO();

		try {
			teachingQualityDAO.addToHive(con, hdfsFilePath);
			System.out.println("[H] Loaded TeachingQuality file: " + hdfsFilePath);

		} catch (SQLException e) {
			System.out.println("LoadException thrown!");
			logString.append("Load," + "-" + "," + "-" + "," + LogFile.getErrorMsg(e) + "\n");
			LogFile.writeToLogFile(absoluteLogFileName, logString);
			throw (new LoadException());
		} finally {
			HiveConnection.closeConnection(con);
		}
	}
}
