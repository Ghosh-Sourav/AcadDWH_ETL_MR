package in.ac.iitkgp.acaddwh.service.etl.dim;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import in.ac.iitkgp.acaddwh.bean.dim.Specialisation;
import in.ac.iitkgp.acaddwh.config.HadoopNodeInfo;
import in.ac.iitkgp.acaddwh.config.NameNodeInfo;
import in.ac.iitkgp.acaddwh.dao.dim.SpecialisationDAO;
import in.ac.iitkgp.acaddwh.exception.ExtractAndTransformException;
import in.ac.iitkgp.acaddwh.exception.LoadException;
import in.ac.iitkgp.acaddwh.service.ETLService;
import in.ac.iitkgp.acaddwh.service.etl.GenericETMapper;
import in.ac.iitkgp.acaddwh.util.HiveConnection;
import in.ac.iitkgp.acaddwh.util.LogFile;

public class SpecialisationETL implements ETLService<Specialisation> {

	public static class ETMapper extends GenericETMapper {
		private Text attributes = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String instituteCode = conf.get("instituteCode");

			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			Specialisation specialisation = new Specialisation();

			/* Extract: BEGIN */
			specialisation.setSplCode(key.toString());
			specialisation.setSplName(itr.nextToken());
			specialisation.setDeptKey(itr.nextToken());
			specialisation.setSplDegree(itr.nextToken());
			specialisation.setSplLevel(itr.nextToken());
			/* Extract: END */

			/* Transform: BEGIN */
			specialisation.setSplKey(instituteCode + '_' + specialisation.getSplCode());
			specialisation.setDeptKey(instituteCode + '_' + specialisation.getDeptKey());
			/* Transform: END */

			attributes.set(specialisation.getPrintableLineWithoutKeyAndNewLine());
			context.write(new Text(specialisation.getSplKey()), attributes);
		}
	}

	public boolean extractAndTransform(String shortFileName, String instituteCode, String absoluteLogFileName)
			throws ExtractAndTransformException {
		try {
			Configuration conf = new Configuration();
			conf.set("key.value.separator.in.input.line", ",");
			conf.set("mapred.textoutputformat.separator", ",");
			conf.set("mapred.min.split.size", HadoopNodeInfo.getSplitSize(shortFileName)+"");
			conf.set("mapred.max.split.size", HadoopNodeInfo.getSplitSize(shortFileName)+"");
			conf.set("dfs.block.size", HadoopNodeInfo.getDfsBlockSize()+"");			
			conf.set("instituteCode", instituteCode);

			Job job = new Job(conf, "extractAndTransform_" + shortFileName);
			job.setMapperClass(ETMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			if (!HadoopNodeInfo.isReducerToBeUsed()) {
				job.setNumReduceTasks(0);
			}

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
		SpecialisationDAO specialisationDAO = new SpecialisationDAO();

		try {
			specialisationDAO.addToHive(con, hdfsFilePath);
			System.out.println("[H] Loaded Specialisation file: " + hdfsFilePath);

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
