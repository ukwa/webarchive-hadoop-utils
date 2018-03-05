package uk.bl.wa.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.l3s.concatgz.io.ImmediateOutput;
import de.l3s.concatgz.io.warc.WarcGzInputFormat;

public class WarcSampler extends Configured implements Tool {
    public final static String TOOL_NAME = "WarcPartitioner";

    @Override
    public int run(String[] args) throws Exception {
        Job job =  new Job();
        job.setJobName(TOOL_NAME);
        job.setJarByClass(this.getClass());


        System.out.println("PATH IN " + args[0]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        for (Path item : FileInputFormat.getInputPaths(job)) {
            System.out.println("PATH " + item);
        }
        job.setInputFormatClass(WarcGzInputFormat.class);

        // job.setMapperClass(WarcSampleMapper.class);

        ImmediateOutput.initialize(job);

        Path outPath = new Path(args[1]);
        ImmediateOutput.setPath(job, outPath);
        ImmediateOutput.setIdPrefix(job, outPath.getName());
        ImmediateOutput.setExtension(job, ".gz");
        // ImmediateOutput.setReplication(job, REPLICATION);

        job.setNumReduceTasks(0);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static Configuration config() {
        Configuration config = new Configuration();
        config.setBoolean("mapreduce.map.speculative.execution", false);
        config.setBoolean("mapreduce.reduce.speculative.execution", false);
        config.setBoolean("mapreduce.output.fileoutputformat.compress", false);

        return config;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(config(), new WarcSampler(), args);
        System.exit(res);
    }
}