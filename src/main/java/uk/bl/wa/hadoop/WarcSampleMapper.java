package uk.bl.wa.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import de.l3s.concatgz.io.warc.WarcWritable;

public class WarcSampleMapper
        extends Mapper<NullWritable, WarcWritable, NullWritable, NullWritable> {

    @Override
    protected void map(NullWritable key, WarcWritable value,
            Mapper<NullWritable, WarcWritable, NullWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.map(key, value, context);

        System.out.println("THING " + value.getFilename());
    }

}
