package com.rukspot.sample.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageSalaryMapper extends Mapper {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vals = line.split(",");

        if(vals.length == 3) {
            String department = vals[1];
            Double average = Double.parseDouble(vals[2]);

            Text departmentName = new Text();
            departmentName.set(department);


            CustomAverageTuple tuple = new CustomAverageTuple();
            tuple.setAverage(average);
            tuple.setCount(1);

            context.write(departmentName, tuple);
        }

    }
}
