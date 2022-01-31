package com.rukspot.sample.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageSalaryReducer extends Reducer {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        long count = 0;

        for (Object o : values) {
            CustomAverageTuple tuple = (CustomAverageTuple) o;
            sum = sum + (tuple.getAverage() * tuple.getCount());
            count = count + tuple.getCount();
        }

        CustomAverageTuple result = new CustomAverageTuple();
        result.setCount(count);
        result.setAverage(sum / count);
        context.write(new Text(key.toString()), result);
    }
}
