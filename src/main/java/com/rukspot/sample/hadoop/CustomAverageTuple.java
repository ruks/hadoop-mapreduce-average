package com.rukspot.sample.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomAverageTuple implements Writable {
    private Double average = (double) 0;
    private long count = 1;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(average);
        dataOutput.writeLong(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        average = dataInput.readDouble();
        count = dataInput.readLong();
    }

    public Double getAverage() {
        return average;
    }

    public void setAverage(Double average) {
        this.average = average;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String toString() {
        return average + "\t" + count;
    }
}
