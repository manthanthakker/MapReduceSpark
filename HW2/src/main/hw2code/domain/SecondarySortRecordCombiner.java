package hw2code.domain;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondarySortRecordCombiner implements Writable {

    /**
     * Variables
     */
    private String year;
    private Long sumMax;
    private Long sumMin;
    private Long countMax;
    private Long countMin;

    public SecondarySortRecordCombiner() {

    }

    @Override
    public String toString() {
        return "TemperatureStats{" +
                "sumMax=" + sumMax +
                ", sumMin=" + sumMin +
                ", countMax=" + countMax +
                ", countMin=" + countMin +
                '}';
    }

    public SecondarySortRecordCombiner(String year, Long sumMax, Long sumMin, Long countMax, Long countMin) {

        this.year = year;
        this.sumMax = sumMax;
        this.countMax = countMax;
        this.sumMin = sumMin;
        this.countMin = countMin;
    }


    public Long getSumMax() {
        return sumMax;
    }

    public void setSumMax(Long sumMax) {
        this.sumMax = sumMax;
    }

    public Long getSumMin() {
        return sumMin;
    }

    public void setSumMin(Long sumMin) {
        this.sumMin = sumMin;
    }

    public Long getCountMax() {
        return countMax;
    }

    public void setCountMax(Long countMax) {
        this.countMax = countMax;
    }

    public Long getCountMin() {
        return countMin;
    }

    public void setCountMin(Long countMin) {
        this.countMin = countMin;
    }


    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeLong(sumMax);
        dataOutput.writeLong(sumMin);
        dataOutput.writeLong(countMax);
        dataOutput.writeLong(countMin);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        year = dataInput.readUTF();
        sumMax = dataInput.readLong();
        sumMin = dataInput.readLong();
        countMax = dataInput.readLong();
        countMin = dataInput.readLong();
    }
}
