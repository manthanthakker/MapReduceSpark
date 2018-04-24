package hw2code.domain;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureStats implements Writable{

    /**
     * Variables
     */
    private  double sumMax;
    private  double sumMin;
    private  double countMax;
    private  double countMin;


    /**
     * Constructor calls
     * @param sumMax
     * @param sumMin
     * @param countMax
     * @param countMin
     */
    public TemperatureStats(double sumMax, double sumMin, double countMax, double countMin) {

        this.sumMax = sumMax;
        this.countMax = countMax;
        this.sumMin = sumMin;
        this.countMin = countMin;
    }

    public TemperatureStats(){}


    /**
     * Getter and setters
     * @return
     */
    public double getSumMax() {
        return sumMax;
    }

    public void setSumMax(double sumMax) {
        this.sumMax = sumMax;
    }

    public double getSumMin() {
        return sumMin;
    }

    public void setSumMin(double sumMin) {
        this.sumMin = sumMin;
    }

    public double getCountMax() {
        return countMax;
    }

    public void setCountMax(double countMax) {
        this.countMax = countMax;
    }

    public double getCountMin() {
        return countMin;
    }

    public void setCountMin(double countMin) {
        this.countMin = countMin;
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

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeDouble(sumMax);
        dataOutput.writeDouble(sumMin);
        dataOutput.writeDouble(countMax);
        dataOutput.writeDouble(countMin);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        sumMax=dataInput.readDouble();
        sumMin=dataInput.readDouble();
        countMax=dataInput.readDouble();
        countMin=dataInput.readDouble();
    }
}
