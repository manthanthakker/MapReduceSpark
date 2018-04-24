package hw2code.domain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StationIdTime implements WritableComparable {

    private String stationId;
    private String year;


    public StationIdTime() {
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getTime() {
        return year;
    }

    public void setTime(String time) {
        this.year = time;
    }

    public StationIdTime(String stationId, String time) {
        this.stationId = stationId;
        this.year = time;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(stationId);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        stationId = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    @Override
    public int compareTo(Object o) {
        StationIdTime record = (StationIdTime) o;
        return this.stationId.compareTo(record.getStationId());
    }
}
