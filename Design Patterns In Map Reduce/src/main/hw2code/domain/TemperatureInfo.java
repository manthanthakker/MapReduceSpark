package hw2code.domain;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TemperatureInfo implements Writable {

  // VARIABLES
  private Long temperature;
  private Integer type;


  //// CONSTRUCTOR AND GETTER AND SETTERS
  public TemperatureInfo() {}

  public Long getTemperature() {
    return temperature;
  }

  public Integer getType() {
    return type;
  }

  public TemperatureInfo(final Integer type, final Long temperature) {
    super();
    this.temperature = temperature;
    this.type = type;
  }


  // SERILIZATION AND DESERILIZATION METHODS
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(type);
    dataOutput.writeLong(temperature);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    type = dataInput.readInt();
    temperature = dataInput.readLong();
  }
}
