package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/26/18
 * @email thakker.m@husky.neu.edu
 */
public class Page implements Writable {

    public Page() {

    }

    public String pageName;
    public Long index;
    public boolean isOutLink;

    public Page(String pageName, Long index, boolean isOutLink) {
        this.pageName = pageName;
        this.index = index;
        this.isOutLink = isOutLink;
    }


    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(pageName);
        dataOutput.writeLong(index);
        dataOutput.writeBoolean(isOutLink);
    }


    public void readFields(DataInput dataInput) throws IOException {


        pageName = dataInput.readUTF();
        index = dataInput.readLong();
        isOutLink = dataInput.readBoolean();

    }


    @Override
    public String toString() {
        return pageName+" : "+index+" "+isOutLink;
    }
}
