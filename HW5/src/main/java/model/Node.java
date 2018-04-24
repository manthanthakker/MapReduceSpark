package model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/20/18
 * @email thakker.m@husky.neu.edu
 */
public class Node implements WritableComparable {

    public String id="DEFAULT";
    public Double pageRank = -1.0;
    public List<String> neighbors;
    public boolean isNode=false;
    public static final long SCALEUP = 1000000000l;

    public Node() {
        this.id=id;
        neighbors = new ArrayList<String>();
        isNode = true;
    }

    public Node(String id) {
        this.id = id.trim();
        this.pageRank = pageRank;
        neighbors = new ArrayList<String>();
        isNode = true;
    }

    public Node(String id, Double pageRank) {
        this.id = id.trim();
        this.pageRank = pageRank;
        neighbors = new ArrayList<String>();
        isNode = false;
    }

    // SERILIZATION AND DESERILIZATION METHODS

    public void write(DataOutput dataOutput) throws IOException {


        dataOutput.writeUTF(id.trim());
        dataOutput.writeBoolean(isNode);
        dataOutput.writeDouble(pageRank);
        String accumulate = "";
        for (String neighbor : neighbors)
            accumulate += neighbor.trim() + ",";
        if (accumulate.length() > 0)
            dataOutput.writeUTF(accumulate.substring(0, accumulate.length()));
        else {
            dataOutput.writeUTF(accumulate);
        }

    }



    public void readFields(DataInput dataInput) throws IOException {

        id = dataInput.readUTF().trim();
        isNode = dataInput.readBoolean();
        pageRank = dataInput.readDouble();
        neighbors = new ArrayList<String>();
        String nei = dataInput.readUTF();

        String neighborsName[] = nei.split(",");
        for (String neighbor : neighborsName) {
            neighbors.add(neighbor.trim());
        }


    }

    @Override
    public String toString() {
        return "#" + pageRank + "#" + neighbors + "#" + isNode;
    }


    public int compareTo(Object o) {
        return pageRank.compareTo(((Node)o).pageRank);
    }
}
