package package1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/20/18
 * @email thakker.m@husky.neu.edu
 */
public class Node implements Serializable {

    public String id = "PAGERANKNODE";
    public Double pageRank = -1.0;
    public Set<String> neighbors;
    public boolean isNode = false;
    public static final long SCALEUP = 1000000000l;

    // invoke for creating pagerank
    public Node(Double pageRank) {
        this.id = id.trim();
        this.pageRank = pageRank;
        this.neighbors = new HashSet<String>();
        this.isNode = false;
    }

    // invoke for creating a node with
    public Node(String id, Double pageRank, Set neighbors) {
        this.id = id.trim();
        this.pageRank = pageRank;
        this.neighbors = neighbors;
        this.isNode = true;

    }

    @Override
    public String toString() {
        if (id.equals("")) {
            System.out.println("Error Id null");
        }
        return id + "#" + pageRank + "#" + neighbors + "#" + isNode;
    }

    public static Node toNode(String node) throws Exception {


        String nodeparams[] = node.split("#");


        String id = nodeparams[0];
        Double pagerank = Double.parseDouble(nodeparams[1]);
        String neigh[] = nodeparams[2].substring(1, nodeparams[2].length() - 1).split(",");
        Set neighs = new HashSet(Arrays.asList(neigh));
        Boolean isNode = Boolean.parseBoolean(nodeparams[3]);

        Node node1 = new Node(pagerank);
        node1.id = id;
        node1.isNode = isNode;
        node1.neighbors = neighs;
        node1.pageRank = pagerank;

        return node1;
    }

}