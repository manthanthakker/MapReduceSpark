package package1;

import java.util.Set;

/**
 * @author Manthan Thakker
 * @project pagerrankhw4
 * @date 3/18/18
 * @email thakker.m@husky.neu.edu
 */
public class NodeIdNeighbors {
    Set<String> neighbors;
    String id;

    public Set<String> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(Set<String> neighbors) {
        this.neighbors = neighbors;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public NodeIdNeighbors(String id, Set<String> neighbors) {
        this.neighbors = neighbors;
        this.id = id;
    }
}
