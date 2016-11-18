package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class keeps track of Page rank and adjacnecy page list for a source page. Apart from that,
 * it can be used to keep track of the page rank contribution from the other pages.
 *
 * isOnlyPageRankContribution flag decides which functionality this class provides.
 *
 * For page rank contribution, isOnlyPageRankContribution flag is true
 * For Page rank and adjacency list, isOnlyPageRankContribution flag is false
 */
public class Node implements WritableComparable<Node> {

	private String pageName = "";
    private boolean isDangling;
    private List<Text> adjacencyNodes;

    public Node() {
        this.adjacencyNodes = new LinkedList<Text>();
        this.isDangling = false;
    }

    public Node(boolean isDangling) {
        this.adjacencyNodes = new LinkedList<Text>();
        this.isDangling = isDangling;
    }

    public Node(List<String> adjacencyNodes) {
        this.isDangling = false;
        this.adjacencyNodes = new LinkedList<Text>();
        for (String value : adjacencyNodes) {
            this.adjacencyNodes.add(new Text(value));
        }
    }

    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
    	out.writeUTF(pageName);
        out.writeBoolean(isDangling);
        out.writeInt(adjacencyNodes.size());
        for (Text node : adjacencyNodes) {
            node.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
    	pageName = in.readUTF();
    	isDangling = in.readBoolean();
        int length = in.readInt();
        adjacencyNodes = new LinkedList<Text>();
        for (int i = 0; i < length; i++) {
            Text text = new Text();
            text.readFields(in);
            adjacencyNodes.add(text);
        }

    }

    public String toString() {
        return (adjacencyNodes.toString());
    }

    public int compareTo(Node o) {
        // TODO Auto-generated method stub
        return 0;
    }

    public List<Text> getAdjacencyNodes() {
        return adjacencyNodes;
    }

    public void setAdjacencyNodes(List<Text> adjacencyNodes) {
        this.adjacencyNodes = adjacencyNodes;
    }

    public void setAdjacencyStringNodes(List<String> adjacencyNodes) {

        this.adjacencyNodes.clear();
        for (String adj : adjacencyNodes) {
            this.adjacencyNodes.add(new Text(adj));
        }
    }

    public boolean isDangling() {
        return this.isDangling;
    }

    public void setIsDangling(boolean isDangling) {
        this.isDangling = isDangling;
    }

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

}