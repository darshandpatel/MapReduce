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

    private double pageRank;
    private double pageRankContribution;
    private boolean isOnlyPageRankContribution;
    private List<Text> adjacencyNodes;

    public Node() {
        this.adjacencyNodes = new LinkedList<Text>();
        this.isOnlyPageRankContribution = false;
    }

    public Node(boolean isOnlyPageRankContribution) {
        this.adjacencyNodes = new LinkedList<Text>();
        this.isOnlyPageRankContribution = isOnlyPageRankContribution;
    }

    public Node(List<String> adjacencyNodes) {
        this.isOnlyPageRankContribution = false;
        this.adjacencyNodes = new LinkedList<Text>();
        for (String value : adjacencyNodes) {
            this.adjacencyNodes.add(new Text(value));
        }
    }

    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeBoolean(isOnlyPageRankContribution);
        out.writeDouble(pageRankContribution);
        out.writeDouble(pageRank);
        out.writeInt(adjacencyNodes.size());
        for (Text node : adjacencyNodes) {
            node.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        isOnlyPageRankContribution = in.readBoolean();
        pageRankContribution = in.readDouble();
        pageRank = in.readDouble();
        int length = in.readInt();
        adjacencyNodes = new LinkedList<Text>();
        for (int i = 0; i < length; i++) {
            Text text = new Text();
            text.readFields(in);
            adjacencyNodes.add(text);
        }

    }

    public String toString() {
        return (Double.toString(pageRank) + " : " + adjacencyNodes.toString());
    }

    public int compareTo(Node o) {
        // TODO Auto-generated method stub
        return 0;
    }

    public double getPageRank() {
        return this.pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
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

    public boolean isOnlyPageRankContribution() {
        return isOnlyPageRankContribution;
    }

    public void setIsOnlyPageRankContribution(boolean isOnlyPageRankContribution) {
        this.isOnlyPageRankContribution = isOnlyPageRankContribution;
    }

    public double getPageRankContribution() {
        return pageRankContribution;
    }

    public void setPageRankContribution(double pageRankContribution) {
        this.pageRankContribution = pageRankContribution;
    }


}