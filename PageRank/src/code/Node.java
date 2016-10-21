package code;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Node implements WritableComparable<Node> {

	public double pageRank;
	public List<Text> adjacencyNodes = new LinkedList<Text>();
	
	public Node(){
		
	}
	
	public Node(List<String> adjacencyNodes){
		for(String value : adjacencyNodes){
			this.adjacencyNodes.add(new Text(value));
		}
	}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(pageRank);
		out.writeInt(adjacencyNodes.size());
		for(Text node : adjacencyNodes){
			node.write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		pageRank = in.readDouble();
		int length = in.readInt();
		adjacencyNodes = new LinkedList<Text>();
		for(int i = 0; i < length; i++){
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


	public double getPageRank() {
		return pageRank;
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

	
}
