package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends Reducer<Text, Node, Text, Node> {

    int iteration;
    double alpha;
    long pageCount;
    double danglingNodesPRSum;
    double constantTerm;
    Node nodeWithAdjNodes = new Node();

     /**
     * This setup method extracts the required information from Map reduce job configuration and
     * initialize the parameter such as current interation number, total number of pages
     * in the Wiki graph, damping factor(alpha) and sum of page rank of dangling nodes.
     * @param context
     */
    public void setup(Context context) {

        Configuration conf = context.getConfiguration();
        iteration = conf.getInt(code.Constant.ITERATION, -10);
        pageCount = conf.getLong(code.Constant.PAGE_COUNT, -10L);
        alpha = conf.getDouble(code.Constant.ALPHA, -10);
        danglingNodesPRSum = conf.getDouble(Constant.DANGLING_NODES_PR_SUM, 0);
        danglingNodesPRSum = danglingNodesPRSum / Math.pow(10, 12);

        if (iteration == -10 || pageCount == -10 || alpha == -10) {
            throw new Error("Didn't propagate on Page Rank Reducer");
        }

        // To calculate new page rank below equation would be constant for each page so
        // calculating the values in the setup only.
        constantTerm = ((alpha / pageCount) + (1 - alpha) * (danglingNodesPRSum / pageCount));
        System.out.println("Constant Term value is : " + constantTerm);
    }

    public void reduce(Text key, Iterable<Node> nodes,
                       Context context) throws IOException, InterruptedException {

        // Calculate New Page Rank for the give page (key)
        double pageRankContributionSum = 0d;
        // If current page doesn't have an adjacency page list attached to it then its dead node.
        boolean deadNode = true;

        for (Node node : nodes) {

            if (node.isOnlyPageRankContribution()) {
                //Sum the contribution from the other pages
                pageRankContributionSum += node.getPageRankContribution();
            } else {
                nodeWithAdjNodes.setAdjacencyNodes(node.getAdjacencyNodes());
                // Current page  have an adjacency page list attached to it so its not a dead node.
                deadNode = false;
            }
        }

        // Ignore the dead nodes.
        if (deadNode) {
            return;
        }

        // New Page Rank
        double newPageRank = (constantTerm + (1 - alpha) * pageRankContributionSum);
        nodeWithAdjNodes.setPageRank(newPageRank);

        // If current node is dangling node then increment global counter which keeps the track of
        // sum of page rank of dangling node.
        if (nodeWithAdjNodes.getAdjacencyNodes().size() == 0) {
            context.getCounter(COUNTERS.DANGLING_NODE_PR_SUM).increment((long) (newPageRank * Math.pow(10, 12)));
        }

        // Emit the node which has new page rank and adjacency page list
        context.write(key, nodeWithAdjNodes);
    }
}