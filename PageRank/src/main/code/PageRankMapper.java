package code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Node, Text, Node> {

    int iteration;
    long pageCount;
    Node pageRankContributionNode = new Node();

    /**
     * This setup method extracts the required information from Map reduce job configuration and
     * initialize the parameter such as current interation number and total number of pages
     * in the Wiki graph
     * @param context
     */
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        iteration = conf.getInt(Constant.ITERATION, -10);
        pageCount = conf.getLong(Constant.PAGE_COUNT, -10);

        if (iteration == -10 || pageCount == -10) {
            throw new Error("Didn't propagate iteration or page count");
        }
    }

    public void map(Text key, Node value, Context context) throws IOException, InterruptedException {

        // At first interation, page rank of each page is equal to 1/N
        if (iteration == 0) {
            value.setPageRank((((double)1) / pageCount)); // * Math.pow(10, 12)
        }

        int adjLen = value.getAdjacencyNodes().size();

        // Current page contribution to its adjacency page nodes.
        for (Text adjPageName : value.getAdjacencyNodes()) {
            // Set IsOnlyPageRankContribution flag to true as the Node object here only contains the page
            // rank contribution from the source page to its adajacency pages.
            pageRankContributionNode.setIsOnlyPageRankContribution(true);
            pageRankContributionNode.setPageRankContribution(value.getPageRank() / adjLen);
            context.write(adjPageName, pageRankContributionNode);
        }

        // Transfer the current page and its corresponding node which contains its page rank
        // and adjanceny page list for reducer.
        context.write(key, value);
    }
}
