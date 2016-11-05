import java.util.LinkedList;
import java.util.List;

/**
 * Created by Darshan on 11/3/16.
 */
public class PageNode {

    String pageName;
    List<String> adjPages;

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public List<String> getAdjPages() {
        return adjPages;
    }

    public void setAdjPages(List<String> adjPages) {
        this.adjPages = adjPages;
    }
}
