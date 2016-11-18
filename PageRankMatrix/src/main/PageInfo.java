package main;

public class PageInfo {
	
	Long pageId;
	Double pageRank;
	
	public PageInfo(Long pageId, Double pageRank){
		this.pageId = pageId;
		this.pageRank = pageRank;
	}
	
	public Long getPageId() {
		return pageId;
	}
	public void setPageId(Long pageId) {
		this.pageId = pageId;
	}
	public Double getPageRank() {
		return pageRank;
	}
	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}
	
	

}
