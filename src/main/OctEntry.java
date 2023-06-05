package main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class OctEntry implements Serializable{
	    private OctPoint point;
	    private String primaryKey;
	    private String tableName;
	    private Vector<Integer> pageIDs = new Vector<>();
	   // private int pageID;
	    
	    
 public OctEntry(OctPoint point, String primaryKey, String tableName,Vector<Integer> pageIDs) {
			
			this.point = point;
			this.primaryKey = primaryKey;
			this.tableName = tableName;
			//this.pageIDs.add(pageID);
			this.pageIDs = pageIDs;
			//this.pageID = pageID;
		}


public OctPoint getPoint() {
	return point;
}


public String getPrimaryKey() {
	return primaryKey;
}



public String getTableName() {
	return tableName;
}



public Vector getPageIDs() {
	return pageIDs;
}
//public int getPageID() {
//	return pageID;
//}
 
public String toString() {
	return this.point.getX() + " " +this.point.getY() + " " + this.point.getZ() ;
}

}
