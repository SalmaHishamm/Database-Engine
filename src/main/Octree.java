package main;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Vector;

import utilities.Metadata;



public class Octree implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private OctNode root;
	private String tableName;
	private String[] strarrColName;
	private String octreeName;
	
	
	
	public Octree(String tableName, String[] strarrColName  ,Object x1 , Object y1, Object z1, Object x2 , Object y2, Object z2) throws DBAppException  {
		OctPoint min= new OctPoint(x1,y1,z1);
		OctPoint max= new OctPoint(x2,y2,z2);
		root= new OctNode(min,max);
		this.strarrColName=strarrColName;
		this.tableName=tableName;
		octreeName= generateIndexName(strarrColName);
		
		Metadata.rewriteMetadata(tableName, strarrColName, octreeName);
		
	}
	
	
	public void insert ( Object x , Object y, Object z, String primaryKey, String tableName, int pageID) throws DBAppException  {
		Vector<Integer> pageIDs= new Vector <>();
		
		pageIDs.add(pageID);
		root.insert(x,y,z,primaryKey,tableName,pageIDs );
	}
	public boolean remove (Object x , Object y , Object z) {
		return root.remove(x, y, z);
	}
	
	public void update (Object x , Object y , Object z,String primaryKey, String tableName,int newPageID) throws DBAppException {
        Vector<Integer> pageIDs= new Vector <>();
		
		pageIDs.add(newPageID);
		root.update(x,y,z, primaryKey,  tableName,pageIDs);
	}
	
	public int searchUpdate( Object primaryKey , char coordinate) {
	   
	    return root.searchUpdate(primaryKey, coordinate);
	}


	public OctNode getRoot() {
		return root;
	}


	public String getOctreeName() {
		return octreeName;
	}


	public String[] getStrarrColName() {
		return strarrColName;
	}


	public String getTableName() {
		return tableName;
	}
	public static String generateIndexName(String[] strarrColName) {
		String indexName="" ;
		for ( String colName : strarrColName) {
			indexName+=colName;
		}
		indexName+="index";
		return indexName;
		
		
	}
	public String toString() {
		return root.toString();
	}


	public boolean find(Object x, Object y, Object z) {
		
		return root.find(x,y,z);
	}
    public OctEntry findEntry(Object x, Object y, Object z) {
		
		return root.findEntry(x,y,z);
	}
    
    public Vector<Integer> searchDelete(Object x, Object y, Object z) throws DBAppException{
    	if(root.findEntry(x,y,z)==null) {
    		throw new DBAppException("Value not found");
    	}
		Vector<Integer> result = root.findEntry(x,y,z).getPageIDs();
    	return new Vector<>(new HashSet<Integer>(result));
    }
    
    public Vector<Integer> searchExactQuery(Object x, Object y, Object z) throws DBAppException{
    	if(root.findEntry(x,y,z)==null) {
    		throw new DBAppException("Value not found");
    	}
		Vector<Integer> result = root.findEntry(x,y,z).getPageIDs();
		
    	return new Vector<>(new HashSet<Integer>(result));
    }
    public Vector<Integer> searchRangeQuery(Object x , Object y , Object z , String opx ,  String opy, String opz) throws DBAppException{
    	 Vector<OctEntry> Moutput =root.SearchRange(x, y, z, opx, opy, opz);
    	 System.out.print(x);
    	 System.out.print(y);
    	 System.out.print(z);
    	 for (int i=0;i<Moutput.size();i++) {
    		
    		 System.out.print(Moutput.get(i).toString());
    	 }
    	 Vector<Integer> result = new Vector<>();
    	 
    	for (int i=0;i<Moutput.size();i++) {
    		result.addAll(Moutput.get(i).getPageIDs());
    	}
    		
    	
        return  new Vector<>(new HashSet<Integer>(result));
    }
	   

}
