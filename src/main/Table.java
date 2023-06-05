package main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import utilities.Metadata;
import utilities.NullWrapper;
import utilities.Serialization;

public class Table implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String name;
	private final Vector<String> columns;
	private Vector<String> columnsNotIndexed;
	private int rowsPerPage;
	private final String clusteringKey;
	private final String clusteringKeyType;
//	private final transient Metadata metadata;
	private final Vector<Integer> pages; //stores pages ID
	private Vector<String []> indices = new Vector<String[]>();
	//private Vector<Octree> octreesOfTable = new Vector<Octree>();

	public Table(String strTableName ,String strClusteringColumn , 
			Hashtable<String,String> htblColNameType, 
            Hashtable<String,String> htblColNameMin, 
            Hashtable<String,String> htblColNameMax) throws DBAppException {
		
		super();
		this.name = strTableName;
		this.clusteringKey = strClusteringColumn;
		this.pages = new Vector<Integer>();
		
		columns = new Vector<String>();
		this.columns.addAll(htblColNameType.keySet());
		this.clusteringKeyType = htblColNameType.get(strClusteringColumn);
		
		columnsNotIndexed = new Vector<String>();
		this.columnsNotIndexed.addAll(htblColNameType.keySet());
		
		//extracting data from DBApp.config
		readconf();
		
		//creating metadata
		Hashtable<String,Hashtable<String,Object>> columnDefinitions = new Hashtable<String,Hashtable<String,Object>>();
		
		
		for(String column: columns) {
			Hashtable<String,Object> definitions = new Hashtable<String,Object>();
			
			//validate that column type has a valid data type before writing in metadata file
			String type = htblColNameType.get(column);
	        if (!(type.equals("java.lang.Integer") || type.equals("java.lang.Double") || type.equals("java.lang.String") || type.equals("java.util.Date"))) {
	        	throw new DBAppException("Invalid Column "+"'"+ column +"'"+" Data Type.");
	        }
			//
	          
	        //validate that min and max have a valid data type before writing in metadata file
	        if (!Metadata.isValidDataType(type,htblColNameMin.get(column)) || !Metadata.isValidDataType(type,htblColNameMax.get(column))) {
	        	throw new DBAppException("Invalid Minimum and Maximum "+"'"+ column +"'"+" Data Type.");
	        }
			//
	        
	        //validate that min and max have a valid values before writing in metadata file (min cannot be greater than max)
	        if (Metadata.compareObjects(htblColNameMin.get(column), htblColNameMax.get(column)) > 0) {
	        	throw new DBAppException("Invalid Minimum and Maximum "+"'"+ column +"'"+" Values. Minimum cannot be greater than Maximum");
	        }
			//
	          
			definitions.put("type", htblColNameType.get(column));
			definitions.put("isClusteringKey",(column== strClusteringColumn));
			definitions.put("indexName", false); // default for now
			definitions.put("indexType", false); // default for now
			
			if(htblColNameType.get(column).equals("java.lang.String")) {
				definitions.put("min", Metadata.parseData(htblColNameType.get(column), ((String)htblColNameMin.get(column)).toLowerCase()));
				definitions.put("max", Metadata.parseData(htblColNameType.get(column), ((String)htblColNameMax.get(column)).toLowerCase()));
			}else {
				definitions.put("min", Metadata.parseData(htblColNameType.get(column), htblColNameMin.get(column)));
				definitions.put("max", Metadata.parseData(htblColNameType.get(column), htblColNameMax.get(column)));
			}
			
			
			columnDefinitions.put(column, definitions);
			
		}
		
		Metadata metadata = new Metadata(name,columnDefinitions);
		metadata.writeMetadata();
	}

	public void readconf() throws DBAppException {
		
		Properties props = new Properties();
		InputStream in =null;
		ArrayList<Integer> configRead= new ArrayList<Integer>();
		
			try {
				in= new FileInputStream("resources/DBApp.config");
				props.load(in);
			   
			} catch (Exception e) {
				throw new DBAppException(e.getLocalizedMessage());
			}
			
			configRead.add(Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage")));
			configRead.add(Integer.parseInt(props.getProperty("MaximumEntriesinOctreeNode")));
			
			rowsPerPage=configRead.get(0);
}
	
	public void insert(Hashtable<String,Object> htblColNameValue) throws DBAppException { // could return boolean too
		
		/*
		//1: No inserting in the middle: check if pages is empty or pages.lastElement().isFull() : yes -> create new Page & call page.insert
																							//  no -> call pages.lastElement().insert
		//OR
		
		//2: Inserting in the middle: check if pages is empty : yes -> create new Page & call page.insert
			//  no -> loop over pages and check whether inputClusteringValue belongs between the min&maxclusteringValue of the page
					//yes: -> insert into that page & call page.insert, if not full (otherwise 'shift' to next page to make room. getlastRecord and insert in next page) 
					//no: -> continue looping till the end, if page = pages.lastElement() && not full insert into this page, if full, create & insert in new Page
		*/
		
//--------------------------------------(2: implementation, works but does not use binary search or loadPage)---------------------------------------------------		
	
// 2: implementation example:
		
		//DONE: handle values less than very first entry
		//DONE:  handle (prevPageMax < nextPageMin): Metadata.castToCompare(clusteringKeyType,prevPage.getMaxClusterValue(), nextPage.getMinClusterValue() ) <0
		
		

			//do appropriate checks (readMetadata file...etc)
				//1: checking number of columns is same as metadata
				//2: DONE: checking if columns inserted into exist in metadata
				//3: DONE: checking if columns have the same data type as metadata
			
			Hashtable<String, Hashtable<String, Object>> metadata = Metadata.readMetadata(name);
			
			if(!htblColNameValue.keySet().contains(clusteringKey)) {
				throw new DBAppException("A value for the primary key is not included");
			}
			
			if(!metadata.keySet().containsAll(htblColNameValue.keySet())) {
				throw new DBAppException("Column to insert into does not exist");
			}
			
			for(String column :metadata.keySet()) {
				
				if(htblColNameValue.get(column) != null) {
					if(htblColNameValue.get(column) != null && Metadata.stringData(metadata.get(column).get("type").toString(),htblColNameValue.get(column)) == null) {
						throw new DBAppException("Invalid column '"+column+"' " +"data type");
					}
					
					Object leastPossibleValue = metadata.get(column).get("min");
					int leastPossibleCompare = Metadata.castToCompare(metadata.get(column).get("type").toString(), htblColNameValue.get(column), leastPossibleValue);
					if(leastPossibleCompare<0) {
						throw new DBAppException("Insert value '"+htblColNameValue.get(column)+"' is less than the minimum possible value '"+ leastPossibleValue+"'");
					}
					
					Object largestPossibleValue = metadata.get(column).get("max");
					int largestPossibleCompare = Metadata.castToCompare(metadata.get(column).get("type").toString(), htblColNameValue.get(column), largestPossibleValue);
					if(largestPossibleCompare>0) {
						throw new DBAppException("Insert value '"+htblColNameValue.get(clusteringKey)+"' is greater than the maximum possible value '"+ largestPossibleValue+"'");
					}
				}else {
					//htblColNameValue.put(column, new NullWrapper());
					throw new DBAppException("Column "+column+ " cannot have null entries.");
				}
				
			}
			//metadata checks above
			
			Page page;
			//if no pages were created before (first insert)
			if(pages.isEmpty()) {
				//create and insert in new first page
				page = new Page(name, rowsPerPage, clusteringKey,clusteringKeyType, 1);
				pages.add(page.getId());
				page.insert(htblColNameValue);
				
				for(String[] index : indices) {
					Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
					octree.insert(htblColNameValue.get(index[0]),htblColNameValue.get(index[1]),htblColNameValue.get(index[2]),clusteringKey,name,page.getId());
					Serialization.writeIndex(octree);
				}
			}
			else {

//----------------------------------------(Attempting to use binarySearch and loadPage, works)------------------------------------------------------------------------				
				
				for(int i = 0; i<pages.size(); i++) {
					page = Serialization.loadPage(name, pages.get(i));
					
					//if primary insert value already exists throw DBAppException
					
					if(page.binarySearch(htblColNameValue.get(clusteringKey)) != null) {
						throw new DBAppException("Primary key already exists in table");
					}
					
					//check whether to insert if: last page || less than current page max || greater than current page max and less than next page min and current page !Full (last condition reason is: insert after delete)
					if(page.getId() == Serialization.loadPage(name,pages.get(pages.size()-1)).getId() || Metadata.compareObjects(htblColNameValue.get(clusteringKey), page.getMaxClusterValue())<0 || (!page.isFull() && Metadata.compareObjects(htblColNameValue.get(clusteringKey), Serialization.loadPage(name, pages.get(i+1)).getMinClusterValue())<0 && Metadata.compareObjects(htblColNameValue.get(clusteringKey), page.getMaxClusterValue())>0)) {
						
						if(page.isFull()) {
							if(page.getId() == Serialization.loadPage(name,pages.get(pages.size()-1)).getId()) {
								Page newPage = new Page(name, rowsPerPage, clusteringKey,clusteringKeyType, page.getId()+1);
								pages.add(newPage.getId());
								
								if(Metadata.compareObjects(htblColNameValue.get(clusteringKey), page.getMaxClusterValue())>0) {
									newPage.insert(htblColNameValue);
									
									for(String[] index : indices) {
										Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
										octree.insert(htblColNameValue.get(index[0]),htblColNameValue.get(index[1]),htblColNameValue.get(index[2]),clusteringKey,name,page.getId());
										Serialization.writeIndex(octree);
									}
									return;
								}
								
							}
							
							//remove lastRecord from index
							for(String[] index : indices) {
								if(!page.getRecords().isEmpty()) {
									Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
									octree.remove(page.getRecords().lastElement().get(index[0]), page.getRecords().lastElement().get(index[1]), page.getRecords().lastElement().get(index[2]));
									Serialization.writeIndex(octree);
								}
							}
							
							insertIntoNextPage(i+1, page.getRecords().lastElement());
							page.getRecords().remove(page.getRecords().lastElement());
							
							
						}
						
						page.insert(htblColNameValue);
						
						//if(indexExists())
						
						for(String[] index : indices) {
							Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
							octree.insert(htblColNameValue.get(index[0]),htblColNameValue.get(index[1]),htblColNameValue.get(index[2]),clusteringKey,name,page.getId());
							Serialization.writeIndex(octree);
						}
						
						page = null; 
						
						return;
					}
					
					page = null;
				}
				
//-----------------------------------------------(Works but only using pages Vector, NOT loadPage)------------------------------------------------------------				
/*				
				//loop over pages
				for(int i = 0; i<pages.size(); i++) {
					Page page = pages.get(i);
					
					
						//if primary insert value already exists throw DBAppException
					
						boolean primaryAlreadyExist = false;
						for(Hashtable<String,Object> element : page.getRecords()) {	
							if(element.get(clusteringKey).equals(htblColNameValue.get(clusteringKey))) {
								primaryAlreadyExist = true;
								break;
							}
						}
						
						if(primaryAlreadyExist) {
							throw new DBAppException("Primary key already exists in table");
						}
						else {
							
							//used in upcoming check
							int minCompare = Metadata.castToCompare(clusteringKeyType, htblColNameValue.get(clusteringKey), page.getMinClusterValue());
							int maxCompare = Metadata.castToCompare(clusteringKeyType, htblColNameValue.get(clusteringKey), page.getMaxClusterValue());
							int minOverlapCompare =0;
							int maxOverlapCompare=0;
							if(!page.equals(pages.get(0))) {
								Page prevPage = pages.get(i-1);
								minOverlapCompare =Metadata.castToCompare(clusteringKeyType, htblColNameValue.get(clusteringKey), prevPage.getMaxClusterValue());
								maxOverlapCompare =Metadata.castToCompare(clusteringKeyType, htblColNameValue.get(clusteringKey), page.getMinClusterValue());
							}
//							else {
//								//if first page, compare insert value with firstValue in table and leastPossibleValue that can be entered (leastPossibleValue< insert <firstValue)
//								Hashtable<String, Hashtable<String, Object>> metadata = Metadata.readMetadata(name);
//								Object leastPossibleValue = metadata.get(clusteringKey).get("min");
//								leastPossibleCompare = Metadata.castToCompare(clusteringKeyType, htblColNameValue.get(clusteringKey), leastPossibleValue);
//							}
							
							//check if insertion should be done in this (i)th page
							if((minCompare >0 && maxCompare<0) || (minOverlapCompare>0 && maxOverlapCompare<0) || (page.equals(pages.get(0)) && (minCompare<0 && Metadata.castToCompare(clusteringKeyType, htblColNameValue.get(clusteringKey),metadata.get(clusteringKey).get("min") ) >=0) ) ) {
								
								//if page is full insertIntoNextPage (if there's no next page, create one and insert into it)
								if(page.isFull()) {
									if(page.equals(pages.lastElement())) {
										Page newPage = new Page(name, rowsPerPage, clusteringKey,clusteringKeyType, pages.size() + 1);
										pages.add(newPage);
									}
									insertIntoNextPage(i+1, page.getRecords().lastElement());
									page.getRecords().remove(page.getRecords().lastElement());
//									Serialization.writePage(page);
									
								}
								page.insert(htblColNameValue);
								break;
							}
							
							//in case of: order of the insert is in the last page (does not belong to pages in the middle)
							if(page.equals(pages.lastElement())) {
								if(page.isFull()) {
									Page newPage = new Page(name, rowsPerPage, clusteringKey,clusteringKeyType, pages.size() + 1);
									newPage.insert(htblColNameValue);
									pages.add(newPage);
									break;
								}
								else {
									page.insert(htblColNameValue);
									break;
								}
							}
						}	
				}
*/
			
			}


//-------------------------------------------------------------------------------------------------------------------------------------------------------------------	
		
		
	}
	
	public void insertIntoNextPage(int nextPageIndex, Hashtable<String,Object> htblColNameValue) throws DBAppException {

		Page nextPage = Serialization.loadPage(name, pages.get(nextPageIndex));
		
		if(!nextPage.isFull()) {
			nextPage.insert(htblColNameValue);
			
			for(String[] index : indices) {
				Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
				octree.insert(htblColNameValue.get(index[0]),htblColNameValue.get(index[1]),htblColNameValue.get(index[2]),clusteringKey,name,nextPage.getId());
				Serialization.writeIndex(octree);
			}
			
			return;
		}else {
				if(nextPage.getId() == Serialization.loadPage(name,pages.get(pages.size()-1)).getId()){ //nextPage.equals(pages.lastElement())) {
					Page page = new Page(name, rowsPerPage, clusteringKey,clusteringKeyType, nextPage.getId()+1);
					pages.add(page.getId());
				}
				
				//remove lastRecord from index
				for(String[] index : indices) {
					if(!nextPage.getRecords().isEmpty()) {
						Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
						octree.remove(nextPage.getRecords().lastElement().get(index[0]), nextPage.getRecords().lastElement().get(index[1]), nextPage.getRecords().lastElement().get(index[2]));
						Serialization.writeIndex(octree);
					}
				}
				
				insertIntoNextPage(nextPageIndex +1, nextPage.getRecords().lastElement());
				
				nextPage.getRecords().remove(nextPage.getRecords().lastElement());
				nextPage.insert(htblColNameValue);
				
				//insert newRecord into index
				for(String[] index : indices) {
					Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
					octree.insert(htblColNameValue.get(index[0]),htblColNameValue.get(index[1]),htblColNameValue.get(index[2]),clusteringKey,name,nextPage.getId());
					Serialization.writeIndex(octree);
				}
				
				
		}
		return;
/*		
		Page nextPage = pages.get(nextPageIndex);
		
			if(!nextPage.isFull()) {
				nextPage.insert(htblColNameValue);
				return;
			}else {
					if(nextPage.equals(pages.lastElement())) {
						Page page = new Page(name, rowsPerPage, clusteringKey,clusteringKeyType, pages.size() + 1);
						pages.add(page);
					}
					insertIntoNextPage(nextPageIndex +1, nextPage.getRecords().lastElement());
					nextPage.getRecords().remove(nextPage.getRecords().lastElement());
//					Serialization.writePage(nextPage);
					nextPage.insert(htblColNameValue);
					
					
			}
			return;
*/			
	}

	public void update(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		////do appropriate checks (readMetadata file...etc)
			//1: checking number of columns is same as metadata
			//2: DONE: checking if columns inserted into exist in metadata
			//3: DONE: checking if columns have the same data type as metadata
	
		Hashtable<String, Hashtable<String, Object>> metadata = Metadata.readMetadata(name);
		
		if(htblColNameValue.keySet().contains(clusteringKey)) {
			throw new DBAppException("The primary key cannot be updated");
		}
		
		if(!metadata.keySet().containsAll(htblColNameValue.keySet())) {
			throw new DBAppException("Column to insert into does not exist");
		}
		
		for(String column :metadata.keySet()) {
			
			if(htblColNameValue.get(column) != null) {
				if(htblColNameValue.get(column) != null && Metadata.stringData(metadata.get(column).get("type").toString(),htblColNameValue.get(column)) == null) {
					throw new DBAppException("Invalid column '"+column+"' " +"data type");
				}
				
				Object leastPossibleValue = metadata.get(column).get("min");
				int leastPossibleCompare = Metadata.castToCompare(metadata.get(column).get("type").toString(), htblColNameValue.get(column), leastPossibleValue);
				if(leastPossibleCompare<0) {
					throw new DBAppException("Insert value '"+htblColNameValue.get(column)+"' is less than the minimum possible value '"+ leastPossibleValue+"'");
				}
				
				Object largestPossibleValue = metadata.get(column).get("max");
				int largestPossibleCompare = Metadata.castToCompare(metadata.get(column).get("type").toString(), htblColNameValue.get(column), largestPossibleValue);
				if(largestPossibleCompare>0) {
					throw new DBAppException("Insert value '"+htblColNameValue.get(clusteringKey)+"' is greater than the maximum possible value '"+ largestPossibleValue+"'");
				}
			}else {
				if(!column.equals(clusteringKey)) {
					throw new DBAppException("Column "+column+ " cannot have null entries.");
				}
			}
			
		}
		//metadata checks above
		
		if(pages.isEmpty()) {
			throw new DBAppException("Table is empty");
		}
		else {
			
			if(Metadata.indexExists(name, clusteringKey)) {
				
				
				for(String[] index : indices) {
					Vector<String> vecIndex = new Vector<String>();
					for(int i = 0; i<index.length ;i++) {
						vecIndex.add(index[i]);
					}
					if(vecIndex.contains(clusteringKey)) {
						char coordinate = 0;
						switch(vecIndex.indexOf(clusteringKey)) {
							case 0: coordinate = 'x';break;
							case 1: coordinate = 'y'; break;
							case 2: coordinate = 'z';break;
						}
						
						Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
						
						int pageId = octree.searchUpdate(Metadata.parseData(clusteringKeyType, strClusteringKeyValue), coordinate);
						
						if(pageId == -1) {
							throw new DBAppException("There is no entry with the given clustering value");
						}
						
						Page page = Serialization.loadPage(name, pageId);
						
						page.update(strClusteringKeyValue, htblColNameValue);
						
						switch(vecIndex.indexOf(clusteringKey)) {
							case 0: octree.update(Metadata.parseData(clusteringKeyType, strClusteringKeyValue), htblColNameValue.get(index[1]), htblColNameValue.get(index[2]), clusteringKey, name, pageId);break;
							case 1: octree.update(htblColNameValue.get(index[0]), Metadata.parseData(clusteringKeyType, strClusteringKeyValue), htblColNameValue.get(index[2]), clusteringKey, name, pageId); break;
							case 2: octree.update(htblColNameValue.get(index[0]), htblColNameValue.get(index[1]),Metadata.parseData(clusteringKeyType, strClusteringKeyValue) , clusteringKey, name, pageId);break;
						}
						
						
						Serialization.writeIndex(octree);
						
						return;
					}
				}
				
			}else {
				for(int i = 0; i<pages.size(); i++) {
					Page page = Serialization.loadPage(name, pages.get(i));
					
					//found page that has record to update
					if(Metadata.compareObjects(Metadata.parseData(clusteringKeyType, strClusteringKeyValue), page.getMaxClusterValue()) <=0) {
						if(!page.update(strClusteringKeyValue, htblColNameValue)) {
							page = null;
							throw new DBAppException("There is no entry with the given clustering value");
						}
						page = null;
						return;
					}
				}
				
				throw new DBAppException("There is no entry with the given clustering value");
			}
		}
		
	}
	
	public void delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		//do appropriate checks (readMetadata file...etc)
		
		Hashtable<String, Hashtable<String, Object>> metadata = Metadata.readMetadata(name);
		
		if(!metadata.keySet().containsAll(htblColNameValue.keySet())) {
			throw new DBAppException("Column to delete from does not exist");
		}
		
		for(String column :metadata.keySet()) {
			
			if(htblColNameValue.get(column) != null) {
				if(htblColNameValue.get(column) != null && Metadata.stringData(metadata.get(column).get("type").toString(),htblColNameValue.get(column)) == null) {
					throw new DBAppException("Invalid column '"+column+"' " +"data type");
				}
				
				Object leastPossibleValue = metadata.get(column).get("min");
				int leastPossibleCompare = Metadata.castToCompare(metadata.get(column).get("type").toString(), htblColNameValue.get(column), leastPossibleValue);
				if(leastPossibleCompare<0) {
					throw new DBAppException("Delete value '"+htblColNameValue.get(column)+"' is less than the minimum possible value '"+ leastPossibleValue+"'");
				}
				
				Object largestPossibleValue = metadata.get(column).get("max");
				int largestPossibleCompare = Metadata.castToCompare(metadata.get(column).get("type").toString(), htblColNameValue.get(column), largestPossibleValue);
				if(largestPossibleCompare>0) {
					throw new DBAppException("Delete value '"+htblColNameValue.get(clusteringKey)+"' is greater than the maximum possible value '"+ largestPossibleValue+"'");
				}
			}
			
		}
		//metadata checks above
		
		if(pages.isEmpty()) {
			throw new DBAppException("Table is empty");
		}
		else {
			//with index
			if(htblColNameValue.keySet().size() == 3) {
				
				String [] hashColumns = new String[3];
				
				hashColumns = (String[]) (htblColNameValue.keySet().toArray(new String[3]));
				
				if(foundIndex(hashColumns)) {
					String [] index = getIndex(hashColumns);
					
					Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index));
					
					Vector<Integer> pageIds =octree.searchDelete(htblColNameValue.get(index[0]), htblColNameValue.get(index[1]), htblColNameValue.get(index[2]));
					
					for(int i = 0; i< pageIds.size(); i++) {
						Page page = Serialization.loadPage(name, pageIds.get(i));
						
						//call page.delete() for each page
						page.delete(htblColNameValue);
						
						if(page.isEmpty()) {
							pages.remove(i);
							Serialization.deletePage(page.getTableName(), page.getId());
							i--;
						}
						
						//set page to null so that memory frees up on System.gc() call
						page = null;
					}
					
					octree.remove(htblColNameValue.get(index[0]), htblColNameValue.get(index[1]), htblColNameValue.get(index[2]));
					
					Serialization.writeIndex(octree);
					
				}
	
			}else { //without index
				for(int i = 0; i<pages.size(); i++) {
					Page page = Serialization.loadPage(name, pages.get(i));
					
					//call page.delete() for each page
					page.delete(htblColNameValue);
					
					if(page.isEmpty()) {
						pages.remove(i);
						Serialization.deletePage(page.getTableName(), page.getId());
						i--;
					}
					
					//set page to null so that memory frees up on System.gc() call
					page = null;
				}
			}
		}
		
	}
	
	public Vector<Hashtable<String, Object>> selectIndex(SQLTerm[] arrSQLTerms) throws DBAppException{
		
		Vector<Hashtable<String,Object>> vectorResult = new Vector<Hashtable<String,Object>>();
		
		String[] columnsToCheck = new String [3]; //can change to vector to be dynamic
		Hashtable<String,Hashtable<String,Object>> data = new Hashtable<String,Hashtable<String,Object>>();
		
		
		//can add helper method to be dynamic 'handle case of e a b c where index on a b c
		
		for(int i =0; i<arrSQLTerms.length; i++) {
			
			if(!columns.contains(arrSQLTerms[i]._strColumnName)) {
				throw new DBAppException("Column to insert into does not exist");
			}
			
			
			Hashtable<String,Object> hash = new Hashtable<String,Object>();
			columnsToCheck[i] = arrSQLTerms[i]._strColumnName;
			hash.put("value", arrSQLTerms[i]._objValue);
			hash.put("operator", arrSQLTerms[i]._strOperator);
			data.put(arrSQLTerms[i]._strColumnName, hash);
		}

		
		
		if(foundIndex(columnsToCheck)) {
			
			String [] index = getIndex(columnsToCheck);
			
			Octree octree = Serialization.loadIndex(name, Octree.generateIndexName(index) );
			
//			System.out.println(data.keySet());
//			System.out.println(index[0] + " "+ index[1] + " "+ index[2]);
			System.out.println(data.get(index[0]).get("value") +" "+ data.get(index[1]).get("value")+ data.get(index[2]).get("value"));
			System.out.println((String)data.get(index[0]).get("operator") + (String)data.get(index[1]).get("operator")+ (String)data.get(index[2]).get("operator"));
			
			Vector<Integer> pageIds = octree.searchRangeQuery(data.get(index[0]).get("value"), data.get(index[1]).get("value"), data.get(index[2]).get("value"),(String)data.get(index[0]).get("operator"), (String)data.get(index[1]).get("operator"), (String)data.get(index[2]).get("operator"));
			
			for(int i=0; i< pageIds.size(); i++) {
				
				System.out.println("hello");
				
				Page page = Serialization.loadPage(name, (int)(pageIds.get(i)));
				
				Vector<Hashtable<String,Object>> temp = page.selectIndexExact(data);
				
				vectorResult.addAll(temp);
			}
		}
		
		return vectorResult;
		
	}
	
	public Vector<Hashtable<String, Object>> select(SQLTerm term)
			throws DBAppException{

		
		if(!columns.contains(term._strColumnName)) {
			throw new DBAppException("Column to insert into does not exist");
		}
		
		Vector<Hashtable<String,Object>> vectorResult = new Vector<Hashtable<String,Object>>();
		
		for(int i =0; i<pages.size(); i++) {
			Page page = Serialization.loadPage(name, pages.get(i));
			
			Vector<Hashtable<String,Object>> temp = page.select(term);
			vectorResult.addAll(temp);
			
		}
			
		return vectorResult;
		
	}
	
	public void createIndex(String[] strarrColName)throws DBAppException, IOException {
		
		if (foundIndex(strarrColName)) {
			throw new DBAppException("Index already exists .");
		}
		if ( !checkColumns(strarrColName)) {
			throw new DBAppException("Column name is not correct .");
		}
		for(int i = 0; i<strarrColName.length;i++) {
			if(!columnsNotIndexed.contains(strarrColName[i])) {
				throw new DBAppException(strarrColName[i]+ " already has an index.");
			}
		}
		
		indices.add(strarrColName);
		
		for(int i = 0; i< strarrColName.length; i++) {
			columnsNotIndexed.remove(strarrColName[i]);
		}
		
		//passing min (x,y,z) and max (x,y,
		Hashtable<String, Hashtable<String, Object>> metadata = Metadata.readMetadata(name);
		Octree octree = new Octree(name,strarrColName,metadata.get(strarrColName[0]).get("min"),metadata.get(strarrColName[1]).get("min"),metadata.get(strarrColName[2]).get("min"),metadata.get(strarrColName[0]).get("max"),metadata.get(strarrColName[1]).get("max"),metadata.get(strarrColName[2]).get("max")); //TODO min x , min y , min z , max x , max y , max z
		//octreesOfTable.add(octree);
		
		//if table already populated loop on records to create index
		for (int i =0 ; i <pages.size() ; i++) {
			Page page = Serialization.loadPage(name, pages.get(i));
			for ( int j=0; j<page.getRecords().size(); j++ ) {
				Hashtable<String,Object> record = page.getRecords().get(j);
				Object x = record.get(strarrColName[0]);
				Object y = record.get(strarrColName[1]);
				Object z = record.get(strarrColName[2]);
				octree.insert(x, y, z, clusteringKey, name, page.getId());
				
				
			}
		}
		
		Serialization.writeIndex(octree);
	}
	
	public boolean checkColumns(String[] strarrColName) {
		for ( int i = 0; i<3; i++) {
			if (!columns.contains(strarrColName[i]))
				return false;
		}
		return true;
	}
	
	public boolean foundIndex(String[] strarrColName) {
//		System.out.println("found index line 541");
//		System.out.println(indices.size());
		int count = 0;
		String[] temp = new String[3];
		String[] tempIndex = new String[3];
		for(int i =0; i<strarrColName.length; i++){
			temp[i] = strarrColName[i];
		}
		
		for (int i =0; i<indices.size(); i++) {
			String[] index = indices.get(i);
			
			for(int j =0; j<index.length;j++) {
				tempIndex[j] = index[j];
			}
			
			Arrays.sort(temp);
			Arrays.sort(tempIndex);
			count = 0;
			for(int j=0 ; j<3 ; j++  ) {
				if(temp[j].compareTo(tempIndex[j]) == 0) {
					count ++;
				}	
			}
			if(count ==3 ) {
				break;
			}
		}
		return count ==3 ;
	}
	
	public String[] getIndex(String[] strarrColName) {
//		System.out.println("found index line 541");
//		System.out.println(indices.size());
		int count = 0;
		String[] temp = new String[3];
		String[] tempIndex = new String[3];
		for(int i =0; i<strarrColName.length; i++){
			temp[i] = strarrColName[i];
		}
		
		for (int i =0; i<indices.size(); i++) {
			String[] index = indices.get(i);
			
			for(int j =0; j<index.length;j++) {
				tempIndex[j] = index[j];
			}
			
			Arrays.sort(temp);
			Arrays.sort(tempIndex);
			count = 0;
			for(int j=0 ; j<3 ; j++  ) {
				if(temp[j].compareTo(tempIndex[j]) == 0) {
					count ++;
				}	
			}
			if(count ==3 ) {
				return index;
			}
		}
		return null;
	}

	public String getName() {
		return name;
	}

	public Vector<String> getColumns() {
		return columns;
	}

	public int getRowsPerPage() {
		return rowsPerPage;
	}

	public String getClusteringKey() {
		return clusteringKey;
	}

	public String getClusteringKeyType() {
		return clusteringKeyType;
	}

	public Vector<Integer> getPages() {
		return pages;
	}

	public Vector<String []> getIndicies(){
		return indices;
	}

	public Vector<String> combination(Vector<String> arrSQLTermsColumns){
		
		for (int i = 0; i < arrSQLTermsColumns.size() - 2; i++) {
			String elementA = arrSQLTermsColumns.get(i);
			

		// // Loop for the second element
		 for (int j = i + 1; j < arrSQLTermsColumns.size() - 1; j++) {
			 String elementB = arrSQLTermsColumns.get(j);
		//
		     // Loop for the third element
		     for (int k = j + 1; k < arrSQLTermsColumns.size(); k++) {
		    	 String elementC = arrSQLTermsColumns.get(k);
		    	 String [] elements =new String[3];
		    	 elements[0]= elementA ;
		    	 elements[1]= elementB ;
		    	 elements[2]= elementC ;
		    		
		    	 if(foundIndex(elements)) {
		    		 Vector<String> result = new Vector<String>();
		    		 for(int b = getIndex(elements).length-1 ; i>=0 ; i--) {
		    			 result.add(getIndex(elements)[b]);
		    		 }
		    		 
		    		 return result;
		    	 }
		    	
		         // Print or process the combination
		    	
		     }
		 }
		}
		
		return null;

	}
}
