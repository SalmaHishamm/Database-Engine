package main;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import utilities.Metadata;
import utilities.Serialization;

public class Page implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String tableName; // or private final String tableName;
	private final Vector<Hashtable<String,Object>> records;
	private final int maxTuples;
    private int size;
    private final String clusteringKey;
    private final String clusteringKeyType;
	private Object minClusterValue;
	private Object maxClusterValue;
	private final int id;
	
	public Page(String table, int maxTuples,
			String clusteringKeyColumn, String clusteringKeyType, int id) {
		
		super();
		this.tableName = table;
		records = new Vector<Hashtable<String,Object>>();
		this.maxTuples = maxTuples;
		size = 0;
		this.clusteringKey = clusteringKeyColumn;
		this.clusteringKeyType = clusteringKeyType;
		this.id = id;
		size=0;
		
		Serialization.writePage(this);
		
	}

	public boolean insert(Hashtable<String,Object> htblColNameValue) {
		
		//insert Entry/Row
		records.add(htblColNameValue);
		size++;
		
		//sort Page: call this.sort()
		sort();
		
		
		//assign minClusterValue and maxClusterValue
		minClusterValue = records.get(0).get(clusteringKey);
		maxClusterValue = records.lastElement().get(clusteringKey);
		
		//write page to .class file
		Serialization.writePage(this);
		
		return true;
		
	}
	
	public void sort() {
		//1: Create a comparator that compares the values of clusteringKey
        Comparator<Hashtable<String, Object>> comparator = new Comparator<Hashtable<String, Object>>() {
            @Override
            public int compare(Hashtable<String, Object> hashtable1, Hashtable<String, Object> hashtable2) {
                Object value1 = hashtable1.get(clusteringKey);
                Object value2 = hashtable2.get(clusteringKey);
                return Metadata.castToCompare(clusteringKeyType, value1, value2);
            }
        };
		
        //2: Sort the vector using the comparator
        Collections.sort(records, comparator);
	}
	
	public boolean update(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) {
		
		//binarySearch over records to find the row to update
		Hashtable<String, Object> row = binarySearch(Metadata.parseData(clusteringKeyType, strClusteringKeyValue));
		
		if(row !=null)	{
			
			//primary key value to update is found
			
				//if(row.get(clusteringKey).equals(Metadata.parseData(clusteringKeyType, strClusteringKeyValue))) {
				
				for(String column: htblColNameValue.keySet()) {
					row.replace(column, htblColNameValue.get(column));
				}
				
				//write page to .class file
				Serialization.writePage(this);
				
				return true;
				
				//}
			
		}

		//primary key value to update is NOT found
		return false;
		
	}
	
	public void delete(Hashtable<String, Object> htblColNameValue) {
		
		if(htblColNameValue.keySet().contains(clusteringKey)) {
			
			//binarySearch over records to find the row to delete
			Hashtable<String, Object> row = binarySearch(htblColNameValue.get(clusteringKey));
			
			if(row != null && toDelete(row,htblColNameValue)) {
				records.remove(row);
				size--;
			}
			
			
		}else {
			for(int i =0; i<records.size(); i++) {
				
				Hashtable<String, Object> currentRow = records.get(i);
				
				if(toDelete(currentRow,htblColNameValue)) {
					records.remove(currentRow);
					size--;
					i--;
				}
			}
		}
		
		//sort Page: call this.sort()
		//sort();
		
		//assign minClusterValue and maxClusterValue
		if(!isEmpty()) {
			minClusterValue = records.get(0).get(clusteringKey);
			maxClusterValue = records.lastElement().get(clusteringKey);
		}
		
		//write page to .class file
		Serialization.writePage(this);
	}
	
	public boolean toDelete(Hashtable<String, Object> currentRow, Hashtable<String, Object> htblColNameValue){
		
		for(String column : htblColNameValue.keySet()) {
			if(!currentRow.get(column).equals(htblColNameValue.get(column))) {
				return false;
			}
		}
		return true;
	}
	
	public Hashtable<String,Object> binarySearch(Object searchKey) {
	  
	    
	    // Calculate the starting and ending indices of the page
	    int start = 0;
	    int end = records.size() - 1;
	    int mid;
	    
	    // Perform the binary search
	    while (start <= end) {
	        // Calculate the midpoint index
	        mid = start + (end - start) / 2;
	        
	        // Compare the search key to the key at the midpoint index
	        Object toCompare = records.get(mid).get(clusteringKey);
	        int cmp = Metadata.compareObjects(searchKey,toCompare);
	        
	        if (cmp == 0) {
	            // Found the key, return the record
	            return records.get(mid);
	        } else if (cmp < 0) {
	            // Search the left half of the range
	            end = mid - 1;
	        } else {
	            // Search the right half of the range
	            start = mid + 1;
	        }
	    }
	    
	    // Key not found
	    return null;
	}
	
	public boolean isFull() {
		return records.size() >= maxTuples;
	}

	public boolean isEmpty() {
		return records.size() == 0;
	}

	public Vector<Hashtable<String, Object>> getRecords() {
		return records;
	}

	public int getMaxTuples() {
		return maxTuples;
	}

	public int getSize() {
		return size;
	}

	public String getClusteringKey() {
		return clusteringKey;
	}

	public Object getMinClusterValue() {
		return minClusterValue;
	}

	public Object getMaxClusterValue() {
		return maxClusterValue;
	}

	public int getId() {
		return id;
	}

	public String getTableName() {
		return tableName;
	}
	
	public Vector<Hashtable<String,Object>> select(SQLTerm term) throws DBAppException {
		Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
		Iterator<Hashtable<String, Object>> vectorIterator = records.iterator();
		
		
		if(clusteringKey.equals(term._strColumnName) && term._strOperator.equals("=") && binarySearch(term._objValue) != null) {
			//binarySearch
			
			result.add(binarySearch(term._objValue));
			
		}
		else {
			while (vectorIterator.hasNext()) {
			    Hashtable<String, Object> currentRow = vectorIterator.next();
			    Iterator<Map.Entry<String, Object>> hashtableIterator = currentRow.entrySet().iterator();
			    
			    while (hashtableIterator.hasNext()) {
			        Map.Entry<String, Object> entry = hashtableIterator.next();
			        
			        if(term._strColumnName.equals(entry.getKey())) {
			        	
			        	//System.out.println(term._strOperator + " " +term._objValue );
			        	
				        if(checkOperator(term._strOperator,entry.getValue(),term._objValue)) {
				        	
				        	result.add(currentRow);
				        }
			        }
			        
			      
			    }
			}
		}
	return result;
	
}
	
	public Vector<Hashtable<String,Object>> selectIndexExact(Hashtable<String,Hashtable<String,Object>> data) throws DBAppException {
		Vector<Hashtable<String,Object>> result = new Vector<Hashtable<String,Object>>();
		Iterator<Hashtable<String, Object>> vectorIterator = records.iterator();
		
		
			if(data.keySet().contains(clusteringKey)&&  binarySearch(data.get(clusteringKey)) != null) {
				//binarySearch
				result.add(binarySearch(data.get(clusteringKey)));
				
				
			}
			else {
				while (vectorIterator.hasNext()) {
				    Hashtable<String, Object> currentRow = vectorIterator.next();
				    Iterator<Map.Entry<String, Object>> hashtableIterator = currentRow.entrySet().iterator();
				    
				    int conditionsSatisfied = 0;
				    
				    while (hashtableIterator.hasNext()) {
				        Map.Entry<String, Object> entry = hashtableIterator.next();
				        
				        for(String column: data.keySet()) {
				        	if(column.equals(entry.getKey())) {
					        	
						        if(checkOperator((String)(data.get(column).get("operator")),entry.getValue(),data.get(column).get("value"))) {
						        	conditionsSatisfied++;
						        }
					        }
				        }
				        
				        if(conditionsSatisfied == 3) {
				        	result.add(currentRow);
				        }
				    }
				}
			}
		
	return result;
	
}
	
	public static boolean checkOperator(String operator,Object value,Object entryValue) throws DBAppException {
		
		//System.out.println(entryValue + " " +operator + " "+ value);
		
		switch(operator) {
		case "=": return entryValue.equals(value);
		case ">": return Metadata.compareObjects(value, entryValue)>0?  true:false; 
		case ">=":return Metadata.compareObjects(value, entryValue)>=0?  true:false;
		case "<=":return Metadata.compareObjects(value, entryValue)<=0?  true:false;
		case "<": return Metadata.compareObjects(value, entryValue)<0?  true:false;
		case "!=":return !entryValue.equals(value);
		default: throw new DBAppException("Invalid operator");
		}
		
	}

	
}
