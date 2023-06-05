package main;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Vector;

import utilities.Metadata;
import utilities.Serialization;

public class DBApp {
	 
	private Vector<Table> tables;
	
	public void init( ){
	}
	
	public void createTable ( String strTableName ,String strClusteringColumn , 
							Hashtable<String,String> htblColNameType, 
				            Hashtable<String,String> htblColNameMin, 
				            Hashtable<String,String> htblColNameMax )
				            throws DBAppException {

		//do appropriate checks (check if table already exists...etc)
			//1: check that type, min and max hashtables are all the same size
					if(htblColNameType.size() != htblColNameMin.size() && htblColNameType.size() != htblColNameMax.size()) {
						throw new DBAppException("Column type, min and max sizes are not the same.");
					}	
					
			//2: check if table already exists
					if(tableExists(strTableName)) {
						throw new DBAppException("Table already exists.");
					}		
					
		//create a new instance of table
		if(tables == null) {
			tables = new Vector<Table>();
		}
		
		//check if table already exists
//		for(Table table : tables) {
//			if(table.getName().equals(strTableName)) {
//				throw new DBAppException("Table already exists");
//			}
//		}
		
		Table table = new Table(strTableName ,strClusteringColumn , htblColNameType, htblColNameMin, htblColNameMax);
		
		//add table to vector
		tables.add(table);
		
		//write table in a .class file
		Serialization.writeTable(table);
		
		//run java garbage collector
		System.gc();
	
	}
	
	public void insertIntoTable(String strTableName,
            Hashtable<String,Object> htblColNameValue) 
            throws DBAppException {
		
		//check if table does not exist
				if(!tableExists(strTableName)) {
					throw new DBAppException("Table does not exist.");
				}
		
		//loadTable with name = strTableName
		Table table = Serialization.loadTable(strTableName);
		
		//do appropriate checks (check if table doesn't exist (i.e ==null)...etc)
			//1: check if htblColNameValue has a key of clusteringColumn
			//2: check if clusteringKey is unique by calling search(clusteringColumn, Object clusteringValue) in class Table
			
		
		//call insert in class Table
		table.insert(htblColNameValue);
		
		//write table to .class file
		Serialization.writeTable(table);
		
		//run java garbage collector
		System.gc();
		
	}

	public void updateTable(String strTableName, 
			 String strClusteringKeyValue, 
			Hashtable<String,Object> htblColNameValue ) 
			throws DBAppException {
		
		//check if table does not exist
				if(!tableExists(strTableName)) {
					throw new DBAppException("Table does not exist.");
				}
		
		//loadTable with name = strTableName
		Table table = Serialization.loadTable(strTableName);
		
		//call update in class Table
		table.update(strClusteringKeyValue,htblColNameValue);
		
		//write table to .class file
		Serialization.writeTable(table);
		
		//run java garbage collector
		System.gc();
		
	}

	public void deleteFromTable(String strTableName, 
			 Hashtable<String,Object> htblColNameValue) 
			 throws DBAppException{
		
		//check if table does not exist
				if(!tableExists(strTableName)) {
					throw new DBAppException("Table does not exist.");
				}
		
		//loadTable with name = strTableName
		Table table = Serialization.loadTable(strTableName);
		
		//call delete in class Table
		table.delete(htblColNameValue);
		
		//write table to .class file
		Serialization.writeTable(table);
		
		//run java garbage collector
		System.gc();
		
	}
	
	public void createIndex(String strTableName,
			String[] strarrColName) throws DBAppException{
		if(!tableExists(strTableName)) {
			throw new DBAppException("Table doesn't exist .");
		}		
		
		
		if ( strarrColName.length !=3 ) {
			throw new DBAppException("Can not create index on those columns ");
		}
		Table table = Serialization.loadTable(strTableName);
		try {
			table.createIndex(strarrColName);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	    
		//write table to .class file
		Serialization.writeTable(table);
		
	}

	public static boolean tableExists(String strTableName) {
		if(Metadata.readMetadata(strTableName)==null) {
			return false;
		}
		return true;
	}
	
	public Iterator<?> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException{
		
		//do appropriate checks:
			//starrOperators.length = arrSQLTerms.length -1
			//arrSQLTerms[]._columnName is valid
		
		if(strarrOperators.length != arrSQLTerms.length-1) {
			throw new DBAppException("Logical operators (strarrOperators) have invalid size");
		}
		
				//check if index is applied on columns if yes
				// -> select with index
				//else
				//CHECK: -> select without index binarySearch if clusteringKey and linearly for others
		
		boolean isIndexed = false;
		
		Vector<Vector<Hashtable<String, Object>>> vectorOfQueries = new Vector<Vector<Hashtable<String, Object>>>();
			
			int isAnd = 0;
			
			for(int i =0; i<strarrOperators.length; i++) {
				if(strarrOperators[i].equals("AND")) {
					isAnd++;
				}
				
			}
			
			if(arrSQLTerms.length ==3 && isAnd ==arrSQLTerms.length-1) {
				
				Table table = Serialization.loadTable(arrSQLTerms[0]._strTableName);
				
//				Vector<String> arrSQLTermsColumns = new Vector<String>();
//				for(int i =0; i<arrSQLTerms.length; i++) {
//					arrSQLTermsColumns.add(arrSQLTerms[i]._strColumnName);
//				}
//				
//				if(table.combination(arrSQLTermsColumns)!= null) {
//					
////					Vector<String> columnsIndex = table.combination(arrSQLTermsColumns);
////					SQLTerm[] arrSQLTermsIndex = new SQLTerm[3];
////					int index =0;
////					for(int i =0; i<arrSQLTerms.length;i++) {
////						if(columnsIndex.contains(arrSQLTerms[i]._strColumnName)) {
////							arrSQLTermsIndex[index] =arrSQLTerms[i] ;
////							index++;
////						}
////					}
////					
////					for(int i=0;i<arrSQLTermsIndex.length;i++) {
////						System.out.println(arrSQLTermsIndex[i]._strColumnName);
////					}
					
					vectorOfQueries.add(table.selectIndex(arrSQLTerms));
					isIndexed = true;
					
					
//					for(int i =0;i<columnsIndex.size();i++) {
//						arrSQLTermsColumns.remove(columnsIndex.get(i));
//					}
//					
//					Vector<SQLTerm> vecSQLTerms = new Vector<SQLTerm>();
//					
//					for(int i = 0; i<arrSQLTerms.length ; i++) {
//						if(arrSQLTermsColumns.contains(arrSQLTerms[i]._strColumnName)) {
//							SQLTerm term = arrSQLTerms[i];
//							vecSQLTerms.add(term);
//						}
//					}
//					
//					while(vectorOfQueries.size() != 1) {
//						
//						vectorOfQueries.add(0, AND(vectorOfQueries.get(0), vecSQLTerms.get(0)));
//						vectorOfQueries.remove(2); vectorOfQueries.remove(1);
//						vecSQLTerms.remove(0);
//					}
//					
//					if(vectorOfQueries.get(0) == null) {
//						throw new DBAppException("No entry satisfies the conditions.");
//					}
//			
//					return vectorOfQueries.get(0).iterator();
				}
			
			if(!isIndexed) {
				for(int i =0; i<arrSQLTerms.length; i++){
					Table table = Serialization.loadTable(arrSQLTerms[i]._strTableName);
					vectorOfQueries.add(table.select(arrSQLTerms[i])); //Iterator = 
				}
			}
				//check for null - table.select()
		
		if(!isIndexed) {
			Vector<SQLTerm> vecSQLTerms = new Vector<SQLTerm>();
			for(int i = 1; i<arrSQLTerms.length ; i++) {
				SQLTerm term = arrSQLTerms[i];
				vecSQLTerms.add(term);
			}
			
			while(vectorOfQueries.size() != 1) {
				
				for(String operator : strarrOperators) {
					
					if(operator.equals("OR")){
						
						vectorOfQueries.add(0, OR(vectorOfQueries.get(0),vectorOfQueries.get(1)));
						vectorOfQueries.remove(2); vectorOfQueries.remove(1);
						vecSQLTerms.remove(0);
						
					}
					else if(operator.equals("AND")){
	
						vectorOfQueries.add(0, AND(vectorOfQueries.get(0), vecSQLTerms.get(0)));
						vectorOfQueries.remove(2); vectorOfQueries.remove(1);
						vecSQLTerms.remove(0);
					}
					else if(operator.equals("XOR")){
						
						vectorOfQueries.add(0, XOR(vectorOfQueries.get(0),vectorOfQueries.get(1), vecSQLTerms.get(0)));
						vectorOfQueries.remove(2); vectorOfQueries.remove(1);
						vecSQLTerms.remove(0);
					}
					else {
						throw new DBAppException("Invalid logical operator");
					}
					
				}
				
				
				
			}
		}		
				// TODO sort before returning
				
				if(vectorOfQueries.get(0) == null) {
					throw new DBAppException("No entry satisfies the conditions.");
				}
		
				return vectorOfQueries.get(0).iterator();
	}
	

	
	public Vector<Hashtable<String, Object>> OR(Vector<Hashtable<String, Object>> firstQuery, Vector<Hashtable<String, Object>> secondQuery) {
		
		// use Set to remove duplicates
		Set<Hashtable<String, Object>> set = new LinkedHashSet<>(firstQuery);
		set.addAll(secondQuery);
		Vector<Hashtable<String, Object>> concatenatedVector = new Vector<>(set);
		
		return concatenatedVector;
		
	}
	
	public Vector<Hashtable<String, Object>> AND(Vector<Hashtable<String, Object>> firstQuery, SQLTerm term) throws DBAppException{
		
		for(int i =0; i<firstQuery.size(); i++) {
			
			if(!Page.checkOperator(term._strOperator, firstQuery.get(i).get(term._strColumnName) , term._objValue)) {
				firstQuery.remove(i);
				i--;
			}
			
		}
		
		return firstQuery;
		
	}
	
	public Vector<Hashtable<String, Object>> XOR(Vector<Hashtable<String, Object>> firstQuery, Vector<Hashtable<String, Object>> secondQuery, SQLTerm term) throws DBAppException {
		
		Vector<Hashtable<String, Object>> ORed = OR(firstQuery, secondQuery);
		
		Vector<Hashtable<String, Object>> ANDed = AND(firstQuery, term);
		
		for(Hashtable<String, Object> currentAndRow : ANDed) {
			if(ORed.contains(currentAndRow)) {
				ORed.remove(currentAndRow);
			}
		}
		
		return ORed;
		
	}
	
	
	
}
