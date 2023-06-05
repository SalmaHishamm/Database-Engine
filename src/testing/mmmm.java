package testing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import main.DBApp;
import main.DBAppException;
import main.Octree;
import main.Page;
import main.SQLTerm;
import main.Table;
import utilities.Metadata;
import utilities.Serialization;

@SuppressWarnings("unused")
public class mmmm {
	
public static void main (String[]args) throws IOException, ParseException {

//Test delete with Index	
	
//-------------------------------------------------------------------------------------------------------------------------------------------------------------	
//	Vector<Hashtable<String, Object>> vector1 = new Vector<>();
//	Hashtable<String, Object> hashtable1 = new Hashtable<>();
//	hashtable1.put("key1", 1);
//	hashtable1.put("key2", 2);
//	vector1.add(hashtable1);
//	
//	Vector<Hashtable<String, Object>> vector2 = new Vector<>();
//	Hashtable<String, Object> hashtable2 = new Hashtable<>();
//	hashtable2.put("key3", 3);
//	hashtable2.put("key4", 4);
//	vector2.add(hashtable2);
//
//	vector1.addAll(vector2);
//
//	System.out.println(vector1); // Output: [a, b, c, d, e, f]

    
//------------------------------------------(Iterator&map.entry testing)-----------------------------------------------------------------------------------	
//
//	Vector<Hashtable<String, Integer>> vector = new Vector<>();
//	Hashtable<String, Integer> hashtable1 = new Hashtable<>();
//	hashtable1.put("key1", 1);
//	hashtable1.put("key2", 2);
//	vector.add(hashtable1);
//	Hashtable<String, Integer> hashtable2 = new Hashtable<>();
//	hashtable2.put("key3", 3);
//	hashtable2.put("key4", 4);
//	vector.add(hashtable2);
//
//	Iterator<Hashtable<String, Integer>> vectorIterator = vector.iterator();
//	while (vectorIterator.hasNext()) {
//	    Hashtable<String, Integer> hashtable = vectorIterator.next();
//        System.out.println(hashtable.entrySet());
//        System.out.println(hashtable.keySet());
//
//	    Iterator<Map.Entry<String, Integer>> hashtableIterator = hashtable.entrySet().iterator();
//	    while (hashtableIterator.hasNext()) {
//	        Map.Entry<String, Integer> entry = hashtableIterator.next();
//	        String key = entry.getKey();
//	        Integer value = entry.getValue();
//	        System.out.println(key + ": " + value);
//	    }
//	}
	
//------------------------------------------(code ended)-----------------------------------------------------------------------------------	

//	SQLTerm[] arrSQLTerms;
//	arrSQLTerms = new SQLTerm[2];
//	arrSQLTerms[0] = new SQLTerm();
//	arrSQLTerms[0]._strTableName = "Student";
//	arrSQLTerms[0]._strColumnName= "name";
//	arrSQLTerms[0]._strOperator = "="; 
//	arrSQLTerms[0]._objValue = "John Noor";
	
//	Vector<String> hello = new Vector<String>();
//	hello.add("Etsh");
//	hello.add("Sarah");
//	hello.add("HAbiba");

//	hello.add("logy");
	

	
//	//Serialize
//	String desktopPath = System.getProperty("user.home") + "/Desktop";
//	FileOutputStream fileOut = new FileOutputStream(desktopPath + "/"+ "Hello.class");
//	ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
//	objectOut.writeObject(hello);
//	objectOut.close();
//	
//	
//	try {
//		
//		//Deserialize
//		String desktopPath = System.getProperty("user.home") + "/Desktop";
//		FileInputStream filein = new FileInputStream(desktopPath + "/"+ "Hello.class");
//		ObjectInputStream objectin = new ObjectInputStream(filein);
//		Vector<String> hello1;
//		hello1 = (Vector<String>) objectin.readObject();
//		objectin.close();
//		
//		for(String h: hello1) {
//			System.out.println(h);
//		}
//		
//	} catch (Exception e) {
//		e.printStackTrace();
//	}
	
	
	
	
//------------------------------------------------(Testing Object Soring)----------------------------------------------------------------------------------------------	
	
/*	
	// Create a vector of hashtables
    Vector<Hashtable<String, Object>> vector = new Vector<>();

    String dataType = "java.util.Date";
    
    // Add some hashtables to the vector
    Hashtable<String, Object> hashtable1 = new Hashtable<>();
    hashtable1.put("key1", Metadata.parseData("java.util.Date", "2020-12-2"));
    hashtable1.put("key2", Metadata.parseData("java.util.Date","2020-10-2"));
    vector.add(hashtable1);

    Hashtable<String, Object> hashtable2 = new Hashtable<>();
    hashtable2.put("key1", Metadata.parseData("java.util.Date","2020-11-2"));
    hashtable2.put("key2", Metadata.parseData("java.util.Date","2021-1-2"));
    vector.add(hashtable2);

    Hashtable<String, Object> hashtable3 = new Hashtable<>();
    hashtable3.put("key1", Metadata.parseData("java.util.Date","2021-8-2"));
    hashtable3.put("key2", Metadata.parseData("java.util.Date","2020-9-2"));
    vector.add(hashtable3);

    // Create a comparator that compares the values of "key1"
    Comparator<Hashtable<String, Object>> comparator = new Comparator<Hashtable<String, Object>>() {
        @Override
        public int compare(Hashtable<String, Object> hashtable1, Hashtable<String, Object> hashtable2) {
            Object value1 = hashtable1.get("key2");
            Object value2 = hashtable2.get("key2");
            return Metadata.castToCompare("java.util.Date", value1, value2);
        }
    };

    // Sort the vector using the comparator
    Collections.sort(vector, comparator);

    // Print the sorted vector
    for (Hashtable<String, Object> hashtable : vector) {
        System.out.println(hashtable.toString());
    }
*/
	
//------------------------------------------(Java Reflection API [String -> Object])-----------------------------------------------------------------------------------	

//	try {
//	String strColType = "java.util.Date"; 
//	String strColValue = "1990-02-02"; 
//	Class<?> clazz = Class.forName( strColType );
//	    Constructor<?> constructor;
//	    try {
//	    	if (clazz == Date.class) {
//	    		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//	            Date date = dateFormat.parse(strColValue);
//	            long millis = date.getTime();
//	            constructor = clazz.getConstructor(long.class);
//	            Object instance = constructor.newInstance(millis);
//	            System.out.println(clazz.isInstance(instance));
//	    	}
//	    else {
//	    	if(clazz == String.class || clazz == Integer.class || clazz == Double.class) {
//	        constructor = clazz.getConstructor(String.class);
//	        Object instance = constructor.newInstance(strColValue);
//	        System.out.println(clazz.isInstance(instance));
//	    	}else
//	    		System.out.println(false);
//	    }
//	    } catch (NoSuchMethodException e) {
//	        System.out.println(false);
//	        
//	    }
//	}
//	catch(Exception e) {
//		e.printStackTrace();
//	}

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------	
	
//	try {
//		Hashtable<String, String> htblColNameType = new Hashtable<String, String>( ); 
//		htblColNameType.put("id", "java.lang.Integer"); 
//		htblColNameType.put("name", "java.lang.String"); 
//		htblColNameType.put("gpa", "java.lang.Double"); 
//		
//		Vector<String[]> metadata = Metadata.readMetadata("Student"); 
//		Hashtable<String,Object> columnNameType = new Hashtable<String,Object>();
//		
//		for(String [] metaDataSplit : metadata) {
//			columnNameType.put(metaDataSplit[1], metaDataSplit[2]);
//		}
//		
//		for (String key : htblColNameType.keySet()) {
//			if(!columnNameType.containsKey(key)) {
//				System.out.println("Column not found!");
//				break;
//			}
//			
//			if(!columnNameType.get(key).equals(htblColNameType.get(key))) {
//				System.out.println("Invalid column '"+key+"' data type");
//				break;
//			}
//			System.out.println("Valid " +key);
//		}
//		
//	} catch (Exception e) {
//		
//		e.printStackTrace();
//	}
	
//------------------------------------------------------------------(Create Table)---------------------------------------------------------------------------------------
	
//	DBApp dbApp = new DBApp(); 
//	Hashtable<String, String> htblColNameType = new Hashtable<String, String>( ); 
//	htblColNameType.put("id", "java.lang.Integer"); 
//	htblColNameType.put("name", "java.lang.String"); 
//	htblColNameType.put("gpa", "java.lang.Double"); 
//	htblColNameType.put("birthdate", "java.util.Date");
//	Hashtable<String, String> htblColNameMin = new Hashtable<String, String>( ); 
//	htblColNameMin.put("id", "0"); 
//	htblColNameMin.put("name", "A"); 
//	htblColNameMin.put("gpa", "0"); 
//	htblColNameMin.put("birthdate", "1990-01-01");
//	Hashtable<String, String> htblColNameMax = new Hashtable<String, String>( ); 
//	htblColNameMax.put("id", "10000"); 
//	htblColNameMax.put("name", "ZZZZZZZZZ"); 
//	htblColNameMax.put("gpa", "10000");
//	htblColNameMax.put("birthdate", "2024-01-01");
//	
//	try {
//		dbApp.createTable("Student", "id", htblColNameType, htblColNameMin, htblColNameMax);
//	} catch (DBAppException e) {
//		e.printStackTrace();
//	}
	
//------------------------------------------------------(Insert into Table)-------------------------------------------------------------------------------------------	
	
//	DBApp dbApp = new DBApp(); 
//	Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( ); 
//	htblColNameValue.put("id", new Integer(17)); 
//	htblColNameValue.put("name", new String("kilo")); 
//	htblColNameValue.put("gpa", new Double( 0.7)); 
//	String dateString = "1999-05-16";
//    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//    try {
//		Date date = dateFormat.parse(dateString);
//		htblColNameValue.put("birthdate", date); 
//	} catch (ParseException e1) {
//		e1.printStackTrace();
//	}
//    
//    
//	try {
//		dbApp.insertIntoTable("Student", htblColNameValue);
//	} catch (DBAppException e) {
//		e.printStackTrace();
//	}
	
//-------------------------------------------------------(Update In Table)--------------------------------------------------------------------
	
//	DBApp dbApp = new DBApp(); 
//	Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( ); 
//	htblColNameValue.put("name", new String("hey")); 
//	htblColNameValue.put("gpa", new Double( 2.0 )); 
//	try {
//		dbApp.updateTable("Student", "13", htblColNameValue);
//	} catch (DBAppException e) {
//		
//		e.printStackTrace();
//	}
	
//-------------------------------------------------------(Delete From Table)--------------------------------------------------------------------
	
//	DBApp dbApp = new DBApp(); 
//	Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( ); 
//	htblColNameValue.put("name", new String("hey")); 
//	htblColNameValue.put("gpa", new Double( 2.0 ));
//	htblColNameValue.put("id", new Integer(13));
//	try {
//		dbApp.deleteFromTable("Student", htblColNameValue);
//	} catch (DBAppException e) {
//		
//		e.printStackTrace();
//	}
	
//-----------------------------------------------------------------(Select From Table)-------------------------------------------------------

	DBApp dbApp = new DBApp(); 
	SQLTerm[] arrSQLTerms; 
	arrSQLTerms = new SQLTerm[3]; 
	arrSQLTerms[0] = new SQLTerm();
	arrSQLTerms[0]._strTableName = "Student";
	arrSQLTerms[0]._strColumnName= "name"; 
	arrSQLTerms[0]._strOperator = "="; 
	arrSQLTerms[0]._objValue = "hana";
	
	arrSQLTerms[1] = new SQLTerm();
	arrSQLTerms[1]._strTableName = "Student"; 
	arrSQLTerms[1]._strColumnName= "gpa"; 
	arrSQLTerms[1]._strOperator = "="; 
	arrSQLTerms[1]._objValue = new Double( 1 ); 
	
	arrSQLTerms[2] = new SQLTerm();
	arrSQLTerms[2]._strTableName = "Student"; 
	arrSQLTerms[2]._strColumnName= "birthdate"; 
	arrSQLTerms[2]._strOperator = "="; 
	String dateString = "1999-05-16";
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	Date date = dateFormat.parse(dateString);
	arrSQLTerms[2]._objValue = date;
	
//	arrSQLTerms[3] = new SQLTerm();
//	arrSQLTerms[3]._strTableName = "Student"; 
//	arrSQLTerms[3]._strColumnName= "id"; 
//	arrSQLTerms[3]._strOperator = "="; 
//	arrSQLTerms[3]._objValue = new Integer( 1); 
	
	String[]strarrOperators = new String[2];
	strarrOperators[0] = "AND"; 
	strarrOperators[1] = "AND"; 
//	strarrOperators[2] = "AND";
	
	try {
		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		
		while(resultSet.hasNext()) {
			System.out.print(resultSet.next() + " ");
		}
		
		System.out.println();
	} catch (DBAppException e) {
		e.printStackTrace();
	}	
	
//------------------------------------------------------(Loading & Reading Table&Page outputs)---------------------------------------------------------------------------
	
	try {
		
		Table temp;
		temp = Serialization.loadTable("Student");
		//System.out.println(temp.getRowsPerPage());
//		for(int i=0; i<temp.getPages().size();i++) {
//			//System.out.println("Table: " + temp.getPages().get(i) + ". ");
//			//System.out.println(temp.getPages().get(i).getRecords());
//			//System.out.println(Serialization.loadPage("Student",temp.getPages().get(temp.getPages().size()-1).getId()).getId());
//			//System.out.println(temp.getPages().get(i).getMinClusterValue() +" " +temp.getPages().get(i).getMaxClusterValue());
//		}
		
		for(int i=0; i<temp.getPages().size();i++) {
			Page page = Serialization.loadPage("Student", temp.getPages().get(i));
			System.out.print("Page: " + page.getId() + ". ");
			System.out.println(page.getRecords());
		}
		
		System.out.println();
		
	} catch (DBAppException e) {
		e.printStackTrace();
	}

//------------------------------------------------------(Create Octree Index)---------------------------------------------------------------------------	
	
//	DBApp dbApp = new DBApp(); 
//	
//	String[] strarrColName = new String[3];
//	strarrColName[0] = "birthdate";
//	strarrColName[1] = "name";
//	strarrColName[2] = "gpa";
//	
//	try {
//		dbApp.createIndex("Student", strarrColName);
//	} catch (DBAppException e) {
//		e.printStackTrace();
//	}
	
//	//-----------------------------------------------(testing Octree)-------------------------------------------------------------------------	
	
		try {
			Octree octree = Serialization.loadIndex("Student", "birthdatenamegpaindex");
//			System.out.println(octree.searchUpdate(new Integer(25), 'x'));
//			octree.update(new Integer(60), new String("j"), new Double(4.0), "id", "Student", 1);
			//octree.remove(new Integer(13), new String("hey"), new Double(2.0));
			System.out.println(octree.toString());
			
			//Serialization.writeIndex(octree);
			
		} catch (DBAppException e1) {
			e1.printStackTrace();
		}		
	
	
	
//---------------------------------------------------------------------------------------------------------------------------------------------------------------------	
//	String desktopPath = System.getProperty("user.home") + "/Desktop";
//    String filename = "metadata.csv";
//    FileWriter writer = new FileWriter(desktopPath + "/" + filename);
    
//	FileWriter file = new FileWriter("metadata.csv");
	
//	//read to csv file
//	FileReader file = new FileReader("metadata.csv");
//	BufferedReader br = new BufferedReader((file));
//	System.out.println(br.readLine());
	
	
	
	//write to csv file
	/*
			try {
				FileWriter file = new FileWriter("/Users/loginelsalnty/Desktop/Semester 6/Database II/Project/metadata.csv");
				BufferedWriter br = new BufferedWriter((file));
				br.write("Hi");
					//System.out.println(br.writeLine());
					br.close();
				} catch (Exception e) {
				
					System.out.println(e.getMessage());
					e.printStackTrace();
				} */
//	br.close();
//	
//	Hashtable<String,String> tr = new Hashtable<String, String>();
//	tr.put("1", "Etsh");
//	tr.put("2", "lojy elmo2refa");
//	tr.put("3", "Etsh");
//	tr.remove("3");
//	String s =tr.toString();
//	System.out.print(s);
	
	
	
			
}
}
