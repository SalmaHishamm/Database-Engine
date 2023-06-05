package utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
//import java.lang.reflect.InvocationTargetException;
//import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import main.DBAppException;

public class Metadata{
	
	private String tableName;
	private Hashtable<String,Hashtable<String,Object>> columnDefinition; // key is columnName and value is a hashtable with   
															   			 //keySet = ["type", "isClusteringKey", "indexName", "indexType", "min", "max"] 
															   			 //all correspond to values of Object
	
	public Metadata(String tableName, Hashtable<String, Hashtable<String,Object>> columnDefinition) {
		super();
		this.tableName = tableName;
		this.columnDefinition = columnDefinition;
	}
	
	public static Object parseData(String strType, String strData) {
		
		//change strData to Object depending on the input data type strType
		
//		switch (strType) {
//        case "java.lang.Integer":
//            return Integer.parseInt(strData);
//        case "java.lang.Long":
//            return Long.parseLong(strData);
//        case "java.lang.Double":
//            return Double.parseDouble(strData);
//        case "java.lang.Boolean":
//            return Boolean.parseBoolean(strData.toLowerCase());
//        case "java.lang.String":
//        case "java.util.Date":
//            return strData;
//		}
		
		//OR use Java reflection API
		
		Class<?> clazz = null;
	    try {
	        clazz = Class.forName(strType);
	    } catch (ClassNotFoundException e) {
	        return null;
	    }
	    
	    Constructor<?> constructor;
		try {
			
			if (clazz == Date.class) {
	        	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	            Date date = dateFormat.parse(strData);
	            long millis = date.getTime();
	            constructor = clazz.getConstructor(long.class);
	            Object instance = constructor.newInstance(millis);
	            return instance;
	        } else {
	        	if(clazz == String.class || clazz == Integer.class || clazz == Double.class || clazz == Boolean.class) {
					constructor = clazz.getConstructor(String.class);
					Object instance = constructor.newInstance(strData);
					return instance;
	        	}else {
	        		throw new DBAppException("Invalid data type: " + strType);
	        	}
	        }
		} catch (Exception e){
			return null;
		}
        
		
	}
	
	public static String stringData(String strType, Object objData) {
		
		//change strData to Object depending on the input data type strType
		try {
			switch (strType) {
				case "java.lang.Integer":
				    return Integer.toString((Integer) objData);
				case "java.lang.Double":
				    return Double.toString((Double) objData);
				case "java.lang.Boolean":
				    return Boolean.toString((Boolean) objData);
				case "java.lang.String":
					return (String) objData;
				case "java.util.Date":
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		            Date date = (Date) objData;
		            return formatter.format(date);
			}
		}catch(Exception e) {
			System.out.println("Column type: '"+ strType+"' does not match column input: '"+objData.toString()+"'");
		}
		
		return null;
	}
	
	public void writeMetadata() throws DBAppException { //maybe not static
		
		//String desktopPath = System.getProperty("user.home") + "/Desktop";
		String desktopPath ="resources";
		String filename = "metadata.csv";
		
		try {
		BufferedWriter writer = new BufferedWriter(new FileWriter(desktopPath + "/" + filename,true));
        
		String strMetadata = "";
		
		for(String column: columnDefinition.keySet()) {
			Hashtable<String,Object> definition = columnDefinition.get(column);
			
			if(!definition.get("type").equals("java.util.Date")){
				strMetadata += tableName +", "+ column +", "+definition.get("type") +", "+ definition.get("isClusteringKey") +", "+ 
								definition.get("indexName")+", "+ definition.get("indexType")+", "+ definition.get("min")+", "+ definition.get("max") + "\n";
			}else {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				strMetadata += tableName +", "+ column +", "+definition.get("type") +", "+ definition.get("isClusteringKey") +", "+ 
						definition.get("indexName")+", "+ definition.get("indexType")+", "+ dateFormat.format(definition.get("min"))+", "+ dateFormat.format(definition.get("max")) + "\n";
			}
		}
		
		writer.write(strMetadata);
		writer.close();
		}
		catch(Exception e) {
			throw new DBAppException(e.getLocalizedMessage());
		}
		
	}
	
	public static Hashtable<String, Hashtable<String, Object>> readMetadata(String tableName) {
		
		//String desktopPath = System.getProperty("user.home") + "/Desktop";
		String desktopPath ="resources";
		String filename = "metadata.csv";
		try {
			BufferedReader reader = new BufferedReader(new FileReader(desktopPath + "/" + filename));
			Hashtable<String,Hashtable<String,Object>> columnDefinition = new Hashtable<String,Hashtable<String,Object>>();
			String metadataString;
			String[] metaDataSplit;
			
			//extracting strings from metadata file
			while ((metadataString = reader.readLine()) != null) {
				metadataString = metadataString.replaceAll(" ", "");
				metaDataSplit = metadataString.split(",");
				
				//tableName = metaDataSplit[0], columnName = metaDataSplit[1], type= metaDataSplit[2], isClusteringKey = metaDataSplit[3], 
				//indexName = metaDataSplit[4], indexType = metaDataSplit[5], min = metaDataSplit[6], max = metaDataSplit[7]
				if(tableName.equals(metaDataSplit[0])) {
					Hashtable<String,Object> definitions = new Hashtable<String,Object>();
					
					definitions.put("type", metaDataSplit[2] );
					definitions.put("isClusteringKey", parseData("java.lang.Boolean",metaDataSplit[3]));
					definitions.put("indexName", metaDataSplit[4]); // default for now
					definitions.put("indexType", metaDataSplit[5]); // default for now
					definitions.put("min", parseData(metaDataSplit[2], metaDataSplit[6]));
					definitions.put("max", parseData(metaDataSplit[2], metaDataSplit[7]));
					
					columnDefinition.put(metaDataSplit[1], definitions);
				}
	            
			}
			reader.close();
			if(columnDefinition.isEmpty()) {
				return null;
			}
			return columnDefinition;
		} catch (IOException e ) {
			//e.printStackTrace();
			return null;
		}
		

		
	}

	public static void rewriteMetadata(String tableName,String[] strarrColName, String octreeName) {
		
		//String desktopPath = System.getProperty("user.home") + "/Desktop";
		String desktopPath ="resources";
		String filename = "metadata.csv";
		
		Vector<String> vecColName = new Vector<String>();
		for(String str : strarrColName) {
			vecColName.add(str);
		}
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(desktopPath + "/" + filename));
			
			File tempFile = new File(desktopPath + "/" +"temp.csv");
		    BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
		    
			String metadataString;
			String[] metaDataSplit;
			
			//extracting strings from metadata file
			while ((metadataString = reader.readLine()) != null) {
				metadataString = metadataString.replaceAll(" ", "");
				metaDataSplit = metadataString.split(",");
				
				//tableName = metaDataSplit[0], columnName = metaDataSplit[1], type= metaDataSplit[2], isClusteringKey = metaDataSplit[3], 
				//indexName = metaDataSplit[4], indexType = metaDataSplit[5], min = metaDataSplit[6], max = metaDataSplit[7]
				if(tableName.equals(metaDataSplit[0]) ) {
						if(vecColName.contains(metaDataSplit[1])) {
							
							String updatedString = metaDataSplit[0] +", "+ metaDataSplit[1] +", "+ metaDataSplit[2] +", "+ metaDataSplit[3] +", "+ octreeName
									+", "+ "Octree" +", "+ metaDataSplit[6] +", "+ metaDataSplit[7] + "\n";
							writer.write(updatedString);
						}
						else {
							writer.write(metadataString + "\n");
						}
				}
				else {
					writer.write(metadataString + "\n");
				}
				
					
			}
	            
			reader.close();
			writer.close();
			
			// Delete the original CSV file
		    File originalFile = new File(desktopPath + "/" + filename);
		    originalFile.delete();

		    // Rename the temporary file to the original filename
		    tempFile.renameTo(new File(desktopPath + "/" + filename));
		    
		} catch (IOException e ) {
			e.printStackTrace();
//			return null;
		}
		

		
	}
	
	public String getTableName() {
		return tableName;
	}

	public Hashtable<String, Hashtable<String, Object>> getColumnDefinition() {
		return columnDefinition;
	}
	
	public static int castToCompare(String dataType, Object obj1, Object obj2) { //-ve if obj1 < obj2
		
		switch (dataType) {
		case "java.lang.Integer":
			Integer valueInt = (Integer) obj1;
			return valueInt.compareTo((Integer) obj2);
		case "java.lang.Double":
			Double valueDouble = (Double) obj1;
			return valueDouble.compareTo((Double) obj2);
		case "java.lang.Boolean":
			Boolean valueBool = (Boolean) obj1;
			return valueBool.compareTo((Boolean) obj2); 
		case "java.lang.String":
			String valueStr = (String) obj1;
			return valueStr.compareTo((String) obj2);
		case "java.util.Date":
			Date value = (Date) obj1;
			return value.compareTo((Date) obj2);
            
		}
		
		return 0;
	}
	
	public static boolean isValidDataType(String strColType,String strColValue) {
		Class<?> clazz = null;
	    try {
	        clazz = Class.forName(strColType);
	    } catch (ClassNotFoundException e) {
	        return false;
	    }
	    
	    Constructor<?> constructor;
	    try {
	        if (clazz == Date.class) {
	        	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	            Date date = dateFormat.parse(strColValue);
	            long millis = date.getTime();
	            constructor = clazz.getConstructor(long.class);
	            Object instance = constructor.newInstance(millis);
	            return clazz.isInstance(instance);
	        } else {
	        	if(clazz == String.class || clazz == Integer.class || clazz == Double.class) {
		            constructor = clazz.getConstructor(String.class);
		            Object instance = constructor.newInstance(strColValue);
		            return clazz.isInstance(instance);
	        	}else
	        		return false;
	        }
	    } catch (Exception e) {
	        return false;
	    }
		
	}

	@SuppressWarnings("unchecked")
	public static int compareObjects(Object a, Object b) {
	    if (a == null && b == null) {
	        return 0;
	    } else if (a == null) {
	        return -1;
	    } else if (b == null) {
	        return 1;
	    }
	    if (a instanceof Comparable && b.getClass().equals(a.getClass())) {
	        return ((Comparable<Object>) a).compareTo(b);
	    } else {
	        return a.toString().compareTo(b.toString());
	    }
	}
	
	public static boolean indexExists(String strTableName,String colName) throws DBAppException {
		Hashtable<String, Hashtable<String, Object>> metadata= readMetadata(strTableName);
		if(metadata==null) {
			throw new DBAppException("Table does not exist");
		}
		if(metadata.get(colName).get("indexType").equals("Octree")) {
			return true;
		}
		return false;
	}
	
}
