package utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import main.DBAppException;
import main.Octree;
import main.Page;
import main.Table;

public class Serialization {

	
//	-----------------------------------------------------( Write & Read table )----------------------------------------------------------------
		
	
	public static boolean writeTable(Table table) {  //Serialize
		try {
			//String desktopPath = System.getProperty("user.home") + "/Desktop";
			String desktopPath ="resources/data";
			FileOutputStream fileOut = new FileOutputStream(desktopPath + "/" + table.getName() + ".class");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(table);
			objectOut.close();
			return true; 

		} catch (Exception ex) {
			return false;
		}
	}
	
	public static Table loadTable(String strTableName) throws DBAppException { //Deserialize
		try {
			//String desktopPath = System.getProperty("user.home") + "/Desktop";
			String desktopPath ="resources/data";
			FileInputStream filein = new FileInputStream(desktopPath + "/" + strTableName + ".class");
			ObjectInputStream objectin = new ObjectInputStream(filein);
			Table table = (Table) objectin.readObject();
			objectin.close();
			return table;

		} catch (Exception ex) {
			//ex.printStackTrace();
			throw new DBAppException("Table not found.\n	" + ex.getLocalizedMessage());
		}
		
	}

//	-----------------------------------------------------( Write & Read page )----------------------------------------------------------------
		
	
	public static boolean writePage(Page page) {  //Serialize
		try {
			//String desktopPath = System.getProperty("user.home") + "/Desktop";
			String desktopPath ="resources/data";
			FileOutputStream fileOut = new FileOutputStream(desktopPath + "/" + page.getTableName() +page.getId() + ".class");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(page);
			objectOut.close();
			return true;

		} catch (Exception ex) {
			return false;
		}
	}
	
	public static Page loadPage(String strTableName, int id) throws DBAppException { //Deserialize
		try {
			//String desktopPath = System.getProperty("user.home") + "/Desktop";
			String desktopPath ="resources/data";
			FileInputStream filein = new FileInputStream(desktopPath + "/" + strTableName + id + ".class");
			ObjectInputStream objectin = new ObjectInputStream(filein);
			Page page = (Page) objectin.readObject();
			objectin.close();
			return page;

		} catch (Exception ex) {
			//ex.printStackTrace();
			throw new DBAppException("Page "+id+" not found.\n	" + ex.getLocalizedMessage());
		}
	}
	
	
	public static boolean writeIndex(Octree index) { // ******** */
		try {
			FileOutputStream fileOut = new FileOutputStream("resources/data/" + index.getTableName()+"_"+index.getOctreeName() + ".class");
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(index);
			objectOut.close();
			return true;

		} catch (Exception ex) {
			return false;
		}
	}
	
	public static Octree loadIndex(String tableName,String octreeIndexName) throws DBAppException { // ********//
		try {
			FileInputStream filein = new FileInputStream("resources/data/" + tableName+"_"+octreeIndexName + ".class");
			ObjectInputStream objectin = new ObjectInputStream(filein);
			Octree octree = (Octree) objectin.readObject();
			objectin.close();
			return octree;

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new DBAppException("Index "+tableName+"_"+octreeIndexName+" not found.\n	" + ex.getLocalizedMessage());
		}
	}
//--------------------------------------------------------(Delete Page .class file)---------------------------------------------------------------------
	
	public static boolean deletePage(String strTableName, int id) {
		
		//String desktopPath = System.getProperty("user.home") + "/Desktop";
		String desktopPath ="resources/data";
		File file = new File(desktopPath + "/" + strTableName + id + ".class");
        if (file.delete()) {
            return true;
        } else {
            return false;
        }
	}
}
