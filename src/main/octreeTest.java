package main;

import java.io.IOException;

public class octreeTest {
      public static void main(String[]args) throws  DBAppException {
    	  String [] arra = {"x","y","z"};
    	  
    	 OctPoint min=new OctPoint("a",0,3);
  	     OctPoint max=new OctPoint("zz",10,11);
  	     Octree o1 = new Octree("Xyz",arra,"a",0,3,"zz",10,11);
    	 // System.out.println(o1);
    	  o1.insert("a",2,4,"pk","tablename", 1);
    	  o1.insert("bc",3,5,"pk","tablename", 1);
    	  o1.insert("ef",8,7,"pk","tablename", 1);
    	  o1.insert("lm",4,8,"pk","tablename", 1);
    	  o1.insert("xy",8,8,"pk","tablename", 1);
    	  o1.insert("gr",7,3,"pk","tablename", 1);
    	  o1.insert("gi",2,8,"pk","tablename", 1);
    	  System.out.println(o1.find("a",2,4));
    	  System.out.println(o1.find("bc",3,5));
    	  System.out.println(o1.find("ef",8,7));
    	  System.out.println(o1.find("lm",4,8));
    	  System.out.println(o1.find("xy",8,8));
    	  System.out.println(o1.find("gr",7,3));
    	  System.out.println(o1.find("gi",2,8));
   	  
    	  o1.remove("xy",8,8);
    	  System.out.println(o1.find("xy",8,8));
    	  System.out.println(o1);
//    	  System.out.println(o1.findEntry("xy",8,8));
//    	  System.out.println(o1.findEntry("gr",7,3));
//    	  
//    	  o1.insert("gi",2,8,"pk","tablename", 2);
//    	  o1.insert("gi",2,8,"pk","tablename", 3);
//    	  for (int i=0; i < o1.findEntry("gi",2,8).getPageIDs().size();i++) {
//    		  System.out.println(o1.findEntry("gi",2,8).getPageIDs().get(i));
//    	  }
      }
}
