package main;


import java.io.FileReader;
import java.io.Serializable;
import java.sql.Date;
//import java.time.LocalDate;
//import java.time.ZoneId;
//import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
//import java.util.List;
import java.util.Properties;
import java.util.Vector;
//import java.util.stream.Collectors;

import utilities.Metadata;

public class OctNode implements Serializable{
	
	private Vector<OctEntry> entries  = new Vector<>();
	private OctNode [] children = new OctNode[8];
	private OctPoint min ;
	private OctPoint max;
	private int maximumEntries ; 
	private OctNode left ;
	private OctNode right ;
	
	
	
//	public OctNode()  {
//		this.entries = new Vector<>();
//		readconf();
//	   
//	}
//	public OctNode(Object x, Object y , Object z, String primaryKey, String tableName, int pageID)  {
//		OctPoint octPoint =new OctPoint(x,y,z);
//		OctEntry octEntry = new OctEntry( octPoint ,primaryKey, tableName,pageID);
//		entries.add(octEntry);
//		readconf();
//	}
//	
	
	public OctNode(OctPoint min, OctPoint max) throws DBAppException {
		if(Metadata.compareObjects(min.getX(),max.getX())>0 || Metadata.compareObjects(min.getY(),max.getY())>0 || Metadata.compareObjects(min.getZ(),max.getZ())>0){
            throw new DBAppException("The bounds are not properly set!");
        }
		
		//this.entries= null; //TODO
		this.min = min;
		this.max = max;
//		for (int i = 0; i <= 7; i++){
//            children[i] = new OctNode();
//        }
		readconf();
	}
	public void insert(Object x, Object y, Object z, String primaryKey, String tableName, Vector<Integer> pageIDs) throws DBAppException {
		 	
		    if (Metadata.compareObjects(x,min.getX())<0 || Metadata.compareObjects(x,max.getX())>0
                || Metadata.compareObjects(y,min.getY())<0 || Metadata.compareObjects(y,max.getY())>0
                || Metadata.compareObjects(z,min.getZ())<0 || Metadata.compareObjects(z,max.getZ())>0)
			    throw new DBAppException("Insertion point is out of bounds! X: " + x + " Y: " + y + " Z: " + z );
		    
		    if(find(x, y, z)){
		    	
	            findEntry(x,y,z).getPageIDs().add(pageIDs.get(0));
	            return;
	        }

			Object midx = mid(min.getX(),max.getX());
            Object midy = mid(min.getY(),max.getY());
            Object midz = mid(min.getZ(),max.getZ());
	        

	        int pos = getPos(midx,midy,midz,x,y,z);
           
	       if(children[pos]!=null) {
	         if(this.entries== null)
//	        	 System.out.println("Non leaf " + pos);
	        	children[pos].insert(x, y, z, primaryKey, tableName, pageIDs);
	       }
	       else if(this.isLeaf()) {
	          if(entries.size()< maximumEntries) {
//	        	  System.out.println("Insert Entry " + pos);
	        	  OctEntry addedEntry = new OctEntry(new OctPoint(x,y,z),primaryKey,tableName, pageIDs );
            	  entries.add(addedEntry);
            	  //children[pos]=new OctNode(x, y, z, primaryKey, tableName, pageID);
            	  
               }
	          else if (entries.size()>= maximumEntries) {
//	        	  System.out.println("Split " + pos);
	        	  Vector<OctEntry> keepTrackOf = entries;
	        	  split(x, y, z, pos);
	        	  
	        	  for (OctEntry entry : keepTrackOf) {
//	        		  System.out.println("ReInsert old Entries in node");
	        		  insert(entry.getPoint().getX(),entry.getPoint().getY(),entry.getPoint().getZ(),
	        				 entry.getPrimaryKey(), entry.getTableName(),entry.getPageIDs());
	        	  }
//	        	  System.out.println("Insert New Entries ");
	        	 insert(x, y, z, primaryKey, tableName, pageIDs);
	        	  
	           }
	         }
//	         
//	       else if( entries.size()< maximumEntries) {
//      	     OctEntry addedEntry = new OctEntry(new OctPoint(x,y,z),primaryKey,tableName,pageID );
//      	     entries.add(addedEntry);
//      	     
//            }
	       
	       this.toString();
}	      
	public boolean remove(Object x, Object y, Object z){
        if (Metadata.compareObjects(x,min.getX())<0 || Metadata.compareObjects(x,max.getX())>0
                || Metadata.compareObjects(y,min.getY())<0 || Metadata.compareObjects(y,max.getY())>0
                || Metadata.compareObjects(z,min.getZ())<0 || Metadata.compareObjects(z,max.getZ())>0) 
        	return false;
        Object midx = mid(min.getX(),max.getX());
        Object midy = mid(min.getY(),max.getY());
        Object midz = mid(min.getZ(),max.getZ());

        int pos = getPos(midx,midy,midz,x,y,z);

        if(children[pos]!=null) {
	         if(this.entries== null)
                return children[pos].remove(x, y, z);
        } else if(this.isLeaf()) {
        	for (OctEntry entry : this.entries) {
        		if(Metadata.compareObjects(x, entry.getPoint().getX())==0 && Metadata.compareObjects(y, entry.getPoint().getY())==0 
        				&& Metadata.compareObjects(z, entry.getPoint().getZ())==0) {
        			this.entries.remove(entry);
        			return true;
        		}
        	}	
        }
        
            
    
        return false;
	}
	
	public void update(Object x , Object y , Object z,String primaryKey, String tableName,Vector<Integer> pageIDs) throws DBAppException {
		//remove(x,y,z);
		insert( x, y, z, primaryKey, tableName, pageIDs);
	}
	
	 public void split (Object x, Object y, Object z, int pos) throws DBAppException {
		 Object midx = mid(min.getX(),max.getX());
         Object midy = mid(min.getY(),max.getY());
         Object midz = mid(min.getZ(),max.getZ());
         
         
		 this.entries=null;
        //children[pos] = null;
//         if(pos == OctLocations.TopLeftFront.getNumber()){
//        	 children[0] = new OctNode(new OctPoint(min.getX(), min.getY(), min.getZ()), new OctPoint(midx, midy, midz));
//         }
//         else if(pos == OctLocations.TopRightFront.getNumber()){
//             children[1] = new OctNode(new OctPoint(midx, min.getY(), min.getZ()),new OctPoint( max.getX(), midy, midz));
//         }
//         else if(pos == OctLocations.BottomRightFront.getNumber()){
//             children[2] = new OctNode(new OctPoint(midx, midy, min.getZ()),new OctPoint( max.getX(), max.getY(), midz));
//         }
//         else if(pos == OctLocations.BottomLeftFront.getNumber()){
//             children[3] = new OctNode(new OctPoint(min.getX(),midy, min.getZ()), new OctPoint(midx, max.getY(), midz));
//         }
//         else if(pos == OctLocations.TopLeftBottom.getNumber()){
//             children[4] = new OctNode(new OctPoint(min.getX(), min.getY(), midz), new OctPoint(midx, midy, max.getZ()));
//         }
//         else if(pos == OctLocations.TopRightBottom.getNumber()){
//             children[5] = new OctNode(new OctPoint(midx, min.getY(), midz),new OctPoint( max.getX(), midy, max.getZ()));
//         }
//         else if(pos == OctLocations.BottomRightBack.getNumber()){
//             children[6] = new OctNode(new OctPoint(midx, midy,midz), new OctPoint(max.getX(), max.getY(), max.getZ()));
//         }
//         else if(pos == OctLocations.BottomLeftBack.getNumber()){
//             children[7] = new OctNode(new OctPoint(min.getX(), midy, midz),new OctPoint( midx, max.getY(), max.getZ()));
//         }
		 children[0] = new OctNode(new OctPoint(min.getX(), min.getY(), min.getZ()), new OctPoint(midx, midy, midz));
		 children[1] = new OctNode(new OctPoint(midx, min.getY(), min.getZ()),new OctPoint( max.getX(), midy, midz));
		 children[2] = new OctNode(new OctPoint(midx, midy, min.getZ()),new OctPoint( max.getX(), max.getY(), midz));
		 children[3] = new OctNode(new OctPoint(min.getX(),midy, min.getZ()), new OctPoint(midx, max.getY(), midz));
		 children[4] = new OctNode(new OctPoint(min.getX(), min.getY(), midz), new OctPoint(midx, midy, max.getZ()));
		 children[5] = new OctNode(new OctPoint(midx, min.getY(), midz),new OctPoint( max.getX(), midy, max.getZ()));
		 children[6] = new OctNode(new OctPoint(midx, midy,midz), new OctPoint(max.getX(), max.getY(), max.getZ()));
		 children[7] = new OctNode(new OctPoint(min.getX(), midy, midz),new OctPoint( midx, max.getY(), max.getZ()));
                
	 }
	 public int getPos(Object midx,Object midy,Object midz,Object x, Object y, Object z) {
		 int pos;
		 if(Metadata.compareObjects(x, midx)<=0){
	            if(Metadata.compareObjects(y, midy)<=0){
	                if(Metadata.compareObjects(z, midz)<=0)
	                    pos = OctLocations.TopLeftFront.getNumber();
	                else
	                    pos = OctLocations.TopLeftBottom.getNumber();
	            }else{
	                if(Metadata.compareObjects(z, midz)<=0)
	                    pos = OctLocations.BottomLeftFront.getNumber();
	                else
	                    pos = OctLocations.BottomLeftBack.getNumber();
	            }
	        }else{
	            if(Metadata.compareObjects(y, midy)<=0){
	                if(Metadata.compareObjects(z, midz)<=0)
	                    pos = OctLocations.TopRightFront.getNumber();
	                else
	                    pos = OctLocations.TopRightBottom.getNumber();
	            }else {
	                if(Metadata.compareObjects(z, midz)<=0)
	                    pos = OctLocations.BottomRightFront.getNumber();
	                else
	                    pos = OctLocations.BottomRightBack.getNumber();
	            }
	        }
		 return pos;
	 }
//	 public void updateSiblings2(int i, int j) {
//		 
//		if(this.children[i].children[j])
//	        	  
//		 
//		 
//		 
//	 }
//	 public void updateSiblings() {
//		for (int i=0 ; i<8 ; i++) {
//			if (this.children[i]!=null) {
//				if (this.children[i].isLeaf())
//			}
//		}
//		 
//		 
//		 
//	 }
	 public boolean find(Object x, Object y, Object z){
	    	if (Metadata.compareObjects(x,min.getX())<0 || Metadata.compareObjects(x,max.getX())>0
	                || Metadata.compareObjects(y,min.getY())<0 || Metadata.compareObjects(y,max.getY())>0
	                || Metadata.compareObjects(z,min.getZ())<0 || Metadata.compareObjects(z,max.getZ())>0)
	        	return false;
	        
	        
	         Object midx = mid(min.getX(),max.getX());
             Object midy = mid(min.getY(),max.getY());
             Object midz = mid(min.getZ(),max.getZ());

             int pos = getPos(midx,midy,midz,x,y,z);

	        if(children[pos]!=null) {	
	          if(this.entries == null)
	            return children[pos].find(x, y, z);
	        }  
	        if(this.isLeaf()) {
	        	for (OctEntry entry : this.entries) {
	        		if(Metadata.compareObjects(x,entry.getPoint().getX())==0 && Metadata.compareObjects(y,entry.getPoint().getY())==0 && Metadata.compareObjects(z,entry.getPoint().getZ())==0)
	        			return true;
	        	}
	          }
	        
//	        else {
//	        	for (OctEntry entry : this.entries) {
//	        		if(Metadata.compareObjects(x,entry.getPoint().getX())==0 && Metadata.compareObjects(y,entry.getPoint().getY())==0 && Metadata.compareObjects(z,entry.getPoint().getZ())==0)
//	        			return true;
//	        	}
//	        }
	            
	        return false;
	        
	    }
	 public OctEntry findEntry(Object x, Object y, Object z){
	    	if (Metadata.compareObjects(x,min.getX())<0 || Metadata.compareObjects(x,max.getX())>0
	                || Metadata.compareObjects(y,min.getY())<0 || Metadata.compareObjects(y,max.getY())>0
	                || Metadata.compareObjects(z,min.getZ())<0 || Metadata.compareObjects(z,max.getZ())>0)
	        	return null;
	        
	        
	      Object midx = mid(min.getX(),max.getX());
          Object midy = mid(min.getY(),max.getY());
          Object midz = mid(min.getZ(),max.getZ());

          int pos = getPos(midx,midy,midz,x,y,z);

	        if(children[pos]!=null) {	
	          if(this.entries == null)
	            return children[pos].findEntry(x, y, z);
	        }  
	        if(this.isLeaf()) {
	        	for (OctEntry entry : this.entries) {
	        		if(Metadata.compareObjects(x,entry.getPoint().getX())==0 && Metadata.compareObjects(y,entry.getPoint().getY())==0 && Metadata.compareObjects(z,entry.getPoint().getZ())==0)
	        			return entry;
	        	}
	          }
	        
//	        else {
//	        	for (OctEntry entry : this.entries) {
//	        		if(Metadata.compareObjects(x,entry.getPoint().getX())==0 && Metadata.compareObjects(y,entry.getPoint().getY())==0 && Metadata.compareObjects(z,entry.getPoint().getZ())==0)
//	        			return true;
//	        	}
//	        }
	            
	        return null;
	        
	    }
	 public int searchUpdate( Object primaryKey , char coordinate) {
		 
		    int result=-1 ;
		    if (this.isLeaf()) {
		        for (int i =0; i< entries.size(); i++) {
		        	OctEntry entry = entries.get(i);
		        	switch(coordinate) {
		        	case 'x':
		                if (Metadata.compareObjects(primaryKey, entry.getPoint().getX()) ==0) {
		                	result=(int) entry.getPageIDs().get(0);
		                	
		                	//remove here
		                	entries.remove(entry);
		                	i--;
		                	
		                }
		                break;
		        	case 'y':
			            if (Metadata.compareObjects(primaryKey, entry.getPoint().getY()) ==0) {
			                result=(int) entry.getPageIDs().get(0);
			                entries.remove(entry);
			                i--;
			            }
			            break;
		        	case 'z':
			            if (Metadata.compareObjects(primaryKey, entry.getPoint().getZ()) ==0) {
			                result=(int) entry.getPageIDs().get(0);
			                entries.remove(entry);
			                i--;
			            }
			            break;    
			            
		          }
		      } 
		    }
		    else {
		    	Object minBound ;
		        Object maxBound ;
		        Object midPoint ;
		      switch(coordinate){
		      case 'x':
		         minBound = min.getX();
		         maxBound = max.getX();
		         midPoint = mid(minBound,maxBound);

		        if (Metadata.compareObjects(primaryKey, midPoint)<=0) {
		            result=children[0].searchUpdate(primaryKey, coordinate);
		            result=children[3].searchUpdate(primaryKey, coordinate);
		            result=children[4].searchUpdate(primaryKey, coordinate);
		            result=children[7].searchUpdate(primaryKey, coordinate);
		        } else {
		        	result=children[1].searchUpdate(primaryKey, coordinate);
		        	result=children[2].searchUpdate(primaryKey, coordinate);
		        	result=children[5].searchUpdate(primaryKey, coordinate);
		        	result=children[6].searchUpdate(primaryKey, coordinate);
		        }
		        break;
		      case 'y':
			         minBound = min.getY();
			         maxBound = max.getY();
			         midPoint = mid(minBound,maxBound);

			        if (Metadata.compareObjects(primaryKey, midPoint)<=0) {
			            result=children[0].searchUpdate(primaryKey, coordinate);
			            result=children[1].searchUpdate(primaryKey, coordinate);
			            result=children[4].searchUpdate(primaryKey, coordinate);
			            result=children[5].searchUpdate(primaryKey, coordinate);
			        } else {
			        	result=children[2].searchUpdate(primaryKey, coordinate);
			        	result=children[3].searchUpdate(primaryKey, coordinate);
			        	result=children[6].searchUpdate(primaryKey, coordinate);
			        	result=children[7].searchUpdate(primaryKey, coordinate);
			        }
			        break;
		      case 'z':
			         minBound = min.getZ();
			         maxBound = max.getZ();
			         midPoint = mid(minBound,maxBound);

			        if (Metadata.compareObjects(primaryKey, midPoint)<=0) {
			            result=children[0].searchUpdate(primaryKey, coordinate);
			            result=children[1].searchUpdate(primaryKey, coordinate);
			            result=children[2].searchUpdate(primaryKey, coordinate);
			            result=children[3].searchUpdate(primaryKey, coordinate);
			        } else {
			        	result=children[4].searchUpdate(primaryKey, coordinate);
			        	result=children[5].searchUpdate(primaryKey, coordinate);
			        	result=children[6].searchUpdate(primaryKey, coordinate);
			        	result=children[7].searchUpdate(primaryKey, coordinate);
			        }
			        break;
			        
		    }
	 }
	 
	 
		    return result;
}
	 
	 public static boolean checkOperator(Object value,OctNode node,String op) throws DBAppException {
		 
	      Object min = node.getMin();
	      Object max = node.getMax();
	      
	      
			switch(op) {
			case "=": return Metadata.compareObjects(value,min )>=0 && Metadata.compareObjects(value,max)<=0?true:false ;
			case ">": return Metadata.compareObjects(max, value)<=0?  false:true; 
			case ">=":return Metadata.compareObjects(max, value)<0?  false:true;
			case "<=":return Metadata.compareObjects(min, value)>0?  false:true;
			case "<": return Metadata.compareObjects(min,value)>=0?  false:true;
			
			default: throw new DBAppException("Invalid operator");
			}
			
		}
	 
	 
	 
	 
	 
	 public static boolean checkOperatorX(Object value,OctEntry entry,String opx) throws DBAppException {
		 
	      Object x = entry.getPoint().getX();
			switch(opx) {
			case "=": return Metadata.compareObjects(x,value )==0? true:false  ;
			case ">": return Metadata.compareObjects(x,value)>0?  true:false ; 
			case ">=":return Metadata.compareObjects(x,value)>=0?  true:false ;
			case "<=":return Metadata.compareObjects(x,value)<=0? true:false ;
			case "<": return Metadata.compareObjects(x,value)<0? true:false ;
			
			default: throw new DBAppException("Invalid operator");
			}
			
		}
	 
	 public static boolean checkOperatorY(Object value,OctEntry entry,String opy) throws DBAppException {
		 
	      Object y = entry.getPoint().getY();
			switch(opy) {
			case "=": return Metadata.compareObjects(y,value )==0?true:false   ;
			case ">": return Metadata.compareObjects(y,value )>0?  true:false  ; 
			case ">=":return Metadata.compareObjects(y,value )>=0?  true:false  ;
			case "<=":return Metadata.compareObjects(y,value )<=0?  true:false  ;
			case "<": return Metadata.compareObjects(y,value )<0?  true:false  ;
			
			default: throw new DBAppException("Invalid operator");
			}
			
		}
	 
	 public static boolean checkOperatorZ(Object value,OctEntry entry,String opz) throws DBAppException {
		 
	      Object z = entry.getPoint().getZ();
			switch(opz) {
			case "=": return Metadata.compareObjects(z,value )==0?true:false;
			case ">": return Metadata.compareObjects(z,value)>0?  true:false ; 
			case ">=":return Metadata.compareObjects(z,value)>=0?  true:false ;
			case "<=":return Metadata.compareObjects(z,value)<=0? true:false;
			case "<": return Metadata.compareObjects(z,value)<0?  true:false;
			
			default: throw new DBAppException("Invalid operator");
			}
			
		}
	 
	 
	 
	 public Vector<OctEntry> SearchRange (Object x , Object y , Object z , String opx ,  String opy, String opz) throws DBAppException{
		 Vector result = new Vector<>();
		 System.out.print(this.isLeaf());
		 if (this.isLeaf()) {
			 System.out.print(entries.size());
			 for (OctEntry entry : this.entries) {
		            if (checkOperatorX(x,entry,opx)
		            		&& checkOperatorY(y,entry,opy)
		            		&& checkOperatorZ(z,entry,opz)) {
		                result.add(entry);
		                System.out.print(entry);
		            }
			 }
		 
	 }
		 else {
			 for (OctNode child : this.children) {
				if (checkOperator(x,child,opx) && checkOperator(y,child,opy)&& checkOperator(z,child,opz)) {
					result.addAll(SearchRange(x,y,z,opx,opy,opz));
				}
				 
				 
			 }
			 
			 
		 }
		 return result;
		 
		 
		 
	 }
//	 public List<OctEntry> rangeQuery(OctNode node, Object x, Object y, Object z) {
//		    List<OctEntry> results = new ArrayList<OctEntry>();
//		    if (!(Metadata.compareObjects(x,min.getX())<0 || Metadata.compareObjects(x,max.getX())>0
//                || Metadata.compareObjects(y,min.getY())<0 || Metadata.compareObjects(y,max.getY())>0
//                || Metadata.compareObjects(z,min.getZ())<0 || Metadata.compareObjects(z,max.getZ())>0)) {
//		    	
//		    }
//		    // check if the node intersects with the range
//		    if (!node.intersectsRange(minValues, maxValues)) {
//		        return results;  // node is outside of the range
//		    }
//		    if ()
//		    
//		    // check if the node is a leaf
//		    if (node.isLeaf()) {
//		        // iterate through all entries in the leaf node
//		        for (Entry entry : node.getEntries()) {
//		            if (entry.intersectsRange(minValues, maxValues)) {
//		                results.add(entry);  // add matching entries to the results list
//		            }
//		        }
//		        return results;
//		    }
//		    
//		    // node is not a leaf, so recursively search its children
//		    for (OctreeNode child : node.getChildren()) {
//		        results.addAll(rangeQuery(child, minValues, maxValues));
//		    }
//		    
//		    return results;
//		}
//	

	 
	 public static Object incrementMid( Object o ) {
	    	if (o instanceof String) {
	    		String s = ((String)o).toLowerCase();
	    		int N = s.length();
				int[] a1 = new int[N + 1];
				 
		        for (int i = 0; i < N; i++) {
		            a1[i + 1] = (int)s.charAt(i) - 97 ;
		        }
	    		String res= "";
	    		a1[a1.length-1]++; //a--> b
	    		if(a1[a1.length-1]>25) {
	    			a1[a1.length-1]=25;
	    		}
	    		for (int i = 1; i <= N; i++) {
		            res= res + (char)(a1[i] + 97);
		        }
		        return res;
	    		
	    		
	    	}
	    	else if ( o instanceof Date ) {
//	    		Date d = (Date)o;
//	    		LocalDate localdateObject = d.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
//	    		LocalDate incrementDate = localdateObject.plusDays(1);
//	    		
//	    		
//	    		return (Date) Date.from(incrementDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
	    		Date d = (Date)o;
	    		return new Date(d.getTime()+1);
	    	}
	    	else if (o instanceof Integer ) {
	    		return (int)o +1;
	    	}
	    	else if(o instanceof Double ) {
	    		return (double)o + 0.1;
	    	}
	    		return o ;
	    }
	public static Date midDate ( Object o1 , Object o2 ) {
		   
//	       Date d1 = (Date) o1;
//	       Date d2 = (Date) o2;
//	       LocalDate localDate1 = d1.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
//	       LocalDate localDate2 = d2.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
//	       
//	       List <LocalDate> listofDates =  localDate1.datesUntil(localDate2).collect(Collectors.toList());
//	       int mid = (int)(listofDates.size()/2);
//	       LocalDate midLocalDate = listofDates.get(mid);
//	       
//           
//	       return (Date) Date.from(midLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
		Date d1 = (Date) o1;
		Date d2 = (Date) o2;
		return new Date(d1.getTime()+d2.getTime()/2);
		
			
		}
		public static String midString(Object o1 , Object o2 ){
			String s1 = (String) o1  ;
			String s2 = (String) o2 ;
			String S="" ;
		    String T="" ;
		    String mid="";
		    if(s1.length()>s2.length()){
				s2= s2+ s1.substring(s2.length(),s1.length());
		    }
		    else if(s1.length()<s2.length()) {
		    	s1= s1+ s2.substring(s1.length(),s2.length());
		    }
		    if ( s1.compareTo(s2) >0) {
			     S= s1;
				 T= s2;
			}
			else {
				 S=s2;
				 T=s1;
			}
			int N = S.length();
			int[] a1 = new int[N + 1];
			 
	        for (int i = 0; i < N; i++) {
	            a1[i + 1] = (int)S.charAt(i) - 97
	                        + (int)T.charAt(i) - 97;
	        }
	 
	        // Iterate from right to left
	        // and add carry to next position
	        for (int i = N; i >= 1; i--) {
	            a1[i - 1] += (int)a1[i] / 26;
	            a1[i] %= 26;
	        }
	 
	        // Reduce the number to find the middle
	        // string by dividing each position by 2
	        for (int i = 0; i <= N; i++) {
	 
	            // If current value is odd,
	            // carry 26 to the next index value
	            if ((a1[i] & 1) != 0) {
	 
	                if (i + 1 <= N) {
	                    a1[i + 1] += 26;
	                }
	            }
	 
	            a1[i] = (int)a1[i] / 2;
	        }
	 
	        for (int i = 1; i <= N; i++) {
	            mid= mid + (char)(a1[i] + 97);
	        }
	        return mid;
	    }
	 
		
		public static double midDouble (Object o1, Object o2) {
			Double d1 = (Double) o1;
			Double d2 = (Double) o2;
			return d1+d2/2 ;
			
		}
		public static int midInteger (Object o1, Object o2) {
			int d1 = (int) o1;
			int d2 = (int) o2;
			return d1+d2/2 ;
			
		}
	    public static Object mid ( Object topLeftFrontObject,Object bottomRightBackObject){
	    	Object mido=null;
	    	if (topLeftFrontObject instanceof Date ) {
	    		mido= midDate(topLeftFrontObject,bottomRightBackObject);
	        }
	        else if (topLeftFrontObject instanceof String) {
	        	mido= midString(topLeftFrontObject,bottomRightBackObject);
	        }
	        else if (topLeftFrontObject instanceof Integer ) {
	        	mido = midInteger(topLeftFrontObject,bottomRightBackObject);
	        }
	        else if (topLeftFrontObject instanceof Double ) {
	        	mido= midDouble(topLeftFrontObject,bottomRightBackObject);
	        }
	    	return mido;
		}
	    public boolean isLeaf() {
	    	boolean flag=true;
	    	
	    	for (int i=0;i<8;i++) {
	    		if (children[i]!=null) {
	    			flag=false;
	    			break;
	    		}
	    			
	    	}
	    	return flag;
	    	
	    }
     public void readconf()  {
		
//		Properties props = new Properties();
//		InputStream in = getClass().getResourceAsStream("resources/DBApp.config");
//		try {
//			props.load(in);
//		} catch (IOException e) {			
//			e.printStackTrace();
//		}
//		rowsPerPage = Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage"));
		
		Properties props = new Properties();
		
		//InputStream in =null;
		//in= new FileInputStream("resources/DBApp.config");
		ArrayList<Integer> configRead= new ArrayList<Integer>();
		
		try {
			FileReader reader = new FileReader("resources/DBApp.config");
			
			props.load(reader);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

			configRead.add(Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage")));
			configRead.add(Integer.parseInt(props.getProperty("MaximumEntriesinOctreeNode")));
			maximumEntries=configRead.get(1);
			//maximumEntries=3;
			
			
        }
	public Vector<OctEntry> getEntries() {
		return entries;
	}
	public OctNode[] getChildren() {
		return children;
	}
	public OctPoint getMin() {
		return min;
	}
	public OctPoint getMax() {
		return max;
	}
	public int getMaximumEntries() {
		return maximumEntries;
	}
	public OctNode getLeft() {
		return left;
	}
	public OctNode getRight() {
		return right;
	}
	
	public String toString(){
		String s ="";
		if (this.isLeaf()) {
		//	System.out.println(entries.size());
		for(int i = 0 ; i<entries.size();i++) {
			OctEntry entry = entries.get(i);
			s+= "Entry " + i+ ": X= "+ entry.getPoint().getX() +", Y= "+ entry.getPoint().getY()+", Z= "+ entry.getPoint().getZ()+", PageID= "+entry.getPageIDs().get(0)+ ". \n";	
		}
		//System.out.println("entries");
		}
		else {
		//	System.out.println("children");
			for (int j = 0 ; j<8; j++) {
				if(children[j]!=null ) {
					
				       s+= "Children " + j +": " + children[j].toString();
					
				}
			}
		}
		return s;
	}
	
     

}
