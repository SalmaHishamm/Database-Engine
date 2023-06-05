package main;

import java.io.Serializable;

public class OctPoint implements Serializable{

    private Object x;
    private Object y;
    private Object z;
  
   
    
    public OctPoint(Object x, Object y, Object z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public OctPoint(){
      x=null;
      y=null;
      z=null;
       
    }

    public Object getX(){
        return x;
    }

    public Object getY(){
        return y;
    }

    public Object getZ(){
        return z;
    }	
}
