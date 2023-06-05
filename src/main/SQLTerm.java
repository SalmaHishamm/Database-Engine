package main;

public class SQLTerm {
	public String _strTableName = "default";
    public String _strColumnName = "default";
    public String _strOperator = "default";
    public Object _objValue = null;

    
//    static {
//        SQLTerm term = new SQLTerm();
//        term._strTableName = "default";
//        term._strColumnName = "default";
//        term._strOperator = "default";
//        term._objValue = null;
//    }
    
    public SQLTerm(){

    	_strTableName = "default";
        _strColumnName = "default";
        _strOperator = "default";
        _objValue = null;
    }

    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) throws DBAppException {
    	this._strTableName =strTableName;
        this._strColumnName =strColumnName;
        if(check(strOperator)==true)
            this._strOperator =strOperator;
        else
            throw new DBAppException("Invalid operator");
        this._objValue =objValue;

    }
    public boolean check(String strOperator) {
    	boolean b = false;
    	if (strOperator.equals("<")|| strOperator.equals(">")||strOperator.equals("<=") ||
    			strOperator.equals(">=")||strOperator.equals("!=") || strOperator.equals("="))
    		b = true;
    	return b;
    }
    
    
    
    public boolean or() {
		
    	return false;
    	
    }
    
}

