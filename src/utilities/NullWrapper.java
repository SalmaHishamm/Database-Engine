package utilities;

import java.io.Serializable;

public class NullWrapper implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NullWrapper() {}
	
	public String toString() {
		return "null";
	}
}
