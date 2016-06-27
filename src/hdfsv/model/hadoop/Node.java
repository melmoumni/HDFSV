/**
 * @author Mohammed El Moumni, Vivien Achet
 */

package hdfsv.model.hadoop;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * What this class represent:
 * 		It represents a node (file or directory) in the hdfs architecture
 * 		- data is the node name
 * 		- size is its size (in the case of a directory, the sum of its children' size)
 * 		- path is the complete path, including its name
 * 		- children the list of its children
 * 		- parent is its parent node
 * 		- lastModified is the date of last modification of the node: this is updated by the same date given by hdfs 
 * 
 * Initial value is defined by the constructor
 *
 */
public class Node{
    private String data;
    private long size;
    private Timestamp lastModified;
    private String path;
	private Node parent;
	private ArrayList<Node> children;
	
    public Node(String data, long size, long lastModified, String path){
        this.data = data;
        this.size = size;
        this.lastModified = new Timestamp(lastModified);
        this.path = path;
        children = new ArrayList<Node>();
    }
    
    public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public ArrayList<Node> getChildren() {
		return children;
	}

	public void setChildren(ArrayList<Node> children) {
		this.children = children;
	}

	public Node getChild(String data){
        for(Node n : children)
            if(n.data.equals(data))
                return n;
        return null;
    }
    
    public long getLastModified() {
		return lastModified.getTime();
	}

	public void setLastModified(long lastModified) {
		this.lastModified.setTime(lastModified);
	}
	
	public String getPath() {
		return path;
	}
	
	public void setPath(String path) {
		this.path = path;
	}
	
	public Node getParent() {
		return parent;
	}
	
	public void setParent(Node parent) {
		if(parent != null)
			this.parent = parent;
	}

	public void removeChild(Node child) {
		children.remove(child);
	}

	// overload the java.lang equals method to check if 2 nodes are the same
	public boolean equals(Node n) {
		if (this == n) return true;
		if (this == null) return false;
		if (this.getClass() != n.getClass()) return false;
		return this.path.equals(n.getPath());
	}
}
