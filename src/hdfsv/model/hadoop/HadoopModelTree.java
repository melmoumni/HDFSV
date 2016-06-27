/**
 * @author Mohammed El Moumni, Vivien Achet
 */

package hdfsv.model.hadoop;

import java.io.Serializable;
import java.util.*;
import javax.enterprise.context.SessionScoped;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import com.google.common.collect.TreeTraverser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import hdfsv.exceptions.HadoopConfException;
import hdfsv.model.hadoop.HadoopModelI;
import hdfsv.model.hadoop.HadoopModelImpl;

/**
 * 
 * What this class represent:
 * 		It represents the whole tree linked to a hdfs configuration file
 * 		In order to improve performances in case the cluster contains millions/billions/trillions of files (which may 
 * 		trigger a long awaiting), the user can set a threshold size, minSize, that ensure the following :
 * 		Ensures :
 * 			- if node.getSize() > minSize :
 * 				the node is displayed in the architecture
 * 			- else :
 * 				if the node is a file, it is displayed as a "others-LT$minSize" file. 
 * 				every file in the node.getParent() directory that does not pass the threshold is added under the same label
 * 		Why doing that :
 * 			It may reduce the number of file rendered by the client browser, so for an average cluster it increases a lot the 
 * 			performances. 
 * 			In the worst case where the cluster contains the same number of directory and file (i.e. each directory
 * 			contains only 1 file) it does not improve performances.
 * 		
 * 		This class is sessionScoped so that when a user make multiple request in a row there is no need to rebuild the
 * 		whole architecture. It allows the implementation to hold some optimization.
 * 		There is 2 possibilities that explains why the client view might need to be updated:
 * 			- the architecture itself have been modified: a file/directory added or deleted
 * 			- the user modified the minSize threshold
 * 		The functions called for those 2 updates are respectively:
 * 			- void updateLastModified(int newMinSize);
 * 			- void updateLastMinSize(Node directory);
 *
 */
@SessionScoped
@Named("treeImpl")
public class HadoopModelTree extends HadoopModelImpl implements TreeI, Serializable, HadoopModelI{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Node root;
	private int minSize;
	private boolean isInitilized;
	private FileSystem hdfs;
	private Map <String, Long> otherNodes;
	private String otherName = ".other-generated-hdfsv";
	
	/**
	 * Constructor: initialize root and hdfs
	 * @throws HadoopConfException
	 */
	public HadoopModelTree() throws HadoopConfException{
		try{
			String cf = System.getenv("HADOOP_CONF");
			Path p = new Path(cf);
			Configuration configuration = new Configuration(true);
			configuration.addResource(p);
			hdfs = FileSystem.get(configuration);
			root = new Node("/", 0, 0, "/");
			this.minSize = 0;
			this.isInitilized = false;
			this.otherNodes = new HashMap<String, Long>();
		}
		catch(Exception e){
			throw new HadoopConfException();
		}
	}

	public String getHadoopData(int minSize) throws HadoopConfException {
		if(!this.isInitilized()){
			System.out.println("init");
			this.init(minSize, "/");
			return this.getJson();
		}
		else{
			System.out.println("update");
			this.update(minSize);
			return this.getJson();
		}
	}
	
	public boolean isEmpty(){
		return root==null;
	}

	/**
	 * Add a node to the current tree
	 * Ensures:
	 * 		Tree t = new Tree();           => /
	 * 		t.add("foo.txt", 180, n);      => /
	 * 									       | foo.txt
	 * 
	 * 		t.add("bar/foobar.sh" 55, n);  => /
	 * 										   | foo.txt
	 * 										   | bar/
	 * 											     | foobar.sh	
	 * 
	 * 		t.add("bar/toto.pdf", 980, n); => /
	 * 										   | foo.txt
	 * 										   | bar/
	 * 											     | foobar.sh	
	 * 												 | toto.pdf     (size 980o)
	 * 
	 * 		t.add("bar/toto.pdf", 103, n); => /
	 * 										   | foo.txt
	 * 										   | bar/
	 * 											     | foobar.sh	
	 * 												 | toto.pdf     (size 103o)
	 * @param str
	 * @param size
	 * @param lastModified
	 */
	private void add(String str, long size, long lastModified){
		// The files which we want to hide because their size is lower than the minSize 
		// given by the user will be stocker under that name
		// String other_name = "others-LT" + minSize;
		try{
		root.setSize(root.getSize()+size);
		Node current = root;
		String[] s = str.split("/");
		String path = "";

		/*
		 * For each parent node of the file, we create them (parent nodes are directory) if they have not been created
		 * yet. 
		 * Expected operation such as increasing the size of the parent directory according to the 
		 * file size are done here. 
		 * The last node, s[s.length - 1] is a file because only path of file are given to this method 
		 * (due to the fact that there is only the hdfs method listFiles() )
		 */
		for(int i = 0; i < s.length; i++){
			str = s[i];
			path = path+"/"+str;
			Node child = current.getChild(str);
			Node newChild = new Node(str, size, lastModified, path);
			newChild.setParent(current);
			if(i == s.length - 1) {       // newChild is a file
				if(size < this.minSize) {
					if(child != null) {   // file already exists
						current.getChildren().remove(child);
						addToOther(current, newChild);
					} else {
						addToOther(current, newChild);
					}
				} else {
					if(child != null) {
						current.getChildren().remove(child);
						current.getChildren().add(newChild);
					} else {
						current.getChildren().add(newChild);
					}
				}
				current = newChild;
			} else { // newChild is a directory 
				if(child == null){ // It does not exist yet
					current.getChildren().add(newChild);
					current = newChild;
				} else {
					current = child;
				}
			}

//			str = s[i];
//			path = path+"/"+str;
//			Node child = current.getChild(str);
//			Node t_child = child;
//			
//			// If the node is a file and already exist it is deleted and replaced by the new one
//			if(child != null && (i == s.length-1)) {
//				if(current.getChild(other_name) != null && child.getSize() < this.minSize)
//					current.getChild(other_name).setSize(current.getChild(other_name).getSize() - child.getSize());
//				current.getChildren().remove(child);
//				child = null;
//			}
//
//			// Node need to be created (file or directory)
//			if(child == null) {
//				// It is a file and its size is under the threshold
//				if((i == s.length-1) && (size < minSize)){
//					if(current.getChild(other_name) == null) { // create the "other" file label
//						current.getChildren().add(new Node(other_name, size, lastModified,path.substring(0, path.lastIndexOf("/")).substring(0, path.lastIndexOf("/"))+"/"+other_name));
//					} else { // add the file to the "other" file label
//						current.getChild(other_name).setSize(current.getChild(other_name).getSize() + size);
//						if(lastModified > current.getChild(other_name).getLastModified())
//							current.getChild(other_name).setLastModified(lastModified);
//					}
//					t_child = current.getChild(other_name);
//				}
//				else {
//					current.getChildren().add(new Node(str, size, lastModified, path));
//					t_child = current.getChild(str);
//				}
//				child = current.getChild(str);
//			}
//			if(t_child != null) { // Current node is being set its parent node
//				t_child.setParent(current);
//			}
//			// Updating the time of last modification
//			if((i == s.length-2) && (lastModified > current.getLastModified()))
//				current.setLastModified(lastModified);
//			current = child;
		}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	
	private void addToOther(Node current, Node child){
		if(otherNodes.get(child.getPath()) != null) { // child is already in "other" node
			if(current.getChild(otherName) != null)
				current.getChild(otherName).setSize(current.getChild(otherName).getSize() - otherNodes.get(child.getPath()) + child.getSize());
			else
				System.out.println("addToOther: Should not be happening.");
		} else { // we just need to update the size of other (the filesize may have changed since the last update)
			if(current.getChild(otherName) != null){
				current.getChild(otherName).setSize(current.getChild(otherName).getSize() + child.getSize());
			} else{
				current.getChildren().add(new Node(otherName, child.getSize(), child.getLastModified(), child.getPath()));
			}
			otherNodes.put(child.getPath(), child.getSize()); 
		}
	}
	
	/**
	 * Create the whole tree by adding all its files/not empty directories.
	 * Empty directory are not added because there is no hadoop method to recursively list everything
	 * @param minSize
	 * @param root
	 */
	public void init(int minSize, String root) throws HadoopConfException{
		try{
			this.setMinSize(minSize);
			RemoteIterator<LocatedFileStatus> it = hdfs.listFiles(new Path(root), true);
			LocatedFileStatus next;
			String path;
			long size;
			long lastModified;
			while(it.hasNext()) {
				next = it.next();
				path = next.getPath().toString();
				size = next.getLen();
				lastModified = next.getModificationTime();
				path = path.replace(hdfs.getConf().get("fs.defaultFS"), "");
				this.add(path, size, lastModified);
			}
			this.isInitilized = true;
		}
		catch(Exception e){
			throw new HadoopConfException();
		}
	}

	public Node getRoot() {
		return root;
	}

	public void setRoot(Node root) {
		this.root = root;
	}

	public int getMinSize() {
		return minSize;
	}

	public void setMinSize(int minSize) {
		this.minSize = minSize;
	}

	public boolean isInitilized() {
		return isInitilized;
	}

	public void setInitilized(boolean isInitilized) {
		this.isInitilized = isInitilized;
	}

	/**
	 * Update minSize when the user decide to do so in the front end interface.
	 * Since this trigger a lot of modification it just recreate the tree from scratch, maybe there is a more
	 * efficient way to update the tree when minSize is modified, you need to provide a different implementation then
	 * @param newMinSize
	 * @throws HadoopConfException
	 */
	private void updateMinSize(int newMinSize) throws HadoopConfException {
		try {
			String cf = System.getenv("HADOOP_CONF");
			Path p = new Path(cf);
			Configuration configuration = new Configuration(true);
			configuration.addResource(p);
			hdfs = FileSystem.get(configuration);
			root = new Node("/", 0, 0, "/");
			this.minSize = 0;
			this.isInitilized = false;
			this.otherNodes = new HashMap<String, Long>();
			this.minSize = newMinSize;
			init(minSize, "/");
		}
		catch(Exception e){
			throw new HadoopConfException();
		}
	}

/**
 * Ensures
 * 		- Update the tree with the new node(s) (file/directory) added since the last request by the user
 * 		- Update the tree without the deleted node(s) since the last request by the user
 * 		- Directory size are updated accordingly
 * 
 * This method allow an important time optimization when a user send a request with the same minSize.
 * It only modify the directories where files/directories have been added/deleted.
 * To know which directory to update since the last request we use the time of last modification.
 * However HDFS only update its time of last modification for the modified file/directory and its parent.
 * This is why this method is done recursively for each node in the tree.
 * 
 * @param directory
 */
	private void updateLastModified(Node directory) {
		try {
			long hdfsAccessTime = hdfs.getFileStatus(new Path(hdfs.getConf().get("fs.defaultFS")+directory.getPath())).getModificationTime();
			Path p = new Path(directory.getPath());
			FileStatus[] t = hdfs.listStatus(p);
			Boolean needUpdate = (directory.getLastModified() != hdfsAccessTime);
			// The current directory does not need to be updated but its children may, because of the way HDFS
			// set its lastModifiedTime : only the modified node and its parent have their modified time updated
			if(!needUpdate) { 
				for(int i = 0; i < t.length; i++) {
					if(t[i].isDirectory()) {
						String pathStr = t[i].getPath().toString();
						String childName = pathStr.substring(pathStr.lastIndexOf('/')+1);
						// recursively look for update for the current directory's children
						if(directory.getChild(childName) != null) {
							updateLastModified(directory.getChild(childName));
						}
						else{ 
							// Some old node have been deleted
							deleteOldElement(directory);
						}
					}
				}
			} else {
				directory.setLastModified(hdfsAccessTime);
				// Create the new node if new node have been created in the directory
				updateFilesInPath(p);
				// Delete old node if node have been deleted in the directory
				deleteOldElement(directory);
				for(int i = 0; i < t.length; i++) {
					if(t[i].isDirectory()) {
						String pathStr = t[i].getPath().toString();
						String childName = pathStr.substring(pathStr.lastIndexOf('/')+1);
						if(directory.getChild(childName) != null) { // if child directory already existed
							updateLastModified(directory.getChild(t[i].getPath().getName()));
						} else {
							// we need to append this subtree
							init(minSize, t[i].getPath().toString());
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

/**
 * Append new node if they have been created in the directory of path p since last user request of the hdfs tree
 * @param p
 */
	private void updateFilesInPath(Path p) {
		try {
			RemoteIterator<LocatedFileStatus> it = hdfs.listFiles(p, false);
			LocatedFileStatus next;
			String path;
			long size;
			long lastModified;
			while(it.hasNext()) {
				next = it.next();
				path = next.getPath().toString();
				size = next.getLen();
				lastModified = next.getModificationTime();
				path = path.replace(hdfs.getConf().get("fs.defaultFS"), "");
				this.add(path, size, lastModified);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

	/**
	 * Delete old node if they have been deleted in the directory of path p since last user request of the hdfs tree
	 * If deleting a file in a directory makes it empty, the directory is deleted
	 * This operation is done iteratively until it potentially reaches the root directory
	 * @param dir
	 */
	private void deleteOldElement(Node dir) {
		ArrayList<Node> children = dir.getChildren();
		ArrayList<Node> newChildren = new ArrayList<Node>();
		Node parent = dir.getParent();
		Node curent = dir;
		for(Node child : children) {
			try {
				if(hdfs.exists(new Path(child.getPath())) || child.getData().equals(otherName)) {
					newChildren.add(child);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		dir.setChildren(newChildren);
		
		// Check if a child who was in "other" was deleted
		for(Node child : children) {
			if(otherNodes.get(child.getPath()) != null) {
				try {
					if(!hdfs.exists(new Path(child.getPath())))
						dir.getChild(otherName).setSize(dir.getChild(otherName).getSize() - child.getSize());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		// Delete the directory if it is now empty due to the deletion of its only child
		// Do it for its parent if needed
		while(true) {
			if(curent != null && ! curent.equals(root) && (curent.getSize() == 0 || curent.getChildren().size() == 0)) {
				parent.getChildren().remove(curent);
				curent = parent;
				parent = curent.getParent();
			} else {
				break;
			}
		}
	}

	/**
	 * Update the tree since last request have been done
	 * @param minSize
	 */
	public void update(int minSize) throws HadoopConfException{
		if(minSize != this.getMinSize()) {
			try {
				updateMinSize(minSize);
			} catch (HadoopConfException e) {
				throw new HadoopConfException();
			}
		}
		updateLastModified(root);
	}

	/**
	 * Jsonify the tree so it can be used by the client javascript library d3.js. This is json is used there
	 * to rendeder the hdfs architecture
	 */
	public String getJson(){
		TreeTraverser<Node> traverser = new TreeTraverser<Node>() {
			@Override
			public Iterable<Node> children(Node root) {
				return root.getChildren();
			}
		};

		//this is the final json that is going to get returned
		JsonObject json_f = new JsonObject(); 
		//we store the json objects under their key so we can quickly access the parent of the current json
		Map <String, JsonObject> mapJson = new HashMap<String, JsonObject>();

		for (Node node : traverser.preOrderTraversal(root)) {
			JsonObject json = new JsonObject();
			String parentKey = node.getPath().substring(0, node.getPath().lastIndexOf("/"));

			JsonObject parentJson = json_f;
			if(parentKey != null && !parentKey.isEmpty()) {
				parentJson = mapJson.get(parentKey);
			}

			json.addProperty("name", node.getData());
			int nchild = node.getChildren().size();
			if(nchild > 0) { //node is a directory containing at least 1 child
				json.add("children",new JsonArray());
			} else {       //node is a file
				json.addProperty("size", node.getSize());	
			}

			if(parentJson != null) {
				try {
					parentJson.get("children").getAsJsonArray().add(json);
				} catch(Exception e) {
					json_f.addProperty("name", "/");
					json_f.add("children", new JsonArray());	
				}
			}
			mapJson.put(node.getPath(), json);
		}
		return json_f.toString();
	}
}