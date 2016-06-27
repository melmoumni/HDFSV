/**
 * @author Mohammed El Moumni, Vivien Achet
 */

package hdfsv.model.hadoop;

import hdfsv.exceptions.HadoopConfException;
/**
 * 
 * What this interface represent:
 * 		It is an interface for the class Tree, which is the whole architecture linked to a hdfs configuration file
 *
 */
public interface TreeI {
	public boolean isEmpty();
	public void init(int minSize, String root) throws HadoopConfException;
	public Node getRoot();
	public void setRoot(Node root) ;
	public int getMinSize();
	public void setMinSize(int minSize);
	public boolean isInitilized();
	public void setInitilized(boolean isInitilized);
	public void update(int minSize) throws HadoopConfException;
	public String getJson();		
}
