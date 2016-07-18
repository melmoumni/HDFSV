package hdfsv.model.hadoop;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import javax.enterprise.inject.Model;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import hdfsv.exceptions.HadoopConfException;

@Model
@Named("standardHadoop")
public class HadoopModelImpl implements HadoopModelI{


	private int indexInArray(JsonArray array, String id){
		for(int i = 0; i < array.size(); i++){
			JsonObject tmp = (JsonObject)array.get(i);
			if(tmp.get("name").getAsString().equals(id))
				return i;
		}
		return -1;
	}


	@Override
	public String getHadoopData(int mSize) throws HadoopConfException {
		JsonObject json_f = new JsonObject();
		try{
			TreeMap<String,Map<String, Long>> structure = new TreeMap<String, Map<String, Long>>();
			String cf = System.getenv("HADOOP_CONF");
			Path p = new Path(cf);
			Configuration configuration = new Configuration(true);
			configuration.addResource(p);
			FileSystem hdfs;
			hdfs = FileSystem.get(configuration);
			RemoteIterator<LocatedFileStatus> it = hdfs.listFiles(new Path("/"), true);
			LocatedFileStatus nxt;
			String path;
			String name;
			Long size;
			String parentPath;
			while(it.hasNext())
			{
				nxt = it.next();
				path = nxt.getPath().toString();
				name = nxt.getPath().getName();
				size = nxt.getLen();
				parentPath = path.substring(0, path.lastIndexOf("/"));
				parentPath = parentPath.replace(configuration.get("fs.defaultFS"), "");
				if(structure.get(parentPath) == null)
					structure.put(parentPath, new HashMap<String,Long>());
				structure.get(parentPath).put(name, size);
			}

			long minSize = (long) Math.pow(10, mSize);
			boolean first = true;
			JsonObject json = new JsonObject();

			json_f.addProperty("name", "/");
			json_f.add("children", new JsonArray());
			for(Map.Entry<String,Map<String,Long>> entry : structure.entrySet()) {
				String key = entry.getKey();
				String[] tokens = key.split("/");

				//create the root node
				if(json.get("name") == null || !json.get("name").getAsString().equals(tokens[0])) {
					if(!first)
						json_f.get("children").getAsJsonArray().add(json);
					else 
						first = false;
					json = new JsonObject();
					json.addProperty("name", tokens[0]);
					json.add("children", new JsonArray());
				}
				//the rest of the tree hierarchy without leaves (files)
				JsonObject next = json;
				for(int i = 1; i < tokens.length; i++ ){
					if(next.get("children") == null)
						next.add("children",new JsonArray());
					int index = indexInArray(next.get("children").getAsJsonArray(), tokens[i]);
					if(index == -1) {
						JsonObject tmp = new JsonObject();
						tmp.addProperty("name", tokens[i]);
						tmp.add("children", new JsonArray());
						next.get("children").getAsJsonArray().add(tmp);
						index = indexInArray(next.get("children").getAsJsonArray(), tokens[i]);
					}
					next = (JsonObject) next.get("children").getAsJsonArray().get(index);
				}

				//now adding the files
				long otherSize = 0;
				Map<String, Long> files = entry.getValue();
				for(Map.Entry<String,Long> file : files.entrySet()) {
					if(file.getValue() < minSize) {
						otherSize += file.getValue();
					} else {
						JsonObject tmp = new JsonObject();
						tmp.addProperty("name", file.getKey());
						tmp.addProperty("size", file.getValue());
						next.get("children").getAsJsonArray().add(tmp);
					}
				}
				if(otherSize > 0) {
					JsonObject tmp = new JsonObject();
					tmp.addProperty("name", "Other-LT"+minSize);
					tmp.addProperty("size", otherSize);
					next.get("children").getAsJsonArray().add(tmp);
				}
			}
			json_f.get("children").getAsJsonArray().add(json);
		}
		catch(Exception e ){
			throw new HadoopConfException();
		}
		return json_f.toString();
	}

	@Override
	public String getNodesData() throws HadoopConfException {
		JsonObject json = new JsonObject();
		JsonObject global = new JsonObject();
		try{	
			String cf = System.getenv("HADOOP_CONF");
			Path p = new Path(cf);
			Configuration configuration = new Configuration(true);
			configuration.addResource(p);
			DistributedFileSystem hdfs = (DistributedFileSystem)DistributedFileSystem.get(configuration);
			DatanodeInfo[] dataNodes = hdfs.getDataNodeStats(DatanodeReportType.ALL);
			json.add("summary", new JsonArray());
			global.addProperty("used", hdfs.getContentSummary(new Path(configuration.get("fs.defaultFS"))).getLength()/*getStatus().getUsed()*/);
			global.addProperty("unused", hdfs.getStatus().getRemaining());
			json.get("summary").getAsJsonArray().add(global);
			for(int i = 0; i < dataNodes.length; i++){
				JsonObject current = new JsonObject();
				current.addProperty("name", dataNodes[i].getName());
				current.addProperty("used", dataNodes[i].getDfsUsed());
				current.addProperty("unused", dataNodes[i].getCapacity());
				current.addProperty("percentage", dataNodes[i].getDfsUsedPercent());
				current.addProperty("nondfs", dataNodes[i].getNonDfsUsed());
				json.get("summary").getAsJsonArray().add(current);
			}
			json.addProperty("replication", hdfs.getFileStatus(new Path("/")).getReplication() + 1);

		}
		catch(Exception e){
			throw new HadoopConfException();
		}
		return json.toString();
	}
}
