package hdfsv.model.hbase;

import javax.enterprise.inject.Model;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import hdfsv.exceptions.HBaseConfException;
import hdfsv.exceptions.HadoopConfException;
@Model
@Named("standardHBase")
public class HBaseModelImpl implements HBaseModelI{

	@Override
	public String getHBaseTbales() throws HadoopConfException, HBaseConfException {
		JsonObject json = new JsonObject();
		json.add("tbls", new JsonArray());
		try{
			//Setting hBase conf
			String hbaseCf = System.getenv("HBASE_CONF");
			Path hbasep = new Path(hbaseCf);
			Configuration hbaseConf = new HBaseConfiguration();
			hbaseConf.addResource(hbasep);
			HBaseAdmin.checkHBaseAvailable(hbaseConf);
			HBaseAdmin admin = new HBaseAdmin(hbaseConf);
			HTableDescriptor[] tablesDescriptor = admin.listTables();
			try{
				//Setting Hadoop Conf
				String hdfsCf = System.getenv("HADOOP_CONF");
				Path hdfsp = new Path(hdfsCf);
				hbaseConf.addResource(hdfsp);
				DistributedFileSystem hdfs = null;
				FileSystem fs = FileSystem.get(hbaseConf);
				hdfs = (DistributedFileSystem) fs;
				hdfs.setConf(hbaseConf);
				String name;
				String location;
				long size;
				for(int i = 0; i < tablesDescriptor.length; i++){
					JsonObject tmp = new JsonObject();
					name = tablesDescriptor[i].getNameAsString();			
					location = hbaseConf.get("hbase.rootdir").replace(hbaseConf.get("fs.defaultFS"), "");
					if(!location.startsWith("/"))
						location = "/"+location;
					if(!location.endsWith("/"))
						location = location+"/";
					location = location+"data/"+tablesDescriptor[i].getTableName().getNamespaceAsString()+"/"+name;
					try{
						size = hdfs.getContentSummary(new Path(location)).getLength();
						tmp.addProperty("name", name);
						tmp.addProperty("location", location);
						tmp.addProperty("size", size);
						tmp.addProperty("isOk", 1);
					}
					catch(Exception e){
						tmp.addProperty("name", name);
						tmp.addProperty("location", location);
						tmp.addProperty("isOk", 0);
					}

					json.get("tbls").getAsJsonArray().add(tmp);
				}
			}
			catch(Exception e){
				throw new HadoopConfException();
			}
		}
		catch(Exception e){
			if(e instanceof HadoopConfException)
				throw new HadoopConfException();
			else
				throw new HBaseConfException();
		}
		return json.toString();
	}


}
