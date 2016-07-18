package hdfsv.model.hive;

import java.util.List;

import javax.enterprise.inject.Model;
import javax.inject.Named;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import hdfsv.exceptions.HadoopConfException;
import hdfsv.exceptions.HiveConfException;
@Model
@Named("standardHive")
public class HiveModelImpl implements HiveModelI{

	@Override
	public String getHiveDatabases() throws HadoopConfException, HiveConfException {
		JsonObject json = new JsonObject();
		try{
			//Setting HiveConf
			String hiveCf = System.getenv("HIVE_CONF");
			Path hivep = new Path(hiveCf);
			HiveConf hiveConf =  new HiveConf();
			hiveConf.addResource(hivep);
			HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);	
			try{
				//Setting HadoopConf
				String hdfsCf = System.getenv("HADOOP_CONF");
				Path hdfsp = new Path(hdfsCf);
				hiveConf.addResource(hdfsp);
				DistributedFileSystem hdfs = (DistributedFileSystem)DistributedFileSystem.get(hiveConf);
				try{
					json.add("dbs", new JsonArray());
					//getting all the databases
					List<String> dbs = client.getAllDatabases();
					JsonObject dbJson = null;
					for(String db:dbs){
						try{
							Database database = client.getDatabase(db);
							dbJson = new JsonObject();
							dbJson.addProperty("label", db);
							String location = database.getLocationUri().replace(hiveConf.get("fs.defaultFS"), "");
							if(!location.startsWith("/"))
								location = "/"+location;
							dbJson.addProperty("location", location);
							dbJson.addProperty("count", hdfs.getContentSummary(new Path(location)).getLength());
							dbJson.addProperty("isOk", 1);
							json.get("dbs").getAsJsonArray().add(dbJson);
						}
						catch(Exception e){
							dbJson.addProperty("label", db);
							dbJson.addProperty("isOk", 0);
							json.get("dbs").getAsJsonArray().add(dbJson);
						}
					}
				}
				catch(Exception e){
					System.out.println("no database");
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
				throw new HiveConfException();
		}
		return json.toString();
	}

	@Override
	public String getHiveTables(String database) throws HadoopConfException, HiveConfException{
		JsonObject json = new JsonObject();
		try{
			//Setting Hive conf
			String hiveCf = System.getenv("HIVE_CONF");
			Path hivep = new Path(hiveCf);
			HiveConf hiveConf =  new HiveConf();
			hiveConf.addResource(hivep);
			HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
			try{
				//Setting hdfs conf
				String hdfsCf = System.getenv("HADOOP_CONF");
				Path hdfsp = new Path(hdfsCf);
				hiveConf.addResource(hdfsp);
				DistributedFileSystem hdfs = (DistributedFileSystem)FileSystem.get(hiveConf);
				try{
					json.addProperty("database", database);
					json.add("tbls", new JsonArray());

					List<String> tables = client.getAllTables(database);
					Table table;
					String location;
					String type;
					long size;
					for(String tb:tables){
						JsonObject tmp = new JsonObject();
						try{
							table = client.getTable(database, tb);
							location = table.getSd().getLocation();
							type = table.getTableType();
							size = hdfs.getContentSummary(new Path(location)).getLength();
							location = location.replace(hiveConf.get("fs.defaultFS"), "");
							tmp.addProperty("label", tb);
							tmp.addProperty("location", location);
							tmp.addProperty("type", type);
							tmp.addProperty("count", size);
							tmp.addProperty("isOk", 1);
							json.get("tbls").getAsJsonArray().add(tmp);
						}
						catch(Exception e){
							System.out.println("hive can't find table"+tb);
							tmp.addProperty("label", tb);
							tmp.addProperty("isOk", 0);
							json.get("tbls").getAsJsonArray().add(tmp);
						}
					}
				}
				catch(Exception e){
					System.out.println("database : "+database+" is empty");
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
				throw new HiveConfException();
		}		
		return json.toString();
	}

}
