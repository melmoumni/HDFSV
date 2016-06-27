package hdfsv.model.hive;

import hdfsv.exceptions.HadoopConfException;
import hdfsv.exceptions.HiveConfException;

public interface HiveModelI {
	public String getHiveDatabases() throws HadoopConfException, HiveConfException;
	public String getHiveTables(String database) throws HadoopConfException, HiveConfException;
}
