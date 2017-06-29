package hdfsv.model.hadoop;

import java.util.logging.Logger;

import hdfsv.exceptions.HadoopConfException;

public interface HadoopModelI {
    Logger logger = Logger.getLogger("HadoopModel");  
    String logPath = " ~/HDFSV/hdfvs.log";

	public String getHadoopData(int minSize) throws HadoopConfException;
	public String getNodesData() throws HadoopConfException;
}
