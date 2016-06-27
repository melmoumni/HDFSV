package hdfsv.model.hadoop;

import hdfsv.exceptions.HadoopConfException;

public interface HadoopModelI {
	public String getHadoopData(int minSize) throws HadoopConfException;
	public String getNodesData() throws HadoopConfException;
}
