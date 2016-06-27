package hdfsv.model.hbase;

import hdfsv.exceptions.HBaseConfException;
import hdfsv.exceptions.HadoopConfException;

public interface HBaseModelI {
	public String getHBaseTbales() throws HadoopConfException, HBaseConfException;
}
