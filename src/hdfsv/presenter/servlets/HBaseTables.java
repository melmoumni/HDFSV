/**
 * @author Mohammed El Moumni, Vivien Achet
 * 
 * Utility: send a json describing the hbase tables. 
 */

package hdfsv.presenter.servlets;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import hdfsv.exceptions.HBaseConfException;
import hdfsv.exceptions.HadoopConfException;
import hdfsv.exceptions.HiveConfException;
import hdfsv.model.hbase.HBaseModelI;

/**
 * Servlet implementation class HbaseTables
 */
@WebServlet("/HBaseTables")
public class HBaseTables extends HttpServlet {
	private static final long serialVersionUID = 1L;
    @Inject @Named("standardHBase")
	private HBaseModelI model;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public HBaseTables() {
        super();
    }

	/**
	 * @throws IOException 
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
		String json = "";
		try{
			json = model.getHBaseTbales();
			response.getWriter().print(json);
		}  catch (HadoopConfException e) {
			String cf = System.getenv("HADOOP_CONF");
			response.sendError(404, "An error occured while trying to access Hadoop Metadata. HADOOP_CONF is set to this path : "+cf+". Pleas check that this path is correct");
		} catch (HBaseConfException e) {
			String cf = System.getenv("HBASE_CONF");
			response.sendError(404, "An error occured while trying to access HBase Metadata. HBASE_CONF is set to this path : "+cf+". Pleas check that this path is correct");		
		}
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
