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
		}
		catch(HadoopConfException e){
			response.sendError(1001);
		}
		catch(HBaseConfException e){
			response.sendError(1002);
		}
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
