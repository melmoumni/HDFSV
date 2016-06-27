/**
 * @author Mohammed El Moumni, Vivien Achet
 * 
 * Utility: send a json describing the hive databases. 
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
import hdfsv.exceptions.HadoopConfException;
import hdfsv.exceptions.HiveConfException;
import hdfsv.model.hive.HiveModelI;

/**
 * Servlet implementation class Databases
 * 
 * Get the Hive databases
 * 
 */
@WebServlet("/HiveDatabases")
public class HiveDatabases extends HttpServlet {
	private static final long serialVersionUID = 1L;
    @Inject @Named("standardHive")
	private HiveModelI model;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public HiveDatabases() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String json = "";
		try {
			json = model.getHiveDatabases();
			response.getWriter().print(json);
		} catch (HiveConfException e) {
			System.out.println("hive servlet");
			response.sendError(1000);
		} catch (HadoopConfException e) {
			System.out.println("hadoop servlet");
			response.sendError(1001);
		} 
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
