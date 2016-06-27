/**
 * @author Mohammed El Moumni, Vivien Achet
 * 
 * Utility: send a json describing the hive tables. 
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
 * Servlet implementation class Tables
 */
@WebServlet("/HiveTables")
public class HiveTables extends HttpServlet {
	private static final long serialVersionUID = 1L;
    @Inject @Named("standardHive")
	private HiveModelI model;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public HiveTables() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String database = request.getParameter("database");
		String json = "";
		try {
			json = model.getHiveTables(database);
			response.getWriter().print(json);
		}  catch (HadoopConfException e) {
			response.sendError(1001);
		} catch (HiveConfException e) {
			response.sendError(1000);
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
