/**
 * @author Mohammed El Moumni, Vivien Achet
 * @deprecated servlet, to use only if Start servlet does not work
 * 		Start servlet uses Node and Tree analyser, so if those classes does not work, use this servlet coupled
 * 		with DFSAnalyser analyser.
 * 
 *  Utility: sens a json describing the cluster architecture
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
import hdfsv.model.hadoop.HadoopModelI;

/**
 * Servlet implementation class FileContent
 */
@WebServlet("/HadoopData")
public class HadoopData extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	@Inject @Named("treeImpl")
    private HadoopModelI model;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public HadoopData() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		int minSize = Integer.parseInt(request.getParameter("minSize"));
		minSize = (int)Math.pow(10, minSize);
		String json = "";
		try {
			json = model.getHadoopData(minSize);
			response.getWriter().print(json);
		} catch (HadoopConfException e) {
			String cf = System.getenv("HADOOP_CONF");
			response.sendError(1001, cf);
		}
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
