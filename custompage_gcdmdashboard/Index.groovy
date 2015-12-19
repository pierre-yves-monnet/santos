import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.text.SimpleDateFormat;
import java.util.logging.Logger;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.Runtime;

import org.json.simple.JSONObject;
import org.codehaus.groovy.tools.shell.CommandAlias;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;


import javax.naming.Context;
import javax.naming.InitialContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import java.sql.DatabaseMetaData;

import org.apache.commons.lang3.StringEscapeUtils

import org.bonitasoft.engine.identity.User;
import org.bonitasoft.console.common.server.page.PageContext
import org.bonitasoft.console.common.server.page.PageController
import org.bonitasoft.console.common.server.page.PageResourceProvider
import org.bonitasoft.engine.exception.AlreadyExistsException;
import org.bonitasoft.engine.exception.BonitaHomeNotSetException;
import org.bonitasoft.engine.exception.CreationException;
import org.bonitasoft.engine.exception.DeletionException;
import org.bonitasoft.engine.exception.ServerAPIException;
import org.bonitasoft.engine.exception.UnknownAPITypeException;

import com.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.api.CommandAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.api.IdentityAPI;
import com.bonitasoft.engine.api.PlatformMonitoringAPI;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.ArchivedActivityInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.ActivityInstance;
import org.bonitasoft.engine.bpm.flownode.ArchivedFlowNodeInstance;
import org.bonitasoft.engine.bpm.flownode.ArchivedActivityInstance;
import org.bonitasoft.engine.search.SearchOptions;
import org.bonitasoft.engine.search.SearchResult;

import org.bonitasoft.engine.command.CommandDescriptor;
import org.bonitasoft.engine.command.CommandCriterion;
import org.bonitasoft.engine.bpm.flownode.ActivityInstance;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
	
	
import com.santos.gcdmaccess.GcdmAccess;

import com.santos.gcdmaccess.GcdmBusinessAccess.GasCompositionParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.NewGasCompositionParameter;

public class Index implements PageController {

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response, PageResourceProvider pageResourceProvider, PageContext pageContext) {
	
		Logger logger= Logger.getLogger("org.bonitasoft");
		
		HashMap<String,Object> result=null;

		def String indexContent;
		pageResourceProvider.getResourceAsStream("Index.groovy").withStream { InputStream s-> indexContent = s.getText() };
		response.setCharacterEncoding("UTF-8");
		PrintWriter out = response.getWriter()
		
		try {

			String action=request.getParameter("action");
			logger.info("###################################### action is["+action+"] V2!");
			if (action==null || action.length()==0 )
			{
				logger.severe("###################################### RUN Default !");
				
				runTheBonitaIndexDoGet( request, response,pageResourceProvider,pageContext);
				return;
			}
			
			APISession session = pageContext.getApiSession()
			ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(session);
			ProfileAPI profileAPI = TenantAPIAccessor.getProfileAPI(session);
			PlatformMonitoringAPI platformMonitoringAPI = TenantAPIAccessor.getPlatformMonitoringAPI(session);
			IdentityAPI identityAPI = TenantAPIAccessor.getIdentityAPI(session);
			
			if ("showgascomposition".equals(action))
			{
				String jsonSt =request.getParameter("json");
				GasCompositionParameter gasCompositionParameter = GasCompositionParameter.getFromJson( jsonSt );
				result = GcdmAccess.getListGasComposition(gasCompositionParameter, session, processAPI );
			}					
			else if ("deletegascomposition".equals(action))
			{
				String jsonSt =request.getParameter("json");
				GasCompositionParameter gasCompositionParameter = GasCompositionParameter.getFromJson( jsonSt );
				result = GcdmAccess.deleteListGasComposition(gasCompositionParameter, session, processAPI );
			}					
			else if ("searchnewgascomposition".equals(action))
			{
				String jsonSt =request.getParameter("json");
				NewGasCompositionParameter newGasCompositionParameter = NewGasCompositionParameter.getFromJson( jsonSt );
				result = GcdmAccess.searchListGasComposition(newGasCompositionParameter, session, processAPI );
			}		
            
            else if ("savenewgascomposition".equals(action))
            {
                String jsonSt =request.getParameter("json");
                NewGasCompositionParameter newGasCompositionParameter = NewGasCompositionParameter.getFromJson( jsonSt );
                result = GcdmAccess.addNewGasComposition(newGasCompositionParameter, session, processAPI );
            }
            
			
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionDetails = sw.toString();
			logger.severe("Exception ["+e.toString()+"] at "+exceptionDetails);
			result = new HashMap<String,Object>();
			result.put("ERRORMESSAGE", "Exception ["+e.toString()+"] at "+exceptionDetails);
		}
		try
		{		
			if (result!=null)
			{
				String jsonDetailsSt = JSONValue.toJSONString( result );
                logger.info("Return "+jsonDetailsSt);
				out.write( jsonDetailsSt );
				out.flush();
				out.close();				
				return;				
			}
			
			out.write( "Unknow command" );
			out.flush();
			out.close();
			return;
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionDetails = sw.toString();
			logger.severe("Exception ["+e.toString()+"] at "+exceptionDetails);
			
		}
	}

	
	/** -------------------------------------------------------------------------
	 *
	 *runTheBonitaIndexDoGet
	 * 
	 */
	private void runTheBonitaIndexDoGet(HttpServletRequest request, HttpServletResponse response, PageResourceProvider pageResourceProvider, PageContext pageContext) {
				try {
						def String indexContent;
						pageResourceProvider.getResourceAsStream("index.html").withStream { InputStream s->
								indexContent = s.getText()
						}
						
						def String pageResource="pageResource?&page="+ request.getParameter("page")+"&location=";
						
						indexContent= indexContent.replace("@_USER_LOCALE_@", request.getParameter("locale"));
						indexContent= indexContent.replace("@_PAGE_RESOURCE_@", pageResource);
						
						response.setCharacterEncoding("UTF-8");
						PrintWriter out = response.getWriter();
						out.print(indexContent);
						out.flush();
						out.close();
				} catch (Exception e) {
						e.printStackTrace();
				}
		}
		
		
}
