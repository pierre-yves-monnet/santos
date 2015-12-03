import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.text.SimpleDateFormat;
import java.util.logging.Logger;
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
	
import com.santos.sdeaccess.SdeAccess;
import com.santos.sdeaccess.SdeAccess.ListCasesParameter;
import com.santos.sdeaccess.SdeBusinessAccess.SystemSummaryParameter;
import com.santos.sdeaccess.SdeBusinessAccess.WellListParameter;
import com.santos.sdeaccess.SdeBusinessAccess.CreateWellParameter;
import com.santos.sdeaccess.SdeBusinessAccess.PADashboardParameter;
 
import com.santos.sdeaccess.SdeAccess.StartprocessParameter;
  
public class Index implements PageController {

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response, PageResourceProvider pageResourceProvider, PageContext pageContext) {
	
		Logger logger= Logger.getLogger("org.bonitasoft");
		Map<String,Object> result=null;
		PrintWriter out = response.getWriter()

		
		try {
			def String indexContent;
			pageResourceProvider.getResourceAsStream("Index.groovy").withStream { InputStream s-> indexContent = s.getText() };
			response.setCharacterEncoding("UTF-8");
	
			File pageDirectory = pageResourceProvider.getPageDirectory();
			String fileExternalProperties = pageDirectory.getPath()+"/externalservice.properties";
	
	
			String action=request.getParameter("action");
			logger.info("###################################### action is["+action+"] V4!");
			if (action==null || action.length()==0 )
			{
				logger.severe("RUN Default !");
				
				runTheBonitaIndexDoGet( request, response,pageResourceProvider,pageContext);
				return;
			}
			
			APISession session = pageContext.getApiSession()
			ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(session);
			ProfileAPI profileAPI = TenantAPIAccessor.getProfileAPI(session);
			PlatformMonitoringAPI platformMonitoringAPI = TenantAPIAccessor.getPlatformMonitoringAPI(session);
			IdentityAPI identityAPI = TenantAPIAccessor.getIdentityAPI(session);

			if ("init".equals(action))
			{
				result = SdeAccess.getProperties(fileExternalProperties);
				logger.info("GetProperties in file ["+fileExternalProperties+"] : "+result);
            	result.put("isAllowPropertiesView", SdeAccess.isAdminProfile( session.getUserId(), profileAPI));
			}
			else if ("getWellTrackerDashboardList".equals(action))
			{
				String json= request.getParameter("json");
				logger.info("SdeDashBoard.groovy: ParamJson ="+json);
				ListCasesParameter listCasesParameter = ListCasesParameter.getFromJson(json);
				result = SdeAccess.getWellTrackerDashboardList(listCasesParameter, session, processAPI, 
				identityAPI );
				logger.info("getListCasesForSdeDashboard:Result="+result.toString());
			}	
			else if ("getWellList".equals(action))
			{
				String json= request.getParameter("json");
				logger.info("SdeDashBoard.groovy: ParamJson ="+json);
				WellListParameter wellListParameter = WellListParameter.getFromJson(json);
				result = SdeAccess.getWellList(wellListParameter);
				logger.info("getWellList:Result="+result.toString());
			}	
			else if ("createWellList".equals(action))
			{
				String json= request.getParameter("json");
				logger.info("SdeDashBoard.groovy: ParamJson ="+json);
				CreateWellParameter createWellParameter = CreateWellParameter.getFromJson(json);
				result = SdeAccess.createWellList(createWellParameter);
				logger.info("getWellList:Result="+result.toString());
			}	
			
			else if("startSdeRequest".equals(action))
			{
				String json= request.getParameter("json");
				logger.info("SdeDashBoard.groovy: ParamJson ="+json);
				StartprocessParameter startprocessParameter = StartprocessParameter.getFromJson(json);
				result = SdeAccess.startProcessParameter(startprocessParameter, session, processAPI, identityAPI );
				logger.info("startProcessParameter:Result="+result.toString());
			}
			else if ("getListSystemSummary".equals(action))
			{
				String json= request.getParameter("json");
				logger.info("SdeDashBoard.groovy: ParamJson ="+json);
				SystemSummaryParameter systemSummaryParameter = SystemSummaryParameter.getFromJson(json);
				result = SdeAccess.getListSystemSummary(systemSummaryParameter, session, processAPI );
				logger.info("getListCasesForSdeDashboard:Result="+result.toString());
			}	
			else if ("getPADashboard".equals(action))
			{
				String json= request.getParameter("json");
				logger.info("getPADashboard.groovy: ParamJson ="+json);
				PADashboardParameter paDashboardParameter = PADashboardParameter.getFromJson(json);
				result = SdeAccess.getListPADAshboard(paDashboardParameter, session, processAPI );
				logger.info("PADashboardParameter:Result="+result.toString());
		
			}
			else if ("getproperties".equals(action))
			{
			  result = SdeAccess.getProperties(fileExternalProperties);
            }
			else if ("setproperties".equals(action))
			{
			  String json= request.getParameter("json");
			  result = new HashMap<String,Object>();
			  result.put("status",SdeAccess.setProperties(fileExternalProperties,json));
            }
				 
			if (result!=null)
			{
				String jsonDetailsSt = JSONValue.toJSONString( result );
	   
				out.write( jsonDetailsSt );
				out.flush();
				out.close();				
				return;				
			}
			logger.severe("Unknow command ["+action+"]");
			
			out.write( "Unknow command" );
			out.flush();
			out.close();
			return;
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionDetails = sw.toString();
			logger.severe("SDEDashBoard.Groovy : Exception ["+e.toString()+"] at "+exceptionDetails);
			
			result = new HashMap<String,Object>();
			
			result.put("error", "Exception ["+e.toString()+"] at "+exceptionDetails);
			String jsonDetailsSt = JSONValue.toJSONString( result );
			out.write( jsonDetailsSt );
			out.flush();
			out.close();	
			logger.severe("SDEDashBoard.Groovy : End flush json["+jsonDetailsSt+"]");
			
			return;				
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
