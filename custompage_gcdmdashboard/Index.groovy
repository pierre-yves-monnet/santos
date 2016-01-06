import com.santos.gcdmaccess.GcdmToolbox;
import com.santos.gcdmaccess.GcdmPressureAccess.PressureParameter;

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
import com.santos.gcdmaccess.GcdmPressureAccess;
import com.santos.gcdmaccess.GcdmPressureAccess.NewPressureParameter;

import com.santos.gcdmaccess.GcdmBusinessAccess.CalculGravityParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.GasCompositionParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.NewGasCompositionParameter;

import com.santos.gcdmaccess.GcdmReport;
import com.santos.gcdmaccess.GcdmReport.ReportParameter;

import com.santos.gcdmaccess.GcdmAdminAccess;
import com.santos.gcdmaccess.GcdmAdminAccess.AdminParameter;


public class Index implements PageController {

	
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
			
			APISession apiSession = pageContext.getApiSession()
			ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);
			ProfileAPI profileAPI = TenantAPIAccessor.getProfileAPI(apiSession);
			PlatformMonitoringAPI platformMonitoringAPI = TenantAPIAccessor.getPlatformMonitoringAPI(apiSession);
			IdentityAPI identityAPI = TenantAPIAccessor.getIdentityAPI(apiSession);
			
			if ("showgascomposition".equals(action))
			{
				String jsonSt =request.getParameter("json");
				GasCompositionParameter gasCompositionParameter = GasCompositionParameter.getFromJson( jsonSt );
				result = GcdmAccess.getListGasComposition(gasCompositionParameter, apiSession  ).toMap();
			}		
            else if ("editgascomposition".equals(action))
            {
                String jsonSt =request.getParameter("json");
                GasCompositionParameter gasCompositionParameter = GasCompositionParameter.getFromJson( jsonSt );
                result = GcdmAccess.getGasComposition(gasCompositionParameter, apiSession ).toMap();
            }       
            
			else if ("deletegascomposition".equals(action))
			{
				String jsonSt =request.getParameter("json");
				GasCompositionParameter gasCompositionParameter = GasCompositionParameter.getFromJson( jsonSt );
				result = GcdmAccess.deleteListGasComposition(gasCompositionParameter, apiSession ).toMap();
			}		            
            else if ("defaultaddgascomposition".equals(action))
            {
                String jsonSt =request.getParameter("json");
                GasCompositionParameter gasCompositionParameter = GasCompositionParameter.getFromJson( jsonSt );
                result = GcdmAccess.getDefaultGasComposition(gasCompositionParameter, apiSession ).toMap();
            }
			else if ("searchnewgascomposition".equals(action))
			{
				String jsonSt =request.getParameter("json");
				NewGasCompositionParameter newGasCompositionParameter = NewGasCompositionParameter.getFromJson( jsonSt );
				result = GcdmAccess.searchListGasComposition(newGasCompositionParameter, apiSession );
			}		
            
         
            else if ("save".equals(action))
            {
                String jsonSt =request.getParameter("json");
                String type =request.getParameter("type");
                logger.info("Save type["+type+"]");
                if ("gascomposition".equals(type))
                {
                    NewGasCompositionParameter newGasCompositionParameter = NewGasCompositionParameter.getFromJson( jsonSt );
                    result = GcdmAccess.addNewGasComposition(newGasCompositionParameter, apiSession );
                } else if ("pressure".equals(type))
                {
                    NewPressureParameter newGasCompositionParameter = NewPressureParameter.getFromJson( jsonSt );
                    result = GcdmPressureAccess.addNewPressure(newGasCompositionParameter, apiSession ).toMap();
             
                } else if ("supplychainpoint".equals(type))
                {
                    AdminParameter adminParameter = AdminParameter.getFromJson( jsonSt );
                    result = GcdmAdminAccess.addNewSupplyChainPoint(adminParameter, apiSession ).toMap();
                }
                else
                {
                    result = new HashMap<String,Object>();
                    result.put("ERRORMESSAGE", "action[save] unknow type["+type+"]");
    
                }
            }
            else if ("calculategascomposition".equals(action))
            {
                String jsonSt =request.getParameter("json");
                 CalculGravityParameter calculGravityParameter = CalculGravityParameter.getFromJson(jsonSt);
                 result = GcdmAccess.calculateGravityAndHeating(calculGravityParameter, apiSession ).toMap();
                 
            }
            else if ("showpressure".equals(action))
            {
                String jsonSt =request.getParameter("json");
                PressureParameter pressureParameter = PressureParameter.getFromJson( jsonSt );
                result = GcdmPressureAccess.getListPressure(pressureParameter,apiSession ).toMap();

            }
            else if ("defaultaddpressure".equals(action))
            {
                String jsonSt =request.getParameter("json");
                NewPressureParameter pressureParameter = NewPressureParameter.getFromJson( jsonSt );
                result = GcdmPressureAccess.getDefaultPressure(pressureParameter, apiSession ).toMap();
      
            }
            else if ("editpressure".equals(action))
            {
                String jsonSt =request.getParameter("json");
                PressureParameter pressureParameter = PressureParameter.getFromJson( jsonSt );
                result = GcdmPressureAccess.getPressure(pressureParameter, apiSession ).toMap();
            }            
            else if ("deletepressure".equals(action))
            {
                String jsonSt =request.getParameter("json");
                PressureParameter pressureParameter = PressureParameter.getFromJson( jsonSt );
                result = GcdmPressureAccess.deleteListPressure(pressureParameter, apiSession ).toMap();
            }
            else if ("reportinfo".equals(action))
            {
                String jsonSt =request.getParameter("json");
                ReportParameter reportParameter = ReportParameter.getFromJson( jsonSt );
                result = GcdmReport.report( reportParameter, apiSession ).toMap();
            }
            else if ("getlist".equals(action))
            {
                String jsonSt =request.getParameter("json");
                AdminParameter adminParameter = AdminParameter.getFromJson( jsonSt );
                result = GcdmAdminAccess.getAdminList(adminParameter, apiSession ).toMap();
            }
             else if ("getdefaultadd".equals(action))
            {
                String jsonSt =request.getParameter("json");
                AdminParameter adminParameter = AdminParameter.getFromJson( jsonSt );
                result = GcdmAdminAccess.getAdminDefaultAdd(adminParameter, apiSession ).toMap();
            }
            else
            {
                result = new HashMap<String,Object>();
                result.put("ERRORMESSAGE", "Unknow action["+action+"]");
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
