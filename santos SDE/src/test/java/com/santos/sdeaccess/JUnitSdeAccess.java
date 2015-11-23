package com.santos.sdeaccess;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ApiAccessType;
import org.bonitasoft.engine.api.LoginAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.exception.BonitaHomeNotSetException;
import org.bonitasoft.engine.exception.ServerAPIException;
import org.bonitasoft.engine.exception.UnknownAPITypeException;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.engine.util.APITypeManager;
import org.junit.Test;

import com.santos.sdeaccess.SdeAccess.ListCasesParameter;
import com.santos.sdeaccess.SdeAccess.StartprocessParameter;
import com.santos.sdeaccess.SdeBusinessAccess.CreateWellParameter;
import com.santos.sdeaccess.SdeBusinessAccess.SdeData;
import com.santos.sdeaccess.SdeBusinessAccess.SystemSummaryParameter;
import com.santos.sdeaccess.SdeBusinessAccess.TableDashBoard;
import com.santos.sdeaccess.SdeBusinessAccess.WellListParameter;

public class JUnitSdeAccess {

    private static Logger logger = Logger.getLogger("org.bonitasoft.SdeAccess");

    //@Test
    public void testIdAdmin()
    {
        final APISession apiSession = getLogin();
        boolean isAdmin;
        try {
            isAdmin = SdeAccess.isAdminProfile(apiSession.getUserId(), TenantAPIAccessor.getProfileAPI(apiSession));
            System.out.println("IsAdmin=" + isAdmin);
        } catch (final BonitaHomeNotSetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (final ServerAPIException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (final UnknownAPITypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    //@Test
    public void testListCaseForSdeDashboard() {
        try
        {
            final APISession apiSession = getLogin();
            final ListCasesParameter listCasesParameter = new ListCasesParameter();
            listCasesParameter.sourceIsDatabase = true;
            listCasesParameter.sdeParameter.allowDirectConnection = true;
            listCasesParameter.sdeParameter.scheduledOnlineDateInFutur = true;

            Map<String, Object> result = SdeAccess.getWellTrackerDashboardList(listCasesParameter, apiSession,
                    TenantAPIAccessor.getProcessAPI(apiSession),
                    TenantAPIAccessor.getIdentityAPI(apiSession));
            System.out.print("Result=" + result);

            listCasesParameter.sdeParameter.scheduledOnlineDateInFutur = false;
            result = SdeAccess.getWellTrackerDashboardList(listCasesParameter, apiSession,
                    TenantAPIAccessor.getProcessAPI(apiSession),
                    TenantAPIAccessor.getIdentityAPI(apiSession));
            System.out.print("Result=" + result);

        } catch (final Exception e)
        {
            logger.severe("Exception =" + e.toString());
        }

    }

    //@Test
    public void testListSystemSummary() {
        try
        {
            final APISession apiSession = getLogin();
            final SystemSummaryParameter systemSummaryParameter = new SystemSummaryParameter();
            systemSummaryParameter.allowDirectConnection = true;

            final Map<String, Object> result = SdeAccess.getListSystemSummary(systemSummaryParameter, apiSession,
                    TenantAPIAccessor.getProcessAPI(apiSession));
            System.out.print("Result=" + result);


        } catch (final Exception e)
        {
            logger.severe("Exception =" + e.toString());
        }

    }

    //@Test
    public void testRList() {
        try
        {
            final WellListParameter listCasesParameter = new WellListParameter();

            listCasesParameter.allowDirectConnection = true;
            listCasesParameter.filterUWI = "34";
            listCasesParameter.filterWellFullName = "FULLNAME";

            final Map<String, Object> result = SdeAccess.getWellList(listCasesParameter);
            System.out.print("Result=" + result);
        } catch (final Exception e)
        {
            System.out.print("Exception =" + e.toString());
        }

    }

    @Test
    public void testReadDatabase()
    {
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();
        final SdeBusinessAccess.SdeParameter sdeParameter = new SdeBusinessAccess.SdeParameter();
        sdeParameter.allowDirectConnection = true;
        final SdeData sdeData = sdeBusinessAccess.readSdeData(19252L, 1L, sdeParameter);
        System.out.println("Data =" + sdeData.data.toString());
        System.out.println("DataJson =" + sdeData.getJsonFormat());

        final SdeData sdeDataNotFound = sdeBusinessAccess.readSdeData(12L, 1L, sdeParameter);
        System.out.println("Data =" + sdeDataNotFound.data.toString());
        System.out.println("DataJson =" + sdeDataNotFound.getJsonFormat());
    }

    // @Test
    public void testWriteDatabase()
    {
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();
        final SdeBusinessAccess.SdeParameter sdeParameter = new SdeBusinessAccess.SdeParameter();
        final SdeBusinessAccess.DataModel dataModel = sdeBusinessAccess.getDataModel();
        final SdeData sdeData = sdeBusinessAccess.getADummyData();

        System.out.println("PopulateSimulate DataJson =" + sdeData.getJsonFormat());
        System.out.println("PopulateSimulate Data =" + sdeData.data.toString());
        sdeParameter.allowDirectConnection = true;
        sdeBusinessAccess.writeSdeData(sdeData, sdeParameter);

        logger.info("Read the same data ");
        final Map<String, Object> dashBoard = (Map<String, Object>) sdeData.data.get("dashboard");
        final SdeData sdeDataRead = sdeBusinessAccess.readSdeData((Long) dashBoard.get(TableDashBoard.SDE_NUMBER),
                (Long) dashBoard.get(TableDashBoard.SDE_STATUS), sdeParameter);
        logger.info("DataJsonWrite =" + sdeData.getJsonFormat());
        logger.info("DataJsonRead  =" + sdeDataRead.getJsonFormat());

    }

    @Test
    public void testUpdateSubmit()
    {
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();
        final SdeBusinessAccess.SdeParameter sdeParameter = new SdeBusinessAccess.SdeParameter();
        sdeParameter.allowDirectConnection = true;

        sdeBusinessAccess.updateSubmitStatus(98L, 9L, "Y", sdeParameter);
        sdeBusinessAccess.updateSubmitStatus(98L, 9L, "N", sdeParameter);

    }
    // @ T e s  t
    public void testGetFromHashMap()
    {
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();
        SdeData sdeData = sdeBusinessAccess.getADummyData();

        logger.info("DataReference       =" + sdeData.getJsonFormat());

        final HashMap<String, Object> jsonValue = new HashMap<String, Object>();
        jsonValue.put("sdeData", sdeData.data);
        jsonValue.put("sdeLists", sdeData.listsValue);
        sdeData = SdeData.getInstanceFromMap(jsonValue);
        logger.info("GetFrom jsonValue   =" + sdeData.getJsonFormat());

        final HashMap<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(TableDashBoard.SDE_NUMBER, 124);
        sdeData = SdeData.getInstanceFromMap(attributes);
        logger.info("GetFrom Attributes  =" + sdeData.getJsonFormat());

        final HashMap<String, Object> dashBoard = new HashMap<String, Object>();
        dashBoard.put("dashboard", attributes);
        attributes.put(TableDashBoard.SDE_NUMBER, 125);
        sdeData = SdeData.getInstanceFromMap(dashBoard);
        logger.info("GetFrom Attributes  =" + sdeData.getJsonFormat());

    }

    @Test
    public void testStartProcess()
    {
        try
        {
            final APISession apiSession = getLogin();
            final StartprocessParameter startprocessParameter = new StartprocessParameter();
            startprocessParameter.processName = "SDEDemo";
            startprocessParameter.processVariables.put("ctrSdeNumber", 98);
            startprocessParameter.processVariables.put("ctrSdeStatus", 9);
            startprocessParameter.waitFirstTask = true;
            startprocessParameter.timeToWaitInMs = 3000;

            final Map<String, Object> result = SdeAccess.startProcessParameter(startprocessParameter, apiSession,
                    TenantAPIAccessor.getProcessAPI(apiSession),
                    TenantAPIAccessor.getIdentityAPI(apiSession));
            System.out.print("Result=" + result);
        } catch (final Exception e)
        {
            System.out.print("Exception =" + e.toString());
        }

    }

    @Test
    public void testCreateWell()
    {
        final CreateWellParameter createWellParameter = new CreateWellParameter();
        createWellParameter.businessUnit = "TEST";
        createWellParameter.fieldName = "MyField";
        createWellParameter.uwi = String.valueOf(System.currentTimeMillis() % 10000);
        createWellParameter.wellFullName = "WellFullName";

        final Map<String, Object> result = SdeAccess.createWellList(createWellParameter);
        System.out.print("Result = " + result.toString());

    }
    private APISession getLogin()
    {
        final Map<String, String> map = new HashMap<String, String>();
        map.put("server.url", "http://localhost:8080");
        map.put("application.name", "bonita");
        APITypeManager.setAPITypeAndParams(ApiAccessType.HTTP, map);

        // Set the username and password
        // final String username = "helen.kelly";
        final String username = "walter.bates";
        final String password = "bpm";

        // get the LoginAPI using the TenantAPIAccessor
        LoginAPI loginAPI;
        try {
            loginAPI = TenantAPIAccessor.getLoginAPI();
            // log in to the tenant to create a session
            return loginAPI.login(username, password);
        } catch (final Exception e)
        {
            return null;
        }
    }

}
