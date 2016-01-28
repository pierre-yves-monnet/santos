package com.santos.gcdmaccess;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.exception.BonitaHomeNotSetException;
import org.bonitasoft.engine.exception.ServerAPIException;
import org.bonitasoft.engine.exception.UnknownAPITypeException;
import org.bonitasoft.engine.session.APISession;
import org.json.simple.JSONValue;

import com.santos.gcdmaccess.GcdmToolbox.GcdmResult;
import com.santos.gcdmaccess.GcdmToolbox.GcdmResult.typeColumn;
import com.santos.toolbox.Toolbox;

public class GcdmAdminAccess {

    static Logger logger = Logger.getLogger("org.bonitasoft.GcdmAdminAccess");

    /* ******************************************************************************** */
    /*                                                                                  */
    /* AdminParameter Pressure */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class AdminParameter
    {

        public boolean allowDirectConnection = false;

        public boolean formatDateJson = true;
        public int maxRecord = 1000;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public String typedata;
        public Long duid;
        public List<String> listToDelete = null;

        public static AdminParameter getFromJson(final String jsonSt)
        {
            final AdminParameter pressureParameter = new AdminParameter();
            if (jsonSt == null) {
                return pressureParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return pressureParameter;
            }
            pressureParameter.typedata = (String) jsonHash.get("typedata");
            pressureParameter.listToDelete = (List) jsonHash.get("listtodelete");
            pressureParameter.duid = Toolbox.getLong(jsonHash.get("DUID"), null);

            return pressureParameter;
        }

    };

    public static GcdmToolbox.GcdmResult getAdminList(final AdminParameter adminParameter,
            final APISession apiSession)
    {
        if ("supplychainpoint".equals(adminParameter.typedata)) {
            return getListSupplyChainPoint(adminParameter, apiSession);
        }
        final GcdmToolbox.GcdmResult result = new GcdmToolbox.GcdmResult();

        result.errorstatus = "Unknow type [" + adminParameter.typedata + "]";
        return result;

    }

    public static GcdmToolbox.GcdmResult getAdminDefaultAdd(final AdminParameter adminParameter,
            final APISession apiSession)
    {
        if ("supplychainpoint".equals(adminParameter.typedata)) {
            return getDefaultAddSupplyChainPoint(adminParameter, apiSession);
        }
        final GcdmToolbox.GcdmResult result = new GcdmToolbox.GcdmResult();

        result.errorstatus = "Unknow type [" + adminParameter.typedata + "]";
        return result;

    }

    /** get the list of the GasComposition FDR-64 */
    public static GcdmToolbox.GcdmResult getListSupplyChainPoint(final AdminParameter adminParameter,
            final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        final Connection con = GcdmToolbox.getConnection(adminParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        String sqlRequest = "";
        try
        {

            // FDR-88 must provide:
            // list of value
            // list of header
            // list of field to edit
            sqlRequest = " select dp.dpt_uid_pk as DUID, "
                    + "dp.effectivedate as \"EFFECTIVEDATE\", "
                    + "dp.pointname as \"SUPPLYCHAINEPOINT\"  "
                    + " from DataPoints dp"
                    + " order by dp.pointname";

            final List<Object> listRequestObject = new ArrayList<Object>();

            gcdmResult.listValues = GcdmToolbox.executeRequest(con, sqlRequest, listRequestObject, adminParameter.maxRecord, adminParameter.formatDateJson);

            // FDR-67 create the header
            completeListSupplyChainPointHeader(gcdmResult);

            // get the NewGasCompositionFields
            completeListSupplyChainPointFields(gcdmResult);

            gcdmResult.status = gcdmResult.listValues.size() + " item+" + (gcdmResult.listValues.size() > 1 ? "s" : "") + " found";

            final IdentityAPI identityAPI = TenantAPIAccessor.getIdentityAPI(apiSession);
            final ProfileAPI profileAPI = TenantAPIAccessor.getProfileAPI(apiSession);

            gcdmResult.isEditProfile = GcdmToolbox.isEditGcdmProfile(apiSession.getUserId(), profileAPI, identityAPI);

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmAdminAccess: Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };
        return gcdmResult;
    }

    /** get the list of the GasComposition FDR-64 */
    public static GcdmToolbox.GcdmResult getDefaultAddSupplyChainPoint(final AdminParameter adminParameter,
            final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        final Connection con = GcdmToolbox.getConnection(adminParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        final String sqlRequest = "";
        try
        {

            gcdmResult.values.put("PERMITBLENDDEFAULT", true);

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmAdminAccess: Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };

        return gcdmResult;
    }

    public static class NewAdminParameter
    {

        public Map<String, Object> allValues = null;
        public String errormessage;
        public boolean allowDirectConnection = false;

        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static NewAdminParameter getFromJson(final String jsonSt)
        {

            final NewAdminParameter adminParameter = new NewAdminParameter();
            if (jsonSt == null) {
                return adminParameter;
            }
            try
            {
                final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
                if (jsonHash == null) {
                    return adminParameter;
                }

                adminParameter.allValues = jsonHash;
            } catch (final Exception e) {
                logger.severe("Santos.GcdmAdminAccess: adminParameter.getFromJson : exception e " + e.toString() + " from parameter [" + jsonSt + "]");
                adminParameter.allValues = new HashMap<String, Object>();
            }
            return adminParameter;

        }

    };

    /**
     * get one specific SupplyChainPoint - FDR-88
     * 
     * @param adminParameter
     * @param apiSession
     * @return
     */
    public static GcdmToolbox.GcdmResult getSupplyChainPoint(final AdminParameter adminParameter,
            final APISession apiSession)
    {
        logger.info("Santos.GcdmAdminAccess.getSupplyChainPoint: parameters=" + adminParameter.toString());
        final GcdmResult gcdmResult = new GcdmResult();
        final Connection con = GcdmToolbox.getConnection(adminParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        final String sqlRequest = "";
        try
        {

            final Calendar c = Calendar.getInstance();
            c.set(Calendar.YEAR, 2015);
            c.set(Calendar.MONTH, 11);

            // simumationd

            gcdmResult.values.put("PERMITBLENDDEFAULT", adminParameter.duid + 23);

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmAdminAccess: Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };
        return gcdmResult;
    }

    // FDR-88 Add new suuply chain
    public static GcdmToolbox.GcdmResult addNewSupplyChainPoint(final NewAdminParameter adminParameter, final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        final Connection con = GcdmToolbox.getConnection(adminParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        final String sqlRequest = "";
        try
        {
            final ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);

            logger.info("Santos.GcdmAdminAccess.addNewPressure data [" + adminParameter.allValues.toString() + "]");
            // FDR-70 Add new pressure
            // simulation :
            gcdmResult.newGasCompositionValues = adminParameter.allValues;
            gcdmResult.status = "Simulation save is done";

            logger.info("Santos.GcdmAdminAccess.addNewPressure : Start process");

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmAdminAccess: Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };
        return gcdmResult;
    }

    // FDR-73
    public static GcdmToolbox.GcdmResult deleteListSupplyChainPoint(final AdminParameter adminParameter, final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        try {
            final ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);

            gcdmResult.status = "Delete list DUID [";
            if (adminParameter.listToDelete != null)
            {
                for (final Object uidObj : adminParameter.listToDelete)
                {
                    final Long duid = Toolbox.getLong(uidObj, null);
                    if (duid == null)
                    {
                        logger.severe("Santos.GcdmAdminAccess: We receive a non LONG value is list [" + uidObj + "] list : ["
                                + adminParameter.listToDelete + "]");
                        continue;
                    }

                    gcdmResult.status += (duid == null ? "null" : duid.toString()) + ",";
                }
            } else {
                gcdmResult.status += " <null list>";
            }
            gcdmResult.status += "]";

            logger.info("Santos.GcdmAdminAccess.deleteListPressure : Deletion");

            // here the request
            logger.info("Santos.GcdmAdminAccess.deleteListPressure - delete " + gcdmResult.status);
        } catch (final BonitaHomeNotSetException | ServerAPIException | UnknownAPITypeException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmAdminAccess: Error during getAPI " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during getAPI : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }

        return gcdmResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* private */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    private static void completeListSupplyChainPointHeader(final GcdmToolbox.GcdmResult gcdmResult)
    {
        // FDR-67
        gcdmResult.addHeaderColumns("EFFECTIVEDATE_ST", "Effective Date/Time", typeColumn.text);
        gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
        gcdmResult.addHeaderColumns("POINTTYPE", "Point Type", typeColumn.text);
        gcdmResult.addHeaderColumns("PERMITBLENDDEFAULT", "Permit Blend Default", typeColumn.text);
        gcdmResult.addHeaderColumns("PERMITBLENDMINIMUM", "Permit Blend Minimum", typeColumn.text);
        gcdmResult.addHeaderColumns("PERMITBLENDTHRESHOLD", "Permit Blend Threshold", typeColumn.text);
    }

    private static void completeListSupplyChainPointFields(final GcdmToolbox.GcdmResult gcdmResult)
    {

        gcdmResult.addEditFields("POINTTYPE", "Point Type", typeColumn.list, false, false, new String[] { "Node", "Point" });
        gcdmResult.addEditFields("PERMITBLENDDEFAULT", "Permit Blend Default", typeColumn.checkbox, false, false, null, null);
        gcdmResult.addEditFields("PERMITBLENDMINIMUM", "Permit Blend Minimum", typeColumn.checkbox, false, false, null, null);
        gcdmResult.addEditFields("PERMITBLENDTHRESHOLD", "Permit Blend Threshold", typeColumn.checkbox, false, false, null, null);

    }
}
