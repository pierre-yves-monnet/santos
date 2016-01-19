package com.santos.gcdmaccess;

import java.io.PrintWriter;
import java.io.Serializable;
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
import com.santos.toolbox.ProcessToolbox;
import com.santos.toolbox.ProcessToolbox.ProcessToolboxResult;
import com.santos.toolbox.Toolbox;

public class GcdmPressureAccess {

    static Logger logger = Logger.getLogger("org.bonitasoft.GcdmPressureAccess");

    public static final String processDeletePressure = "CGDMDeletePressure";
    public static final String processAddPressure = "GCDMCreatePressure";

    /* ******************************************************************************** */
    /*                                                                                  */
    /* GCDM Pressure */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class PressureParameter
    {

        public boolean allowDirectConnection = false;

        public boolean formatDateJson = true;
        public int maxRecord = 1000;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;
        public Long duid;
        public List<String> listToDelete = null;

        public static PressureParameter getFromJson(final String jsonSt)
        {
            final PressureParameter pressureParameter = new PressureParameter();
            if (jsonSt == null) {
                return pressureParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return pressureParameter;
            }
            pressureParameter.listToDelete = (List) jsonHash.get("listtodelete");
            pressureParameter.duid = Toolbox.getLong(jsonHash.get("DUID"), null);

            return pressureParameter;
        }

    };

    /** get the list of the GasComposition FDR-64 */
    public static GcdmToolbox.GcdmResult getListPressure(final PressureParameter gasComposition,
            final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        final Connection con = GcdmToolbox.getConnection(gasComposition.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        String sqlRequest = "";
        try
        {

            // FDR-65 -- FDR 67 Column must be dynamique
            sqlRequest = " select pre_uid_pk as DUID, "
                    + "p.effectivedate as \"EFFECTIVEDATE\", "
                    + "dp.pointname as \"SUPPLYCHAINEPOINT\",  "
                    + "p.minimumthreshold as \"MINIMUMTHRESHOLD\","
                    + " p.minimumaction as \"MINIMUMACTION\", "
                    + "p.maximumthreshold as \"MAXIMUMTHRESHOLD\",  p.maximumaction as \"MAXIMUMACTION\" "
                    + " from pressures p, DataPoints dp"
                    + " where p.pre_dpt_uid_fk = dp.dpt_uid_pk and p.recordstatus='CURRENT'"
                    + " order by dp.pointname";

            final List<Object> listRequestObject = new ArrayList<Object>();

            gcdmResult.listValues = GcdmToolbox.executeRequest(con, sqlRequest, listRequestObject, gasComposition.maxRecord, gasComposition.formatDateJson);




            // FDR-67 create the header
            completePressureHeader(gcdmResult);

            // get the NewGasCompositionFields
            completeCompositionFields(gcdmResult);

            gcdmResult.status = gcdmResult.listValues.size() + " item+" + (gcdmResult.listValues.size() > 1 ? "s" : "") + " found";

            final IdentityAPI identityAPI = TenantAPIAccessor.getIdentityAPI(apiSession);
            final ProfileAPI profileAPI = TenantAPIAccessor.getProfileAPI(apiSession);

            gcdmResult.isEditProfile = GcdmToolbox.isEditGcdmProfile(apiSession.getUserId(), profileAPI, identityAPI);

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmPressureAccess: Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
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

    public static class NewPressureParameter
    {

        public Map<String, Object> allValues = null;
        public String errormessage;
        public boolean allowDirectConnection = false;

        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static NewPressureParameter getFromJson(final String jsonSt)
        {

            final NewPressureParameter gasCompositionParameter = new NewPressureParameter();
            if (jsonSt == null) {
                return gasCompositionParameter;
            }
            try
            {
                final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
                if (jsonHash == null) {
                    return gasCompositionParameter;
                }

                gasCompositionParameter.allValues = jsonHash;
            } catch (final Exception e) {
                logger.severe("Santos.GcdmPressureAccess: NewPressureParameter.getFromJson : exception e " + e.toString() + " from paramter [" + jsonSt + "]");
                gasCompositionParameter.allValues = new HashMap<String, Object>();
            }
            return gasCompositionParameter;

        }

    };

    /**
     * FDR-35
     *
     * @param gasComposition
     * @return
     */
    public static GcdmToolbox.GcdmResult getDefaultPressure(final NewPressureParameter pressureParameter, final APISession apiSession)
    {
        logger.info("Santos.GcdmPressureAccess.getDefaultPressure: parameters=" + pressureParameter.toString());
        final GcdmResult gcdmResult = new GcdmResult();
        final Connection con = GcdmToolbox.getConnection(pressureParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        final String sqlRequest = "";
        try
        {

            gcdmResult.values.put("MINIMUMTHRESHOLD", System.currentTimeMillis() % 1000);

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmPressureAccess: Error during the getDefaultAddComposition request [" + sqlRequest + "] call " + e.toString() + " at "
                    + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during getDefaultAddComposition request[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };

        return gcdmResult;
    }

    // FDR-71

    public static GcdmToolbox.GcdmResult getPressure(final PressureParameter pressureParameter,
            final APISession apiSession)
    {
        logger.info("Santos.GcdmPressureAccess.getPressure: parameters=" + pressureParameter.toString());
        final GcdmResult gcdmResult = new GcdmResult();
        final Connection con = GcdmToolbox.getConnection(pressureParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        final String sqlRequest = "";
        try
        {
            // FDR-14
            final long delta = pressureParameter.duid;

            final Calendar c = Calendar.getInstance();
            c.set(Calendar.YEAR, 2015);
            c.set(Calendar.MONTH, 11);

            gcdmResult.values.put("MINIMUMTHRESHOLD", 23);

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmPressureAccess: Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
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

    // FDR 70 add new pressure
    public static GcdmToolbox.GcdmResult addNewPressure(final NewPressureParameter pressureComposition, final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        final Connection con = GcdmToolbox.getConnection(pressureComposition.allowDirectConnection);
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

            logger.info("Santos.GcdmPressureAccess.addNewPressure data [" + pressureComposition.allValues.toString() + "]");
            // FDR-70 Add new pressure
            // simulation :
            gcdmResult.newGasCompositionValues = pressureComposition.allValues;
            gcdmResult.status = "Simulation save is done";

            logger.info("Santos.GcdmPressureAccess.addNewPressure : Start process");
            final Map<String, Serializable> variables = new HashMap<String, Serializable>();
            variables.put("pressuremap", (Serializable) pressureComposition.allValues);

            final ProcessToolboxResult processToolboxResult = ProcessToolbox.startACase(processAddPressure, variables, processAPI);
            gcdmResult.listEvents.addAll(processToolboxResult.listEvents);

            logger.info("Santos.GcdmPressureAccess.addNewPressure - Search " + gcdmResult.status);

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmPressureAccess: Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
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
    public static GcdmToolbox.GcdmResult deleteListPressure(final PressureParameter pressureParameter, final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        try {
            final ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);

            gcdmResult.status = "Delete list DUID [";
            if (pressureParameter.listToDelete != null)
            {
                for (final Object uidObj : pressureParameter.listToDelete)
                {
                    final Long duid = Toolbox.getLong(uidObj, null);
                    if (duid == null)
                    {
                        logger.severe("Santos.GcdmPressureAccess: We receive a non LONG value is list [" + uidObj + "] list : ["
                                + pressureParameter.listToDelete + "]");
                        continue;
                    }

                    gcdmResult.status += (duid == null ? "null" : duid.toString()) + ",";
                }
            } else {
                gcdmResult.status += " <null list>";
            }
            gcdmResult.status += "]";

            logger.info("Santos.GcdmPressureAccess.deleteListPressure : Deletion");

            // here the request
            logger.info("Santos.GcdmPressureAccess.deleteListPressure : Start process");
            final Map<String, Serializable> variables = new HashMap<String, Serializable>();
            variables.put("pressurelistid", (Serializable) pressureParameter.listToDelete);

            final ProcessToolboxResult processToolboxResult = ProcessToolbox.startACase(processDeletePressure, variables, processAPI);
            gcdmResult.listEvents.addAll(processToolboxResult.listEvents);

            logger.info("Santos.GcdmPressureAccess.deleteListPressure - delete " + gcdmResult.status);
        } catch (final BonitaHomeNotSetException | ServerAPIException | UnknownAPITypeException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Santos.GcdmPressureAccess: Error during getAPI " + e.toString() + " at " + sw.toString());
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
    private static void completePressureHeader(final GcdmToolbox.GcdmResult gcdmResult)
    {
        // FDR-67
        gcdmResult.addHeaderColumns("EFFECTIVEDATE_ST", "Effective Date/Time", typeColumn.text);
        gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
        gcdmResult.addHeaderColumns("MINIMUMTHRESHOLD", "Minimum Operating Envelope", typeColumn.text);
        gcdmResult.addHeaderColumns("MINIMUMACTION", "PGMS Minimum Action", typeColumn.text);
        gcdmResult.addHeaderColumns("MAXIMUMTHRESHOLD", "Maximum Operating Envelope", typeColumn.text);
        gcdmResult.addHeaderColumns("MAXIMUMACTION", "PGMS Maximum Action", typeColumn.text);
    }

    private static void completeCompositionFields(final GcdmToolbox.GcdmResult gcdmResult)
    {
        gcdmResult.addEditFields("MINIMUMTHRESHOLD", "Minimum Operating Envelope", typeColumn.number, false, false, 12, 100);
        gcdmResult.addEditFields("MINIMUMACTION", "Minimum Operating Envelope", typeColumn.number, false, false, null, null);
        gcdmResult.addEditFields("MAXIMUMTHRESHOLD", "Maximum Operating Envelope", typeColumn.number, false, false, null, null);
        gcdmResult.addEditFields("MAXIMUMACTION", "PGMS Maximum Action", typeColumn.number, false, false, null, null);

    }
}
