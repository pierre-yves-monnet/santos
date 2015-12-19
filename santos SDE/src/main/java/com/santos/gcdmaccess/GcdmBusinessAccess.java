package com.santos.gcdmaccess;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;
import org.json.simple.JSONValue;

import com.santos.toolbox.ProcessToolbox;
import com.santos.toolbox.ProcessToolbox.ProcessToolboxResult;
import com.santos.toolbox.Toolbox;

public class GcdmBusinessAccess {

    public static String DATASOURCE_NAME = "GCDM_DS";

    public static final String processDeleteGasComposition = "CGDMDeleteGasComposition";
    public static final String processAddGasComposition = "GCDMCreateGasComposition";

    private final BEvent eventDeleteGasCompositionInThePast = new BEvent(GcdmBusinessAccess.class.getName(), 1, Level.APPLICATIONERROR,
            "Gas composition in the past", "You can't delete a gas composition in the past, only in the future",
            "Select an another gas composition");

    private static Logger logger = Logger.getLogger("org.bonitasoft.SdeAccess");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static SimpleDateFormat sdfHuman = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    private static SimpleDateFormat sdfEffectiveDate = new SimpleDateFormat("dd-MM-yyyy HH:mm");

    public static class GcdmResult {

        public String status;
        public String errorstatus;
        public List<Map<String, Object>> listValues = new ArrayList<Map<String, Object>>();
        public Map<String, List<Map<String, Object>>> listsSelect = new HashMap<String, List<Map<String, Object>>>();
        public List<Map<String, Object>> listHeader = new ArrayList<Map<String, Object>>();
        public List<Map<String, Object>> newGasCompositionFields = new ArrayList<Map<String, Object>>();
        public Map<String, Object> newGasCompositionValues = new HashMap<String, Object>();
        public List<BEvent> listEvents = new ArrayList<BEvent>();

        public void addHeader(final String id, final String display) {
            final HashMap<String, Object> oneheader = new HashMap<String, Object>();
            oneheader.put("id", id);
            oneheader.put("display", display);
            listHeader.add(oneheader);
        }

        public void addNewGasCompositionFields(final String id, final String display, final String typeofField,
                final boolean readonly,
                final Object minrange, final Object maxrange) {
            final HashMap<String, Object> oneField = new HashMap<String, Object>();
            oneField.put("id", id);
            oneField.put("display", display);
            oneField.put("typeoffield", typeofField);
            oneField.put("readonly", readonly);
            if (readonly) {
                oneField.put("cssstyle", "background-color: burlywood;");
                oneField.put("cssclass", "santosreadonly");
            }

            if (minrange != null) {
                oneField.put("minrange", minrange);
            }
            if (maxrange != null) {
                oneField.put("maxrange", maxrange);
            }

            newGasCompositionFields.add(oneField);
        }

        /**
         * @return
         */
        public Map<String, Object> toMap()
        {
            logger.info("GcdmResult: message[" + status + "] erroMessage[" + errorstatus + "] events[" + listEvents.toString() + "]");
            final Map<String, Object> result = new HashMap<String, Object>();
            result.put("LISTVALUES", listValues);
            result.put("LISTHEADERS", listHeader);
            result.put("MESSAGE", status);
            result.put("ERRORMESSAGE", errorstatus);
            result.put("LISTSUPPLYCHAIN", listsSelect.get("LISTSUPPLYCHAIN"));
            result.put("NEWGASCOMPOSITIONFIELDS", newGasCompositionFields);
            result.put("NEWGASCOMPOSITIONVALUES", newGasCompositionValues);
            // logger.info("GcdmResult:GetHtml" + BEventFactory.getHtml(listEvents));
            result.put("LISTEVENTS", BEventFactory.getHtml(listEvents));

            return result;
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* GasComposition */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public enum EnuTypeDisplays {
        Defaults, Minimum, BlendAlarm
    };
    public static class GasCompositionParameter {

        public EnuTypeDisplays typeDisplay;
        public String filterUWI = null;

        public boolean allowDirectConnection = false;

        public int maxRecord = 100;
        public String orderByField = "ELEMENTNAME";
        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public List<String> listToDelete = null;
        public static GasCompositionParameter getFromJson(final String jsonSt)
        {
            final GasCompositionParameter gasCompositionParameter = new GasCompositionParameter();
            if (jsonSt == null) {
                return gasCompositionParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return gasCompositionParameter;
            }
            gasCompositionParameter.typeDisplay = EnuTypeDisplays.valueOf((String) jsonHash.get("type"));
            gasCompositionParameter.filterUWI = (String) jsonHash.get("filteruwi");
            gasCompositionParameter.listToDelete = (List) jsonHash.get("listtodelete");
            return gasCompositionParameter;
        }

    };

    /** get the list of the GasComposition FRDR-07 */
    public GcdmResult getListGasComposition(final GasCompositionParameter gasComposition)
    {
        final GcdmResult gcdmResult = new GcdmResult();
        Connection con = getConnection(gasComposition.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        String sqlRequest = "";
        try
        {
            if (gasComposition.typeDisplay == EnuTypeDisplays.Defaults)
            {
                sqlRequest = "select c.cpn_cse_uid_fk as uid, c.EffectiveDate, dp.PointName, 'SpecificGravity'   as SpecificGravity, 'HeatingValue'      as HeatingValue, c.value"
                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                        + " order by dp.PointName, c.pgmscode";
            }

            // FDR 60
            if (gasComposition.typeDisplay == EnuTypeDisplays.Minimum)
            {
                sqlRequest = "select c.cpn_cse_uid_fk as uid, c.EffectiveDate, dp.PointName, 'SpecificGravity'   as SpecificGravity, 'HeatingValue'      as HeatingValue, c.value"
                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                        + " order by dp.PointName, c.pgmscode";
            }
            // FDR-62
            if (gasComposition.typeDisplay == EnuTypeDisplays.BlendAlarm)
            {
                sqlRequest = "select c.cpn_cse_uid_fk as uid, c.EffectiveDate, dp.PointName, 'SpecificGravity'   as SpecificGravity, 'HeatingValue'      as HeatingValue, c.value"
                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                        + " order by dp.PointName, c.pgmscode";
            }
            final List<Object> listRequestObject = new ArrayList<Object>();


            gcdmResult.listValues = executeRequest(con, sqlRequest, listRequestObject, gasComposition.maxRecord, gasComposition.formatDateJson);


            // Simulation : just remplace some value
            if (gcdmResult.listValues.size()>1) {
                gcdmResult.listValues.get(0).put("UID",3);
                gcdmResult.listValues.get(0).put("SUPPLYCHAINEPOINT", gasComposition.typeDisplay.toString());
            }
            if (gcdmResult.listValues.size()>2) {
                gcdmResult.listValues.get(1).put("UID",3);
            }


            // list Select
            final List<Map<String, Object>> listRecords = new ArrayList<Map<String, Object>>();
            final HashMap<String, Object> option = new HashMap<String, Object>();
            option.put("id", "1");
            option.put("name", "First value");
            listRecords.add(option);
            gcdmResult.listsSelect.put("LISTSUPPLYCHAIN", listRecords);

            // create the header
            completeGasCompositionHeader(gasComposition.typeDisplay, gcdmResult);

            // get the NewGasCompositionFields
            completeNewGasCompositionFields(gcdmResult);

            gcdmResult.status = gcdmResult.listValues.size() + " item+" + (gcdmResult.listValues.size() > 1 ? "s" : "") + " found";



        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }
        return gcdmResult;
    }

    /** Delete a list of record FDR-50, FDR-55, FDR-57, FDR-58 */
    public GcdmResult deleteListGasComposition(final GasCompositionParameter gasComposition, final ProcessAPI processAPI)
    {
        final GcdmResult gcdmResult = new GcdmResult();
        gcdmResult.status = "Delete list UID [";
        String isInThePast = "";
        if (gasComposition.listToDelete != null)
        {
            for (final Object uidObj : gasComposition.listToDelete)
            {
                final Long uid = Toolbox.getLong( uidObj, null);
                if (uid==null)
                {
                    logger.severe("We receive a non LONG value is list ["+uidObj+"] list : ["+gasComposition.listToDelete+"]");
                    continue;
                }
                if (uid % 3 ==0) {
                    isInThePast+=uid.toString();
                }

                gcdmResult.status += (uid == null ? "null" : uid.toString()) + ",";
            }
        } else {
            gcdmResult.status += " <null list>";
        }
        gcdmResult.status += "]";
        // FDR-55 Check if all Effective date in this list are In the futur, else refuse
        if (isInThePast.length()>0)
        {
            gcdmResult.listEvents.add(new BEvent(eventDeleteGasCompositionInThePast, isInThePast));
            return gcdmResult;
        }

        logger.info("Santos.GcdmBusinessAccess : Deletion");

        logger.info("Santos.GcdmBusinessAccess : Start process");
        final Map<String, Serializable> variables = new HashMap<String, Serializable>();
        variables.put("gascompositionlistid", (Serializable) gasComposition.listToDelete);

        final ProcessToolboxResult processToolboxResult = ProcessToolbox.startACase(processDeleteGasComposition, variables, processAPI);
        gcdmResult.listEvents.addAll(processToolboxResult.listEvents);

        logger.info("GdmBusinessAccess.deleteListGasComposition - delete " + gcdmResult.status);
        return gcdmResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* NewGasComposition */
    /* fonction about the Modal popup */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class NewGasCompositionParameter
    {

        public Date effectiveDate = null;
        public String supplyChainPoint = null;

        public Map<String, Object> allValues = null;
        public String errormessage;
        public boolean allowDirectConnection = false;

        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static NewGasCompositionParameter getFromJson(final String jsonSt)
        {
            final NewGasCompositionParameter gasCompositionParameter = new NewGasCompositionParameter();
            if (jsonSt == null) {
                return gasCompositionParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return gasCompositionParameter;
            }

            gasCompositionParameter.supplyChainPoint = (String) jsonHash.get("SUPPLYCHAINPOINT");
            final String effectiveDateSt = (String) jsonHash.get("EFFECTIVEDATE_ST");
            gasCompositionParameter.allValues = jsonHash;
            try
            {
                gasCompositionParameter.effectiveDate = sdfEffectiveDate.parse(effectiveDateSt);
            } catch (final Exception e)
            {
                logger.info("NewGasCompositionParameter.fromJson : Bad dateFormat [" + effectiveDateSt + "]");
                gasCompositionParameter.errormessage = "Please give a complete date";
            }

            return gasCompositionParameter;
        }

    };

    /**
     * FDR-51
     *
     * @param newGasComposition
     * @return
     */
    public GcdmResult searchListGasComposition(final NewGasCompositionParameter newGasComposition)
    {
        final GcdmResult gcdmResult = new GcdmResult();
        gcdmResult.status = "Search SupplyChain [" + newGasComposition.supplyChainPoint + "] at date " + sdfHuman.format(newGasComposition.effectiveDate);

        // simulation :
        completeNewGasCompositionFields(gcdmResult);
        for (final Map<String, Object> oneField : gcdmResult.newGasCompositionFields) {
            gcdmResult.newGasCompositionValues.put((String) oneField.get("id"), Long.valueOf((long) (10 + 50 * Math.random())));
        }
        logger.info("GdmBusinessAccess.deleteListGasComposition - Search " + gcdmResult.status);
        return gcdmResult;
    }

    /**
     * FDR-30
     *
     * @param newGasComposition
     * @return
     */
    public GcdmResult addNewGasComposition(final NewGasCompositionParameter newGasComposition, final ProcessAPI processAPI)
    {
        final GcdmResult gcdmResult = new GcdmResult();

        // simulation :
        gcdmResult.newGasCompositionValues = newGasComposition.allValues;
        gcdmResult.status = "Simulation save is done";

        logger.info("Santos.GcdmBusinessAccess.addNewGasComposition : Start process");
        final Map<String, Serializable> variables = new HashMap<String, Serializable>();
        variables.put("gascompositionmap", (Serializable) newGasComposition.allValues);

        final ProcessToolboxResult processToolboxResult = ProcessToolbox.startACase(processAddGasComposition, variables, processAPI);
        gcdmResult.listEvents.addAll(processToolboxResult.listEvents);

        logger.info("GdmBusinessAccess.deleteListGasComposition - Search " + gcdmResult.status);
        return gcdmResult;
    }
    /* ******************************************************************************** */
    /*                                                                                  */
    /* private */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    private void completeGasCompositionHeader(final EnuTypeDisplays typeDisplay, final GcdmResult gcdmResult)
    {
        if (typeDisplay == EnuTypeDisplays.Defaults)
        {
            gcdmResult.addHeader("EFFECTIVEDATETIME_ST", "Effective Date/Time");
            gcdmResult.addHeader("SUPPLYCHAINEPOINT", "Supply Chain point");
            gcdmResult.addHeader("SPECIFICGRAVITY", "Specific Gravity");
            gcdmResult.addHeader("HEATINGVALUE", "Heating Value");
            gcdmResult.addHeader("METHANEC1", "Methane C1");
            gcdmResult.addHeader("ETHANEC2", "Ethane C2");
            gcdmResult.addHeader("PROPANEC3", "Propane C3");
            gcdmResult.addHeader("IBUTANEC4I", "I-Butane C4i");
            gcdmResult.addHeader("NBUTANEC4N", "N-Butane C4n");
            gcdmResult.addHeader("BUTANEC4", "Butane C4");
        }
        else if (typeDisplay == EnuTypeDisplays.Minimum)
        {
            // FDR 60
            gcdmResult.addHeader("EFFECTIVEDATETIME_ST", "Effective Date/Time");
            gcdmResult.addHeader("SUPPLYCHAINEPOINT", "Supply Chain point");
            gcdmResult.addHeader("SPECIFICGRAVITY", "Specific Gravity");
            gcdmResult.addHeader("HEATINGVALUE", "Heating Value");
            gcdmResult.addHeader("METHANEC1", "Methane C1");
            gcdmResult.addHeader("ETHANEC2", "Ethane C2");
            gcdmResult.addHeader("PROPANEC3", "Propane C3");
            gcdmResult.addHeader("IBUTANEC4I", "I-Butane C4i");
            gcdmResult.addHeader("NBUTANEC4N", "N-Butane C4n");
            gcdmResult.addHeader("BUTANEC4", "Butane C4");
            gcdmResult.addHeader("IPENTANEC5", "IPentane C5");
            gcdmResult.addHeader("NEOPENTANE", "Neo_Pentane");

        }
        else if (typeDisplay == EnuTypeDisplays.BlendAlarm)
        {
            // FDR 62
            gcdmResult.addHeader("EFFECTIVEDATETIME_ST", "Effective Date/Time");
            gcdmResult.addHeader("SUPPLYCHAINEPOINT", "Supply Chain point");
            gcdmResult.addHeader("SPECIFICGRAVITY", "Specific Gravity");
            gcdmResult.addHeader("HEATINGVALUE", "Heating Value");
            gcdmResult.addHeader("METHANEC1", "Methane C1");
            gcdmResult.addHeader("ETHANEC2", "Ethane C2");
            gcdmResult.addHeader("PROPANEC3", "Propane C3");
            gcdmResult.addHeader("IBUTANEC4I", "I-Butane C4i");
            gcdmResult.addHeader("NBUTANEC4N", "N-Butane C4n");
            gcdmResult.addHeader("BUTANEC4", "Butane C4");
            gcdmResult.addHeader("IPENTANEC5", "IPentane C5");
        }

    }

    private void completeNewGasCompositionFields(final GcdmResult gcdmResult)
    {
        gcdmResult.addNewGasCompositionFields("C1", "C1 (mole %)", "number", false, 70, 100);
        gcdmResult.addNewGasCompositionFields("C2", "C2 (mole %)", "number", false, 0, 20);
        gcdmResult.addNewGasCompositionFields("C3", "C3 (mole %)", "number", false, 0, 6);
        gcdmResult.addNewGasCompositionFields("C4", "C4 (mole %)", "number", true, 0, 3);
        gcdmResult.addNewGasCompositionFields("C4I", "C4i (mole %)", "number", false, 0, 1.5);
        gcdmResult.addNewGasCompositionFields("C5", "C5 (mole %)", "number", true, 0, 0.6);
        gcdmResult.addNewGasCompositionFields("C5I", "C5i (mole %)", "number", false, 0, 0.3);
        gcdmResult.addNewGasCompositionFields("C5N", "C5n (mole %)", "number", false, 0, 0.3);
        gcdmResult.addNewGasCompositionFields("C6PLUS", "C6+ (ppm)", "number", false, 0, 3000);
        gcdmResult.addNewGasCompositionFields("C8", "C8 (ppb)", "number", false, 0, 2000);
        gcdmResult.addNewGasCompositionFields("C9", "C9 (ppb)", "number", false, 0, 2000);
        gcdmResult.addNewGasCompositionFields("CO2", "CO2 (model %)", "number", false, 0, 30);
        gcdmResult.addNewGasCompositionFields("H2S", "H2S(ppm)", "number", false, 0, 10);
        gcdmResult.addNewGasCompositionFields("N2", "N2 (mole %)", "number", false, 0, 50);
        gcdmResult.addNewGasCompositionFields("H2O", "H2O (mg/Sm3)", "number", false, 0, 650);
        gcdmResult.addNewGasCompositionFields("TSU", "TSU (ppm)", "number", false, 0, 20);
        gcdmResult.addNewGasCompositionFields("BEN", "BEN (ppb)", "number", false, 0, 5000);
        gcdmResult.addNewGasCompositionFields("CYC", "CYC (ppb)", "number", false, 0, 5000);
        gcdmResult.addNewGasCompositionFields("SPEGRA", "Specific Gravity", "text", true, null, null);
        gcdmResult.addNewGasCompositionFields("HEATING", "Heating Value (MJ/Sm3)", "text", true, null, null);

    }

    public List<Map<String, Object>> executeRequest(final Connection con, final String sqlRequest, final List<Object> listRequestObject, final int maxRecord,
            final boolean formatDateJson) throws SQLException
    {
        logger.info("getListGasComposition: Execute the request [" + sqlRequest + "] parameters");
        final List<Map<String, Object>> listRecords = new ArrayList<Map<String, Object>>();
        final PreparedStatement pstmt = con.prepareStatement(sqlRequest);
        for (int i = 0; i < listRequestObject.size(); i++)
        {
            final Object o = listRequestObject.get(i);
            pstmt.setObject(i + 1, o);
        }

        int numberOfRecords = 0;
        final ResultSet rs = pstmt.executeQuery();
        final ResultSetMetaData rsmd = rs.getMetaData();

        while (rs.next() && numberOfRecords < maxRecord)
        {
            numberOfRecords++;
            final HashMap<String, Object> record = new HashMap<String, Object>();

            final int count = rsmd.getColumnCount();
            for (int i = 1; i <= count; i++)
            {
                String key = rsmd.getColumnName(i);
                key = key.toUpperCase();
                if (rs.getObject(i) instanceof Date)
                {
                    final Date date = rs.getDate(i);
                    record.put(key + "_ST", sdfHuman.format(date.getTime()));
                }
                if (rs.getObject(i) instanceof Date && formatDateJson) {
                    final Date date = rs.getDate(i);
                    record.put(key, date.getTime());
                    continue;
                }

                record.put(key, rs.getObject(i));
            }
            logger.info("Read  [" + record + "]");
            listRecords.add(record);
        }
        pstmt.close();
        return listRecords;
    }

    /**
     * get the connection
     *
     * @return
     */
    private Connection getConnection(final boolean allowDirectConnection)
    {
        Context ctx = null;
        try
        {
            ctx = new InitialContext();
        } catch (final Exception e)
        {
            logger.severe("Cant' get an InitialContext : can't access the datasource");
            return null;
        }

        DataSource ds = null;
        Connection con = null;
        try
        {
            ds = (DataSource) ctx.lookup("java:/comp/env/" + DATASOURCE_NAME);
            con = ds.getConnection();

        } catch (final Exception e)
        {
            if (allowDirectConnection) {
                con = getDirectConnection();
            } else {
                logger.severe("Can't access the DataSource [" + DATASOURCE_NAME + "] " + e.toString());
            }

        }

        try {
            if (con != null) {
                con.setAutoCommit(false);
            }
        } catch (final SQLException e) {
            logger.severe("Can't set autocommit to false on connection");
        }
        return con;
    }

    private Connection getDirectConnection()
    {
        try {
            Class.forName("org.postgresql.Driver");

            Connection connection = null;
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/santosGCDM", "bonita", "bonita");
            return connection;
        } catch (final ClassNotFoundException e) {
            logger.severe("error " + e.toString());

        } catch (final SQLException e) {
            logger.severe("error " + e.toString());
        }
        return null;
    }

}
