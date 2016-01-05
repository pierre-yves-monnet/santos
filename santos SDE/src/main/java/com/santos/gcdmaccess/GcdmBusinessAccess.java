package com.santos.gcdmaccess;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.exception.BonitaHomeNotSetException;
import org.bonitasoft.engine.exception.ServerAPIException;
import org.bonitasoft.engine.exception.UnknownAPITypeException;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.json.simple.JSONValue;

import com.santos.gcdmaccess.GcdmToolbox.GcdmResult;
import com.santos.gcdmaccess.GcdmToolbox.GcdmResult.typeColumn;
import com.santos.toolbox.ProcessToolbox;
import com.santos.toolbox.ProcessToolbox.ProcessToolboxResult;
import com.santos.toolbox.Toolbox;

public class GcdmBusinessAccess {


    public static final String processDeleteGasComposition = "CGDMDeleteGasComposition";
    public static final String processAddGasComposition = "GCDMCreateGasComposition";

    private final static BEvent eventDeleteGasCompositionInThePast = new BEvent(GcdmBusinessAccess.class.getName(), 1, Level.APPLICATIONERROR,
            "Gas composition in the past", "You can't delete a gas composition in the past, only in the future",
            "Select an another gas composition");
    private final static BEvent eventCorrectDateExpected = new BEvent(GcdmBusinessAccess.class.getName(), 2, Level.APPLICATIONERROR,
            "Correct date expected", "The effective date must be correct",
            "Check the date");


    static Logger logger = Logger.getLogger("org.bonitasoft.SdeAccess");
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
        public boolean viewFuturDatedDefaults = false;

        public Long uid;

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
            gasCompositionParameter.viewFuturDatedDefaults = Toolbox.getBoolean(jsonHash.get("viewFuturDatedDefaults"), Boolean.FALSE);
            gasCompositionParameter.listToDelete = (List) jsonHash.get("listtodelete");
            gasCompositionParameter.uid = Toolbox.getLong(jsonHash.get("UID"), null);
            return gasCompositionParameter;
        }

        @Override
        public String toString()
        {
            return "typeDisplay[" + typeDisplay + "] viewFuturDatedDefaults[" + viewFuturDatedDefaults + "]";
        }
    };

    /** get the list of the GasComposition FDR-07 */
    public GcdmToolbox.GcdmResult getListGasComposition(final GasCompositionParameter gasComposition)
    {
        logger.info("GcdmBusinessAccess.getListGasComposition: parameters=" + gasComposition.toString());
        final GcdmResult gcdmResult = new GcdmResult();
        Connection con = GcdmToolbox.getConnection(gasComposition.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        String sqlRequest = "";
        try
        {
            // FDR-07
            if (gasComposition.typeDisplay == EnuTypeDisplays.Defaults)
            {
                sqlRequest = "select c.cpn_cse_uid_fk as uid, c.EffectiveDate, dp.PointName, 'SpecificGravity '  as SPECIFICGRAVITY, 'HeatingValue'      as HeatingValue, c.value,"
                        + " 12 as METHANEC1"
                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                        + " order by dp.PointName, c.pgmscode";
            }

            // FDR-60
            if (gasComposition.typeDisplay == EnuTypeDisplays.Minimum)
            {
                sqlRequest = "select c.cpn_cse_uid_fk as uid, c.EffectiveDate, dp.PointName, 'SpecificGravity'  as SPECIFICGRAVITY, 'HeatingValue'      as HeatingValue, c.value"
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
                sqlRequest = "select c.cpn_cse_uid_fk as UID, c.EffectiveDate, dp.PointName, 'SpecificGravity' as SPECIFICGRAVITY, 'HeatingValue'      as HeatingValue, c.value"
                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                        + " order by dp.PointName, c.pgmscode";
            }
            final List<Object> listRequestObject = new ArrayList<Object>();


            gcdmResult.listValues = GcdmToolbox.executeRequest(con, sqlRequest, listRequestObject, gasComposition.maxRecord, gasComposition.formatDateJson);


            // Simulation : just remplace some value
            if (gcdmResult.listValues.size()>1) {
                gcdmResult.listValues.get(0).put("UID",3);
                gcdmResult.listValues.get(0).put("SUPPLYCHAINEPOINT", gasComposition.typeDisplay.toString());
            }
            if (gcdmResult.listValues.size()>2) {
                gcdmResult.listValues.get(1).put("UID",3);
            }
            for (final Map<String, Object> oneValue : gcdmResult.listValues)
            {
                oneValue.put("SPECIFICGRAVITY", oneValue.get("SPECIFICGRAVITY") + " " + oneValue.get("UID"));
            }

            // list SupplyChaine FDR-18
            completeListSupplyChain(con, gcdmResult);


            // FDR-10 create the header
            completeGasCompositionHeader(con, gasComposition.typeDisplay, gcdmResult);

            // get the NewGasCompositionFields
            completeNewGasCompositionFields(con, gcdmResult);

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

    /**
     * FDR-35
     *
     * @param gasComposition
     * @return
     */
    public GcdmToolbox.GcdmResult getDefaultGasComposition(final GasCompositionParameter gasComposition)
    {
        logger.info("GcdmBusinessAccess.getDefaultGasComposition: parameters=" + gasComposition.toString());
        final GcdmResult gcdmResult = new GcdmResult();
        Connection con = GcdmToolbox.getConnection(gasComposition.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        final String sqlRequest = "";
        try
        {
            // FDR-35
            long delta = 155;
            if (gasComposition.typeDisplay == EnuTypeDisplays.Defaults)
            {
                delta = 20;
            }
            if (gasComposition.typeDisplay == EnuTypeDisplays.Minimum)
            {
                delta = 200;
            };
            if (gasComposition.typeDisplay == EnuTypeDisplays.BlendAlarm)
            {
                delta = 2000;
            }

            final Calendar c = Calendar.getInstance();
            c.set(Calendar.YEAR, 2015);
            c.set(Calendar.MONTH, 11);

            gcdmResult.values.put("SUPPLYCHAINPOINT", "Defaults");
            // give as a LONG format
            gcdmResult.values.put("EFFECTIVEDATE", Toolbox.sdfJavasscript.format(c.getTimeInMillis()));
            // give as HH:MM format
            gcdmResult.values.put("EFFECTIVETIME", GcdmToolbox.sdfJavasscriptHour.format(c.getTime()));

            // listSupplyChaind
            completeListSupplyChain(con, gcdmResult);

            gcdmResult.values.put("EFFECTIVEDATETIME_ST", GcdmToolbox.sdfEffectiveDate.format(c.getTime()));
            gcdmResult.values.put("SPECIFICGRAVITY", delta + 2);
            gcdmResult.values.put("HEATINGVALUE", delta + 3);
            gcdmResult.values.put("C1", delta + 4);
            gcdmResult.values.put("C2", delta + 5);
            gcdmResult.values.put("C3", delta + 6);
            gcdmResult.values.put("C4", delta + 7);
            gcdmResult.historyValues.put("title", Arrays.asList("5/08/2014", "6/5/2015", "10/10/2015"));
            gcdmResult.historyValues.put("C1", Arrays.asList(78, 79, 60));
            gcdmResult.historyValues.put("C2", Arrays.asList(54, 55, 56));

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the getDefaultAddComposition request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during getDefaultAddComposition request[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }
        return gcdmResult;
    }
    /**
     * FDR-32
     *
     * @param gasComposition
     * @return
     */
    public GcdmToolbox.GcdmResult getGasComposition(final GasCompositionParameter gasComposition)
    {
        logger.info("GcdmBusinessAccess.getGasComposition: parameters=" + gasComposition.toString());
        final GcdmResult gcdmResult = new GcdmResult();
        Connection con = GcdmToolbox.getConnection(gasComposition.allowDirectConnection);
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
            long delta = gasComposition.uid;
            if (gasComposition.typeDisplay == EnuTypeDisplays.Defaults)
            {
                delta = 10 + gasComposition.uid;;
            }
            if (gasComposition.typeDisplay == EnuTypeDisplays.Minimum)
            {
                delta = 100 + gasComposition.uid;
            };
            if (gasComposition.typeDisplay == EnuTypeDisplays.BlendAlarm)
            {
                delta = 1000 + gasComposition.uid;
            }

            final Calendar c = Calendar.getInstance();
            c.set(Calendar.YEAR, 2015);
            c.set(Calendar.MONTH, 11);

            gcdmResult.values.put("SUPPLYCHAINPOINT", "Defaults");
            // give as a LONG format
            gcdmResult.values.put("EFFECTIVEDATE", Toolbox.sdfJavasscript.format(c.getTimeInMillis()));
            // give as HH:MM format
            gcdmResult.values.put("EFFECTIVETIME", GcdmToolbox.sdfJavasscriptHour.format(c.getTime()));

            // listSupplyChaind
            completeListSupplyChain(con, gcdmResult);


            gcdmResult.values.put("EFFECTIVEDATETIME_ST", GcdmToolbox.sdfEffectiveDate.format(c.getTime()));
            gcdmResult.values.put("SPECIFICGRAVITY", delta + 2);
            gcdmResult.values.put("HEATINGVALUE", delta + 3);
            gcdmResult.values.put("C1", delta + 4);
            gcdmResult.values.put("C2", delta + 5);
            gcdmResult.values.put("C3", delta + 6);
            gcdmResult.values.put("C4", delta + 7);
            gcdmResult.historyValues.put("title", Arrays.asList("5/08/2014", "6/5/2015", "10/10/2015"));
            gcdmResult.historyValues.put("C1", Arrays.asList(78, 79, 60));
            gcdmResult.historyValues.put("C2", Arrays.asList(54, 55, 56));

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

    /** Delete a list of record FDR-15, FDR-50, FDR-53, FDR-55, FDR-57, FDR-58 */
    public GcdmToolbox.GcdmResult deleteListGasComposition(final GasCompositionParameter gasComposition, final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        try {
            final ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);

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
        } catch (final BonitaHomeNotSetException | ServerAPIException | UnknownAPITypeException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during getAPI " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during getAPI : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }

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
        public List<BEvent> listEvents = new ArrayList<BEvent>();

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
                gasCompositionParameter.effectiveDate = GcdmToolbox.sdfEffectiveDate.parse(effectiveDateSt);
            } catch (final Exception e)
            {
                logger.info("NewGasCompositionParameter.fromJson : Bad dateFormat [" + effectiveDateSt + "]");

                gasCompositionParameter.listEvents.add(new BEvent(eventCorrectDateExpected, effectiveDateSt));
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
    public GcdmToolbox.GcdmResult searchListGasComposition(final NewGasCompositionParameter newGasComposition)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        final Connection con = GcdmToolbox.getConnection(newGasComposition.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }

        gcdmResult.status = "Search SupplyChain [" + newGasComposition.supplyChainPoint + "] at date "
                + GcdmToolbox.sdfHuman.format(newGasComposition.effectiveDate);

        // simulation :
        completeNewGasCompositionFields(con, gcdmResult);
        for (final Map<String, Object> oneField : gcdmResult.newGasCompositionFields) {
            gcdmResult.newGasCompositionValues.put((String) oneField.get("id"), Long.valueOf((long) (10 + 50 * Math.random())));
        }
        logger.info("GdmBusinessAccess.deleteListGasComposition - Search " + gcdmResult.status);
        return gcdmResult;
    }

    /**
     * FDR-25 / FDR-30 / FDR-42 / FDR-46, FDR-47 / FDR 51
     *
     * @param newGasComposition
     * @return
     */
    public GcdmToolbox.GcdmResult addNewGasComposition(final NewGasCompositionParameter newGasComposition, final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();

        try
        {
            final ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);

        // FDR-25 & FDR-28 & FDR-29
        gcdmResult.newGasCompositionValues = newGasComposition.allValues;
        gcdmResult.status = "Simulation save is done";

        // FDR-30
        logger.info("Santos.GcdmBusinessAccess.addNewGasComposition : Start process");
        final Map<String, Serializable> variables = new HashMap<String, Serializable>();
        variables.put("gascompositionmap", (Serializable) newGasComposition.allValues);

        final ProcessToolboxResult processToolboxResult = ProcessToolbox.startACase(processAddGasComposition, variables, processAPI);
        gcdmResult.listEvents.addAll(processToolboxResult.listEvents);

        logger.info("GdmBusinessAccess.deleteListGasComposition - Search " + gcdmResult.status);
        } catch (final BonitaHomeNotSetException | ServerAPIException | UnknownAPITypeException e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during getAPI " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during getAPI : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }
        return gcdmResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Conversion factor */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class CalculGravityParameter
    {

        public int maxRecord = 1000;
        public boolean allowDirectConnection = false;

        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public Map<String, Object> values;

        public static CalculGravityParameter getFromJson(final String jsonSt)
        {
            final CalculGravityParameter conversionFactorParameter = new CalculGravityParameter();
            if (jsonSt == null) {
                return conversionFactorParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return conversionFactorParameter;
            }

            conversionFactorParameter.values = jsonHash;
            return conversionFactorParameter;
        }

        public BigDecimal getValue(final String param)
        {
            if (values == null) {
                return BigDecimal.valueOf(0.0);
            }
            return Toolbox.getBigDecimal(values.get(param), BigDecimal.valueOf(0.0));
        }

    }

    /**
     * get conversion factor
     *
     * @param newGasComposition
     * @param processAPI
     * @return
     */
    public GcdmToolbox.GcdmResult getConversionFactors(final CalculGravityParameter calculGravityParameter)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        final Connection con = GcdmToolbox.getConnection(calculGravityParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        String sqlRequest = "";
        try
        {
            sqlRequest = "select c.pgmscode as PGMSCODE, c.value*r.SGConversionFactor as SGVALUE,  c.value*r.HVConversionFactor as HVVALUE "
                    + "from DataPoints dp, CompositionSets cs, Compositions c, R_ConversionFactors r "
                    + "where dp.dpt_uid_pk = cs.cse_dpt_uid_fk"
                    + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                    + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                    + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                    + " and c.elementname_fk = r.elementname_uk"
                    + " order by dp.PointName, c.pgmscode";
            final List<Object> listRequestObject = new ArrayList<Object>();

            gcdmResult.listValues = GcdmToolbox.executeRequest(con, sqlRequest, listRequestObject, calculGravityParameter.maxRecord,
                    calculGravityParameter.formatDateJson);
        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
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
    private void completeGasCompositionHeader(final Connection con, final EnuTypeDisplays typeDisplay, final GcdmToolbox.GcdmResult gcdmResult)
    {
        // FDR-10
        if (typeDisplay == EnuTypeDisplays.Defaults)
        {
            gcdmResult.addHeaderColumns("EFFECTIVEDATE", "Effective Date/Time", typeColumn.date);
            gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
            gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
            gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.text);
            gcdmResult.addHeaderColumns("METHANEC1", "Methane C1", typeColumn.number);
            gcdmResult.addHeaderColumns("ETHANEC2", "Ethane C2", typeColumn.number);
            gcdmResult.addHeaderColumns("PROPANEC3", "Propane C3", typeColumn.number);
            gcdmResult.addHeaderColumns("IBUTANEC4I", "I-Butane C4i", typeColumn.number);
            gcdmResult.addHeaderColumns("NBUTANEC4N", "N-Butane C4n", typeColumn.number);
            gcdmResult.addHeaderColumns("BUTANEC4", "Butane C4", typeColumn.number);
        }
        else if (typeDisplay == EnuTypeDisplays.Minimum)
        {
            // FDR-60
            gcdmResult.addHeaderColumns("EFFECTIVEDATE", "Effective Date/Time", typeColumn.date);
            gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
            gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
            gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.text);
            gcdmResult.addHeaderColumns("METHANEC1", "Methane C1", typeColumn.number);
            gcdmResult.addHeaderColumns("ETHANEC2", "Ethane C2", typeColumn.number);
            gcdmResult.addHeaderColumns("PROPANEC3", "Propane C3", typeColumn.number);
            gcdmResult.addHeaderColumns("IBUTANEC4I", "I-Butane C4i", typeColumn.number);
            gcdmResult.addHeaderColumns("NBUTANEC4N", "N-Butane C4n", typeColumn.number);
            gcdmResult.addHeaderColumns("BUTANEC4", "Butane C4", typeColumn.number);
            gcdmResult.addHeaderColumns("IPENTANEC5", "IPentane C5", typeColumn.number);
            gcdmResult.addHeaderColumns("NEOPENTANE", "Neo_Pentane", typeColumn.number);

        }
        else if (typeDisplay == EnuTypeDisplays.BlendAlarm)
        {
            // FDR-62
            gcdmResult.addHeaderColumns("EFFECTIVEDATE_ST", "Effective Date/Time", typeColumn.date);
            gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
            gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
            gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.text);
            gcdmResult.addHeaderColumns("METHANEC1", "Methane C1", typeColumn.number);
            gcdmResult.addHeaderColumns("ETHANEC2", "Ethane C2", typeColumn.number);
            gcdmResult.addHeaderColumns("PROPANEC3", "Propane C3", typeColumn.number);
            gcdmResult.addHeaderColumns("IBUTANEC4I", "I-Butane C4i", typeColumn.number);
            gcdmResult.addHeaderColumns("NBUTANEC4N", "N-Butane C4n", typeColumn.number);
            gcdmResult.addHeaderColumns("BUTANEC4", "Butane C4", typeColumn.number);
            gcdmResult.addHeaderColumns("IPENTANEC5", "IPentane C5", typeColumn.number);
        }

    }

    // list SupplyChaine FDR-18
    protected static void completeListSupplyChain(final Connection con, final GcdmToolbox.GcdmResult gcdmResult)
    {
        final List<Map<String, Object>> listRecords = new ArrayList<Map<String, Object>>();
        HashMap<String, Object> option = new HashMap<String, Object>();
        option.put("id", "1");
        option.put("name", "First value");
        listRecords.add(option);

        option = new HashMap<String, Object>();
        option.put("id", "Defaults");
        option.put("name", "Defaults");
        listRecords.add(option);

        gcdmResult.listsSelect.put("LISTSUPPLYCHAIN", listRecords);
    }

    private void completeNewGasCompositionFields(final Connection con, final GcdmToolbox.GcdmResult gcdmResult)
    {
        // FDR-17
        gcdmResult.addEditFields("C1", "C1 (mole %)", typeColumn.number, true, false, 70, 100);
        gcdmResult.addEditFields("C2", "C2 (mole %)", typeColumn.number, true, false, 0, 20);
        gcdmResult.addEditFields("C3", "C3 (mole %)", typeColumn.number, false, false, 0, 6);
        gcdmResult.addEditFields("C4", "C4 (mole %)", typeColumn.number, false, true, 0, 3);
        gcdmResult.addEditFields("C4I", "C4i (mole %)", typeColumn.number, false, false, 0, 1.5);
        gcdmResult.addEditFields("C5", "C5 (mole %)", typeColumn.number, false, true, 0, 0.6);
        gcdmResult.addEditFields("C5I", "C5i (mole %)", typeColumn.number, false, false, 0, 0.3);
        gcdmResult.addEditFields("C5N", "C5n (mole %)", typeColumn.number, false, false, 0, 0.3);
        gcdmResult.addEditFields("C6PLUS", "C6+ (ppm)", typeColumn.number, false, false, 0, 3000);
        gcdmResult.addEditFields("C8", "C8 (ppb)", typeColumn.number, false, false, 0, 2000);
        gcdmResult.addEditFields("C9", "C9 (ppb)", typeColumn.number, false, false, 0, 2000);
        gcdmResult.addEditFields("CO2", "CO2 (model %)", typeColumn.number, false, false, 0, 30);
        gcdmResult.addEditFields("H2S", "H2S(ppm)", typeColumn.number, false, false, 0, 10);
        gcdmResult.addEditFields("N2", "N2 (mole %)", typeColumn.number, false, false, 0, 50);
        gcdmResult.addEditFields("H2O", "H2O (mg/Sm3)", typeColumn.number, false, false, 0, 650);
        gcdmResult.addEditFields("TSU", "TSU (ppm)", typeColumn.number, false, false, 0, 20);
        gcdmResult.addEditFields("BEN", "BEN (ppb)", typeColumn.number, false, false, 0, 5000);
        gcdmResult.addEditFields("CYC", "CYC (ppb)", typeColumn.number, false, false, 0, 5000);
        gcdmResult.addEditFields("SPEGRA", "Specific Gravity", typeColumn.text, false, true, null, null);
        gcdmResult.addEditFields("HEATING", "Heating Value (MJ/Sm3)", typeColumn.text, false, true, null, null);

    }



}
