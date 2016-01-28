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

        public Long duid;

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
            gasCompositionParameter.duid = Toolbox.getLong(jsonHash.get("DUID"), null);
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
            // FDR-07
            if (gasComposition.typeDisplay == EnuTypeDisplays.Defaults)
            {
                //                sqlRequest = "select c.cpn_cse_uid_fk as duid, c.EffectiveDate, dp.PointName, 'SpecificGravity '  as SPECIFICGRAVITY, 'HeatingValue'      as HeatingValue, c.value,"
                //                        + " 12 as METHANEC1"
                //                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                //                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                //                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                //                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                //                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                //                        + " order by dp.PointName, c.pgmscode";
                //

                // FDR-06
                sqlRequest = "select distinct"
                        +
                        "    Q1.comp_effectivedate as \"EFFECTIVEDATE\",	"
                        +
                        "    Q1.data_point as \"SUPPLYCHAINEPOINT\","
                        +
                        "    (nvl(Q2.C1_SG, 0) + nvl(Q2.C2_SG, 0) + nvl(Q2.C3_SG, 0) + nvl(Q2.C4I_SG, 0) + nvl(Q2.C4N_SG, 0) + nvl(Q2.C5I_SG, 0) + nvl(Q2.C5N_SG, 0) + nvl(Q2.C6_SG, 0) + nvl(Q2.C7_SG, 0) + nvl(Q2.C8_SG, 0) + nvl(Q2.C9_SG, 0) + nvl(Q2.C10_SG, 0) + nvl(Q2.CO2_SG, 0) + nvl(Q2.H2_SG, 0) + nvl(Q2.H2S_SG, 0) + nvl(Q2.He_SG, 0) + nvl(Q2.N2_SG, 0) + nvl(Q2.O2_SG, 0)) as \"SPECIFICGRAVITY\","
                        +
                        "    (nvl(Q2.C1_HV, 0) + nvl(Q2.C2_HV, 0) + nvl(Q2.C3_HV, 0) + nvl(Q2.C4I_HV, 0) + nvl(Q2.C4N_HV, 0) + nvl(Q2.C5I_HV, 0) + nvl(Q2.C5N_HV, 0) + nvl(Q2.C6_HV, 0) + nvl(Q2.C7_HV, 0) + nvl(Q2.C8_HV, 0) + nvl(Q2.C9_HV, 0) + nvl(Q2.C10_HV, 0) + nvl(Q2.CO2_HV, 0) + nvl(Q2.H2_HV, 0) + nvl(Q2.H2S_HV, 0) + nvl(Q2.He_HV, 0) + nvl(Q2.N2_HV, 0) + nvl(Q2.O2_HV, 0) ) as \"HEATINGVALUE\",  "
                        +
                        "    Q2.C1  as \"COLUMN_1\",  "
                        +
                        "    Q2.C2  as \"COLUMN_2\",  "
                        +
                        "    Q2.C3  as \"COLUMN_3\",   "
                        +
                        "    Q2.C4I  as \"COLUMN_4\",   "
                        +
                        "    Q2.C4N  as \"COLUMN_5\",   "
                        +
                        "    Q2.C5I  as \"COLUMN_6\",  "
                        +
                        "    Q2.C5N  as \"COLUMN_7\",   "
                        +
                        "    Q2.C6  as \"COLUMN_8\","
                        +
                        "    Q2.C7  as \"COLUMN_9\",    "
                        +
                        "    Q2.C8  as \"COLUMN_10\",  "
                        +
                        "    Q2.C9  as \"COLUMN_11\","
                        +
                        "    Q2.C10  as \"COLUMN_12\",    "
                        +
                        "    Q2.CO2  as \"COLUMN_13\","
                        +
                        "    Q2.H2  as \"COLUMN_14\","
                        +
                        "    Q2.H2S  as \"COLUMN_15\","
                        +
                        "    Q2.He  as \"COLUMN_16\",        "
                        +
                        "    Q2.N2  as \"COLUMN_17\","
                        +
                        "    Q2.O2  as \"COLUMN_18\""
                        +

                        /*
                         * "    Q2.C1  as \"Methane C1 mole %\",  " +
                         * "    Q2.C2  as \"Ethane C2 mole %\",  " +
                         * "    Q2.C3  as \"Propane C3 mole %\",   " +
                         * "    Q2.C4I  as \"i-Butane C4I mole %\",   " +
                         * "    Q2.C4N  as \"n-Butane C4N mole %\",   " +
                         * "    Q2.C5I  as \"i-Pentane C5I mole %\",  " +
                         * "    Q2.C5N  as \"n-Pentane C5N mole %\",   " +
                         * "    Q2.C6  as \"Hexane C6 ppm\"," +
                         * "    Q2.C7  as \"Water C7 mg/Sm3\",    " +
                         * "    Q2.C8  as \"Octane C8 ppb\",  " +
                         * "    Q2.C9  as \"Nonane C9 ppb\"," +
                         * "    Q2.C10  as \"Total Sulphur C10 ppm\",    " +
                         * "    Q2.CO2  as \"Carbon Dioxide CO2 mole %\"," +
                         * "    Q2.H2  as \"Benzene H2 ppb\"," +
                         * "    Q2.H2S  as \"Hydrogen Sulphide H2S ppm\"," +
                         * "    Q2.He  as \"Neo-Pentane He NEO\",        " +
                         * "    Q2.N2  as \"Nitrogen N2 mole %\"," +
                         * "    Q2.O2  as \"CycloHexane O2 ppb\"" +
                         */
                        " from"
                        +
                        " ("
                        +
                        " Select "
                        +
                        " data_point_name as data_point, "
                        +
                        " comp_effectivedate as comp_effectivedate"
                        +
                        " from"
                        +
                        " ("
                        +
                        " Select"
                        +
                        "    dp.pointname as data_point_name,"
                        +
                        "    c.effectivedate as comp_effectivedate"
                        +
                        " From"
                        +
                        "    gcdm.compositions  				c,"
                        +
                        "    gcdm.compositionsets      			cs,"
                        +
                        "    gcdm.datapoints  				dp"
                        +
                        " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk"
                        +
                        "                    and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        +
                        "            	    and dp.recordstatus = 'CURRENT' and dp.effectivedate < sysdate and dp.enddate is null"
                        +
                        "		    and cs.effectivedate = (select max(effectivedate) from gcdm.CompositionSets where c.recordstatus = 'CURRENT' and cs.cse_uid_pk = c.cpn_cse_uid_fk and effectivedate < sysdate and enddate is null ) "
                        +
                        "		    and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate and c.effectivedate < sysdate and c.enddate is null)"
                        +
                        " group by data_point_name, comp_effectivedate "
                        +
                        " ) Q1,"
                        +
                        " ("
                        +
                        " Select"
                        +
                        "    DP.pointname   as data_point,"
                        +
                        "    c.effectivedate as comp_effectivedate,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , C.value ) ) as C1,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , C.value ) ) as C2,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , C.value ) ) as C3,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , C.value ) ) as C4I,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , C.value ) ) as C4N,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , C.value ) ) as C5I,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , C.value ) ) as C5N,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , C.value ) ) as C6,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , C.value ) ) as C7,"
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , C.value ) ) as C8,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , C.value ) ) as C9,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , C.value ) ) as C10,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , C.value ) ) as CO2,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , C.value ) ) as H2,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , C.value ) ) as H2S,"
                        +
                        "    sum( decode ( C.pgmscode, 'He' , C.value ) ) as He,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , C.value ) ) as N2,"
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , C.value ) ) as O2,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C1_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C3_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4I_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4N_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5I_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5N_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C6_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C7_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C8_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C9_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C10_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as CO2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2S_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'He' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as He_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as N2_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as O2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C1_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C3_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4I_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4N_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5I_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5N_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C6_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C7_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C8_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C9_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C10_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as CO2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2_SG, "
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2S_SG,  "
                        +
                        "    sum( decode ( C.pgmscode, 'He' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as He_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as N2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as O2_SG       "
                        +
                        " From"
                        +
                        "    gcdm.compositions  				c,"
                        +
                        "    gcdm.compositionsets      			cs,"
                        +
                        "    gcdm.datapoints  				dp,"
                        +
                        "    gcdm.r_conversionfactors			rc"
                        +
                        " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk"
                        +
                        "    and cs.cse_uid_pk = c.cpn_cse_uid_fk"
                        +
                        "    and c.pgmscode = rc.pgmscode  "
                        +
                        "    and dp.recordstatus = 'CURRENT' and dp.effectivedate < sysdate and dp.enddate is null"
                        +
                        "    and cs.effectivedate = (select max(effectivedate) from gcdm.CompositionSets where c.recordstatus = 'CURRENT' and cs.cse_uid_pk = c.cpn_cse_uid_fk and effectivedate < sysdate and enddate is null ) "
                        +
                        "    and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate and c.effectivedate < sysdate and c.enddate is null"
                        +
                        "    and rc.recordstatus = 'CURRENT' and rc.effectivedate <= c.effectivedate and rc.effectivedate < sysdate and rc.enddate is null" +
                        " Group by" +
                        "    DP.pointname, c.effectivedate    " +
                        " ) Q2" +
                        " where Q1.data_point = Q2.data_point" +
                        "    and Q1.comp_effectivedate = Q2.comp_effectivedate" +
                        " order by 1";
            }

            // FDR-60
            if (gasComposition.typeDisplay == EnuTypeDisplays.Minimum)
            {
                //                sqlRequest = "select c.cpn_cse_uid_fk as duid, c.EffectiveDate, dp.PointName, 'SpecificGravity'  as SPECIFICGRAVITY, 'HeatingValue'      as HeatingValue, c.value"
                //                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                //                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                //                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                //                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                //                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                //                        + " order by dp.PointName, c.pgmscode";
                // FDR-06
                sqlRequest = "select distinct"
                        +
                        "    Q1.comp_effectivedate as \"EFFECTIVEDATE\",	"
                        +
                        "    Q1.data_point as \"SUPPLYCHAINEPOINT\","
                        +
                        "    (nvl(Q2.C1_SG, 0) + nvl(Q2.C2_SG, 0) + nvl(Q2.C3_SG, 0) + nvl(Q2.C4I_SG, 0) + nvl(Q2.C4N_SG, 0) + nvl(Q2.C5I_SG, 0) + nvl(Q2.C5N_SG, 0) + nvl(Q2.C6_SG, 0) + nvl(Q2.C7_SG, 0) + nvl(Q2.C8_SG, 0) + nvl(Q2.C9_SG, 0) + nvl(Q2.C10_SG, 0) + nvl(Q2.CO2_SG, 0) + nvl(Q2.H2_SG, 0) + nvl(Q2.H2S_SG, 0) + nvl(Q2.He_SG, 0) + nvl(Q2.N2_SG, 0) + nvl(Q2.O2_SG, 0)) as \"SPECIFICGRAVITY\","
                        +
                        "    (nvl(Q2.C1_HV, 0) + nvl(Q2.C2_HV, 0) + nvl(Q2.C3_HV, 0) + nvl(Q2.C4I_HV, 0) + nvl(Q2.C4N_HV, 0) + nvl(Q2.C5I_HV, 0) + nvl(Q2.C5N_HV, 0) + nvl(Q2.C6_HV, 0) + nvl(Q2.C7_HV, 0) + nvl(Q2.C8_HV, 0) + nvl(Q2.C9_HV, 0) + nvl(Q2.C10_HV, 0) + nvl(Q2.CO2_HV, 0) + nvl(Q2.H2_HV, 0) + nvl(Q2.H2S_HV, 0) + nvl(Q2.He_HV, 0) + nvl(Q2.N2_HV, 0) + nvl(Q2.O2_HV, 0) ) as \"HEATINGVALUE\",  "
                        +
                        "    Q2.C1  as \"COLUMN_1\",  "
                        +
                        "    Q2.C2  as \"COLUMN_2\",  "
                        +
                        "    Q2.C3  as \"COLUMN_3\",   "
                        +
                        "    Q2.C4I  as \"COLUMN_4\",   "
                        +
                        "    Q2.C4N  as \"COLUMN_5\",   "
                        +
                        "    Q2.C5I  as \"COLUMN_6\",  "
                        +
                        "    Q2.C5N  as \"COLUMN_7\",   "
                        +
                        "    Q2.C6  as \"COLUMN_8\","
                        +
                        "    Q2.C7  as \"COLUMN_9\",    "
                        +
                        "    Q2.C8  as \"COLUMN_10\",  "
                        +
                        "    Q2.C9  as \"COLUMN_11\","
                        +
                        "    Q2.C10  as \"COLUMN_12\",    "
                        +
                        "    Q2.CO2  as \"COLUMN_13\","
                        +
                        "    Q2.H2  as \"COLUMN_14\","
                        +
                        "    Q2.H2S  as \"COLUMN_15\","
                        +
                        "    Q2.He  as \"COLUMN_16\",        "
                        +
                        "    Q2.N2  as \"COLUMN_17\","
                        +
                        "    Q2.O2  as \"COLUMN_18\""
                        +

                        /*
                         * "    Q2.C1  as \"Methane C1 mole %\",  " +
                         * "    Q2.C2  as \"Ethane C2 mole %\",  " +
                         * "    Q2.C3  as \"Propane C3 mole %\",   " +
                         * "    Q2.C4I  as \"i-Butane C4I mole %\",   " +
                         * "    Q2.C4N  as \"n-Butane C4N mole %\",   " +
                         * "    Q2.C5I  as \"i-Pentane C5I mole %\",  " +
                         * "    Q2.C5N  as \"n-Pentane C5N mole %\",   " +
                         * "    Q2.C6  as \"Hexane C6 ppm\"," +
                         * "    Q2.C7  as \"Water C7 mg/Sm3\",    " +
                         * "    Q2.C8  as \"Octane C8 ppb\",  " +
                         * "    Q2.C9  as \"Nonane C9 ppb\"," +
                         * "    Q2.C10  as \"Total Sulphur C10 ppm\",    " +
                         * "    Q2.CO2  as \"Carbon Dioxide CO2 mole %\"," +
                         * "    Q2.H2  as \"Benzene H2 ppb\"," +
                         * "    Q2.H2S  as \"Hydrogen Sulphide H2S ppm\"," +
                         * "    Q2.He  as \"Neo-Pentane He NEO\",        " +
                         * "    Q2.N2  as \"Nitrogen N2 mole %\"," +
                         * "    Q2.O2  as \"CycloHexane O2 ppb\"" +
                         */
                        " from"
                        +
                        " ("
                        +
                        " Select "
                        +
                        " data_point_name as data_point, "
                        +
                        " comp_effectivedate as comp_effectivedate"
                        +
                        " from"
                        +
                        " ("
                        +
                        " Select"
                        +
                        "    dp.pointname as data_point_name,"
                        +
                        "    c.effectivedate as comp_effectivedate"
                        +
                        " From"
                        +
                        "    gcdm.compositions  				c,"
                        +
                        "    gcdm.compositionsets      			cs,"
                        +
                        "    gcdm.datapoints  				dp"
                        +
                        " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk"
                        +
                        "                    and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        +
                        "            	    and dp.recordstatus = 'CURRENT' and dp.effectivedate < sysdate and dp.enddate is null"
                        +
                        "		    and cs.effectivedate = (select max(effectivedate) from gcdm.CompositionSets where c.recordstatus = 'CURRENT' and cs.cse_uid_pk = c.cpn_cse_uid_fk and effectivedate < sysdate and enddate is null ) "
                        +
                        "		    and c.CompositionUse='MINIMUM' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate and c.effectivedate < sysdate and c.enddate is null)"
                        +
                        " group by data_point_name, comp_effectivedate "
                        +
                        " ) Q1,"
                        +
                        " ("
                        +
                        " Select"
                        +
                        "    DP.pointname   as data_point,"
                        +
                        "    c.effectivedate as comp_effectivedate,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , C.value ) ) as C1,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , C.value ) ) as C2,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , C.value ) ) as C3,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , C.value ) ) as C4I,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , C.value ) ) as C4N,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , C.value ) ) as C5I,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , C.value ) ) as C5N,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , C.value ) ) as C6,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , C.value ) ) as C7,"
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , C.value ) ) as C8,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , C.value ) ) as C9,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , C.value ) ) as C10,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , C.value ) ) as CO2,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , C.value ) ) as H2,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , C.value ) ) as H2S,"
                        +
                        "    sum( decode ( C.pgmscode, 'He' , C.value ) ) as He,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , C.value ) ) as N2,"
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , C.value ) ) as O2,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C1_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C3_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4I_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4N_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5I_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5N_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C6_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C7_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C8_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C9_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C10_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as CO2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2S_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'He' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as He_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as N2_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as O2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C1_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C3_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4I_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4N_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5I_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5N_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C6_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C7_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C8_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C9_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C10_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as CO2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2_SG, "
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2S_SG,  "
                        +
                        "    sum( decode ( C.pgmscode, 'He' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as He_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as N2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as O2_SG       "
                        +
                        " From"
                        +
                        "    gcdm.compositions  				c,"
                        +
                        "    gcdm.compositionsets      			cs,"
                        +
                        "    gcdm.datapoints  				dp,"
                        +
                        "    gcdm.r_conversionfactors			rc"
                        +
                        " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk"
                        +
                        "    and cs.cse_uid_pk = c.cpn_cse_uid_fk"
                        +
                        "    and c.pgmscode = rc.pgmscode  "
                        +
                        "    and dp.recordstatus = 'CURRENT' and dp.effectivedate < sysdate and dp.enddate is null"
                        +
                        "    and cs.effectivedate = (select max(effectivedate) from gcdm.CompositionSets where c.recordstatus = 'CURRENT' and cs.cse_uid_pk = c.cpn_cse_uid_fk and effectivedate < sysdate and enddate is null ) "
                        +
                        "    and c.CompositionUse='MINIMUM' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate and c.effectivedate < sysdate and c.enddate is null"
                        +
                        "    and rc.recordstatus = 'CURRENT' and rc.effectivedate <= c.effectivedate and rc.effectivedate < sysdate and rc.enddate is null" +
                        " Group by" +
                        "    DP.pointname, c.effectivedate    " +
                        " ) Q2" +
                        " where Q1.data_point = Q2.data_point" +
                        "    and Q1.comp_effectivedate = Q2.comp_effectivedate" +
                        " order by 1";
            }
            // FDR-62
            if (gasComposition.typeDisplay == EnuTypeDisplays.BlendAlarm)
            {
                //                sqlRequest = "select c.cpn_cse_uid_fk as DUID, c.EffectiveDate, dp.PointName, 'SpecificGravity' as SPECIFICGRAVITY, 'HeatingValue'      as HeatingValue, c.value"
                //                        + " from DataPoints dp, CompositionSets cs, Compositions c"
                //                        + " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk "
                //                        + " and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                //                        + " and cs.effectivedate = (select max(effectivedate) from CompositionSets where c.recordstatus = 'CURRENT')"
                //                        + " and c.CompositionUse='DEFAULT' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate"
                //                        + " order by dp.PointName, c.pgmscode";
                // FDR-06
                sqlRequest = "select distinct"
                        +
                        "    Q1.comp_effectivedate as \"EFFECTIVEDATE\",	"
                        +
                        "    Q1.data_point as \"SUPPLYCHAINEPOINT\","
                        +
                        "    (nvl(Q2.C1_SG, 0) + nvl(Q2.C2_SG, 0) + nvl(Q2.C3_SG, 0) + nvl(Q2.C4I_SG, 0) + nvl(Q2.C4N_SG, 0) + nvl(Q2.C5I_SG, 0) + nvl(Q2.C5N_SG, 0) + nvl(Q2.C6_SG, 0) + nvl(Q2.C7_SG, 0) + nvl(Q2.C8_SG, 0) + nvl(Q2.C9_SG, 0) + nvl(Q2.C10_SG, 0) + nvl(Q2.CO2_SG, 0) + nvl(Q2.H2_SG, 0) + nvl(Q2.H2S_SG, 0) + nvl(Q2.He_SG, 0) + nvl(Q2.N2_SG, 0) + nvl(Q2.O2_SG, 0)) as \"SPECIFICGRAVITY\","
                        +
                        "    (nvl(Q2.C1_HV, 0) + nvl(Q2.C2_HV, 0) + nvl(Q2.C3_HV, 0) + nvl(Q2.C4I_HV, 0) + nvl(Q2.C4N_HV, 0) + nvl(Q2.C5I_HV, 0) + nvl(Q2.C5N_HV, 0) + nvl(Q2.C6_HV, 0) + nvl(Q2.C7_HV, 0) + nvl(Q2.C8_HV, 0) + nvl(Q2.C9_HV, 0) + nvl(Q2.C10_HV, 0) + nvl(Q2.CO2_HV, 0) + nvl(Q2.H2_HV, 0) + nvl(Q2.H2S_HV, 0) + nvl(Q2.He_HV, 0) + nvl(Q2.N2_HV, 0) + nvl(Q2.O2_HV, 0) ) as \"HEATINGVALUE\",  "
                        +
                        "    Q2.C1  as \"COLUMN_1\",  "
                        +
                        "    Q2.C2  as \"COLUMN_2\",  "
                        +
                        "    Q2.C3  as \"COLUMN_3\",   "
                        +
                        "    Q2.C4I  as \"COLUMN_4\",   "
                        +
                        "    Q2.C4N  as \"COLUMN_5\",   "
                        +
                        "    Q2.C5I  as \"COLUMN_6\",  "
                        +
                        "    Q2.C5N  as \"COLUMN_7\",   "
                        +
                        "    Q2.C6  as \"COLUMN_8\","
                        +
                        "    Q2.C7  as \"COLUMN_9\",    "
                        +
                        "    Q2.C8  as \"COLUMN_10\",  "
                        +
                        "    Q2.C9  as \"COLUMN_11\","
                        +
                        "    Q2.C10  as \"COLUMN_12\",    "
                        +
                        "    Q2.CO2  as \"COLUMN_13\","
                        +
                        "    Q2.H2  as \"COLUMN_14\","
                        +
                        "    Q2.H2S  as \"COLUMN_15\","
                        +
                        "    Q2.He  as \"COLUMN_16\",        "
                        +
                        "    Q2.N2  as \"COLUMN_17\","
                        +
                        "    Q2.O2  as \"COLUMN_18\""
                        +

                        /*
                         * "    Q2.C1  as \"Methane C1 mole %\",  " +
                         * "    Q2.C2  as \"Ethane C2 mole %\",  " +
                         * "    Q2.C3  as \"Propane C3 mole %\",   " +
                         * "    Q2.C4I  as \"i-Butane C4I mole %\",   " +
                         * "    Q2.C4N  as \"n-Butane C4N mole %\",   " +
                         * "    Q2.C5I  as \"i-Pentane C5I mole %\",  " +
                         * "    Q2.C5N  as \"n-Pentane C5N mole %\",   " +
                         * "    Q2.C6  as \"Hexane C6 ppm\"," +
                         * "    Q2.C7  as \"Water C7 mg/Sm3\",    " +
                         * "    Q2.C8  as \"Octane C8 ppb\",  " +
                         * "    Q2.C9  as \"Nonane C9 ppb\"," +
                         * "    Q2.C10  as \"Total Sulphur C10 ppm\",    " +
                         * "    Q2.CO2  as \"Carbon Dioxide CO2 mole %\"," +
                         * "    Q2.H2  as \"Benzene H2 ppb\"," +
                         * "    Q2.H2S  as \"Hydrogen Sulphide H2S ppm\"," +
                         * "    Q2.He  as \"Neo-Pentane He NEO\",        " +
                         * "    Q2.N2  as \"Nitrogen N2 mole %\"," +
                         * "    Q2.O2  as \"CycloHexane O2 ppb\"" +
                         */
                        " from"
                        +
                        " ("
                        +
                        " Select "
                        +
                        " data_point_name as data_point, "
                        +
                        " comp_effectivedate as comp_effectivedate"
                        +
                        " from"
                        +
                        " ("
                        +
                        " Select"
                        +
                        "    dp.pointname as data_point_name,"
                        +
                        "    c.effectivedate as comp_effectivedate"
                        +
                        " From"
                        +
                        "    gcdm.compositions  				c,"
                        +
                        "    gcdm.compositionsets      			cs,"
                        +
                        "    gcdm.datapoints  				dp"
                        +
                        " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk"
                        +
                        "                    and cs.cse_uid_pk = c.cpn_cse_uid_fk "
                        +
                        "            	    and dp.recordstatus = 'CURRENT' and dp.effectivedate < sysdate and dp.enddate is null"
                        +
                        "		    and cs.effectivedate = (select max(effectivedate) from gcdm.CompositionSets where c.recordstatus = 'CURRENT' and cs.cse_uid_pk = c.cpn_cse_uid_fk and effectivedate < sysdate and enddate is null ) "
                        +
                        "		    and c.CompositionUse='ALARM' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate and c.effectivedate < sysdate and c.enddate is null)"
                        +
                        " group by data_point_name, comp_effectivedate "
                        +
                        " ) Q1,"
                        +
                        " ("
                        +
                        " Select"
                        +
                        "    DP.pointname   as data_point,"
                        +
                        "    c.effectivedate as comp_effectivedate,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , C.value ) ) as C1,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , C.value ) ) as C2,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , C.value ) ) as C3,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , C.value ) ) as C4I,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , C.value ) ) as C4N,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , C.value ) ) as C5I,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , C.value ) ) as C5N,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , C.value ) ) as C6,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , C.value ) ) as C7,"
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , C.value ) ) as C8,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , C.value ) ) as C9,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , C.value ) ) as C10,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , C.value ) ) as CO2,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , C.value ) ) as H2,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , C.value ) ) as H2S,"
                        +
                        "    sum( decode ( C.pgmscode, 'He' , C.value ) ) as He,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , C.value ) ) as N2,"
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , C.value ) ) as O2,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C1_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C3_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4I_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4N_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5I_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5N_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C6_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C7_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C8_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C9_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C10_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as CO2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2S_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'He' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as He_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as N2_HV, "
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.hvconversionfactor > 0  THEN RC.hvconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.hvconversionfactor > 0 THEN RC.hvconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as O2_HV,"
                        +
                        "    sum( decode ( C.pgmscode, 'C1' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C1_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C3' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C3_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4I_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C4N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C4N_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5I' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5I_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C5N' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C5N_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C6' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C6_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C7' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C7_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C8' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C8_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C9' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C9_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'C10' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as C10_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'CO2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as CO2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'H2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2_SG, "
                        +
                        "    sum( decode ( C.pgmscode, 'H2S' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as H2S_SG,  "
                        +
                        "    sum( decode ( C.pgmscode, 'He' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as He_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'N2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as N2_SG,"
                        +
                        "    sum( decode ( C.pgmscode, 'O2' , CASE WHEN C.unitofmeasure_fk='ppm' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000 "
                        +
                        "	                                   WHEN C.unitofmeasure_fk='ppb' and RC.sgconversionfactor > 0  THEN RC.sgconversionfactor*C.value/1000000000"
                        +
                        "					   WHEN C.unitofmeasure_fk not in ('ppb', 'ppm') and RC.sgconversionfactor > 0 THEN RC.sgconversionfactor*C.value/100"
                        +
                        "					   ELSE 0 END)) as O2_SG       "
                        +
                        " From"
                        +
                        "    gcdm.compositions  				c,"
                        +
                        "    gcdm.compositionsets      			cs,"
                        +
                        "    gcdm.datapoints  				dp,"
                        +
                        "    gcdm.r_conversionfactors			rc"
                        +
                        " where dp.dpt_uid_pk = cs.cse_dpt_uid_fk"
                        +
                        "    and cs.cse_uid_pk = c.cpn_cse_uid_fk"
                        +
                        "    and c.pgmscode = rc.pgmscode  "
                        +
                        "    and dp.recordstatus = 'CURRENT' and dp.effectivedate < sysdate and dp.enddate is null"
                        +
                        "    and cs.effectivedate = (select max(effectivedate) from gcdm.CompositionSets where c.recordstatus = 'CURRENT' and cs.cse_uid_pk = c.cpn_cse_uid_fk and effectivedate < sysdate and enddate is null ) "
                        +
                        "    and c.CompositionUse='ALARM' and c.recordstatus = 'CURRENT' and cs.effectivedate <= c.effectivedate and c.effectivedate < sysdate and c.enddate is null"
                        +
                        "    and rc.recordstatus = 'CURRENT' and rc.effectivedate <= c.effectivedate and rc.effectivedate < sysdate and rc.enddate is null" +
                        " Group by" +
                        "    DP.pointname, c.effectivedate    " +
                        " ) Q2" +
                        " where Q1.data_point = Q2.data_point" +
                        "    and Q1.comp_effectivedate = Q2.comp_effectivedate" +
                        " order by 1";
            }
            final List<Object> listRequestObject = new ArrayList<Object>();

            gcdmResult.listValues = GcdmToolbox.executeRequest(con, sqlRequest, listRequestObject, gasComposition.maxRecord, gasComposition.formatDateJson);

            // Simulation : just remplace some value
            if (gcdmResult.listValues.size() > 1) {
                gcdmResult.listValues.get(0).put("DUID", 3);
                gcdmResult.listValues.get(0).put("SUPPLYCHAINEPOINT", gasComposition.typeDisplay.toString());
            }
            if (gcdmResult.listValues.size() > 2) {
                gcdmResult.listValues.get(1).put("DUID", 3);
            }
            for (final Map<String, Object> oneValue : gcdmResult.listValues)
            {
                oneValue.put("SPECIFICGRAVITY", oneValue.get("SPECIFICGRAVITY") + " " + oneValue.get("DUID"));
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
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };

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
        final Connection con = GcdmToolbox.getConnection(gasComposition.allowDirectConnection);
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
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };

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
        final Connection con = GcdmToolbox.getConnection(gasComposition.allowDirectConnection);
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
            long delta = gasComposition.duid;
            if (gasComposition.typeDisplay == EnuTypeDisplays.Defaults)
            {
                delta = 10 + gasComposition.duid;;
            }
            if (gasComposition.typeDisplay == EnuTypeDisplays.Minimum)
            {
                delta = 100 + gasComposition.duid;
            };
            if (gasComposition.typeDisplay == EnuTypeDisplays.BlendAlarm)
            {
                delta = 1000 + gasComposition.duid;
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
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };

        return gcdmResult;
    }

    /** Delete a list of record FDR-15, FDR-50, FDR-53, FDR-55, FDR-57, FDR-58 */
    public GcdmToolbox.GcdmResult deleteListGasComposition(final GasCompositionParameter gasComposition, final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
        try {
            final ProcessAPI processAPI = TenantAPIAccessor.getProcessAPI(apiSession);

            gcdmResult.status = "Delete list DUID [";
            String isInThePast = "";
            if (gasComposition.listToDelete != null)
            {
                for (final Object uidObj : gasComposition.listToDelete)
                {
                    final Long duid = Toolbox.getLong(uidObj, null);
                    if (duid == null)
                    {
                        logger.severe("We receive a non LONG value is list [" + uidObj + "] list : [" + gasComposition.listToDelete + "]");
                        continue;
                    }
                    if (duid % 3 == 0) {
                        isInThePast += duid.toString();
                    }

                    gcdmResult.status += (duid == null ? "null" : duid.toString()) + ",";
                }
            } else {
                gcdmResult.status += " <null list>";
            }
            gcdmResult.status += "]";
            // FDR-55 Check if all Effective date in this list are In the futur, else refuse
            if (isInThePast.length() > 0)
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
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };

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
        // release the connection to the datasource
        try {
            con.close();
        } catch (final Exception e) {
        };

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

            //            Methane C1 mole %
            //            Ethane C2 mole %
            //            Propane C3 mole %
            //            i-Butane C4I mole %
            //            n-Butane C4N mole %
            //            i-Pentane C5I mole %
            //            n-Pentane C5N mole %
            //            Hexane C6 ppm
            //            Water C7 mg/Sm3
            //            Octane C8 ppb
            //            Nonane C9 ppb
            //            Total Sulphur C10 ppm
            //            Carbon Dioxide CO2 mole %
            //            Benzene H2 ppb
            //            Hydrogen Sulphide H2S ppm
            //            Neo-Pentane He NEO
            //            Nitrogen N2 mole %
            //            CycloHexane O2 ppb

            gcdmResult.addHeaderColumns("EFFECTIVEDATE", "Effective Date/Time", typeColumn.date);
            gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
            gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
            gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.text);
            gcdmResult.addHeaderColumns("COLUMN_1", "Methane C1 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_2", "Ethane C2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_3", "Propane C3 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_4", "i-Butane C4I mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_5", "n-Butane C4N mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_6", "i-Pentane C5I mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_7", "n-Pentane C5N mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_8", "Hexane C6 ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_9", "Water C7 mg/Sm3", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_10", "Octane C8 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_11", "Nonane C9 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_12", "Total Sulphur C10 ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_13", "Carbon Dioxide CO2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_14", "Benzene H2 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_15", "Hydrogen Sulphide H2S ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_16", "Neo-Pentane He NEO", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_17", "Nitrogen N2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_18", "CycloHexane O2 ppb", typeColumn.number);

            //            gcdmResult.addHeaderColumns("EFFECTIVEDATE", "Effective Date/Time", typeColumn.date);
            //            gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
            //            gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
            //            gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.text);
            //            gcdmResult.addHeaderColumns("METHANEC1", "Methane C1", typeColumn.number);
            //            gcdmResult.addHeaderColumns("ETHANEC2", "Ethane C2", typeColumn.number);
            //            gcdmResult.addHeaderColumns("PROPANEC3", "Propane C3", typeColumn.number);
            //            gcdmResult.addHeaderColumns("IBUTANEC4I", "I-Butane C4i", typeColumn.number);
            //            gcdmResult.addHeaderColumns("NBUTANEC4N", "N-Butane C4n", typeColumn.number);
            //            gcdmResult.addHeaderColumns("BUTANEC4", "Butane C4xxxxxxxxxx", typeColumn.number);
        }
        else if (typeDisplay == EnuTypeDisplays.Minimum)
        {
            gcdmResult.addHeaderColumns("EFFECTIVEDATE", "Effective Date/Time", typeColumn.date);
            gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
            gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
            gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.text);
            gcdmResult.addHeaderColumns("COLUMN_1", "Methane C1 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_2", "Ethane C2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_3", "Propane C3 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_4", "i-Butane C4I mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_5", "n-Butane C4N mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_6", "i-Pentane C5I mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_7", "n-Pentane C5N mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_8", "Hexane C6 ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_9", "Water C7 mg/Sm3", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_10", "Octane C8 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_11", "Nonane C9 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_12", "Total Sulphur C10 ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_13", "Carbon Dioxide CO2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_14", "Benzene H2 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_15", "Hydrogen Sulphide H2S ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_16", "Neo-Pentane He NEO", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_17", "Nitrogen N2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_18", "CycloHexane O2 ppb", typeColumn.number);

        }
        else if (typeDisplay == EnuTypeDisplays.BlendAlarm)
        {
            gcdmResult.addHeaderColumns("EFFECTIVEDATE", "Effective Date/Time", typeColumn.date);
            gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain point", typeColumn.text);
            gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
            gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.text);
            gcdmResult.addHeaderColumns("COLUMN_1", "Methane C1 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_2", "Ethane C2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_3", "Propane C3 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_4", "i-Butane C4I mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_5", "n-Butane C4N mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_6", "i-Pentane C5I mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_7", "n-Pentane C5N mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_8", "Hexane C6 ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_9", "Water C7 mg/Sm3", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_10", "Octane C8 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_11", "Nonane C9 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_12", "Total Sulphur C10 ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_13", "Carbon Dioxide CO2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_14", "Benzene H2 ppb", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_15", "Hydrogen Sulphide H2S ppm", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_16", "Neo-Pentane He NEO", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_17", "Nitrogen N2 mole %", typeColumn.number);
            gcdmResult.addHeaderColumns("COLUMN_18", "CycloHexane O2 ppb", typeColumn.number);

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
