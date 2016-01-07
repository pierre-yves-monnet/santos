package com.santos.sdeaccess;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.session.APISession;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.santos.toolbox.Toolbox;

public class SdeBusinessAccess {

    private static Logger logger = Logger.getLogger("org.bonitasoft.SdeAccess");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static SimpleDateFormat dashboardDateFormat = new SimpleDateFormat("dd-MM-yyyy");

    public static String DATASOURCE_NAME = "SDE_DS";

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Constantes to match different table of the Business Data */
    /*
     * By definition, all attributes are in UPPER CASE in all records, and in information
     * sent to the browser.
     */
    /*                                                                                  */
    /* ******************************************************************************** */

    public final static class TableDashBoard {

        public static String TABLE_NAME = "DASHBOARD";
        // public static String COMPLETE_DB_ID = "\""+TABLE_NAME + "\"." + "\"DB_ID\"";

        public static String DB_ID = "DB_ID";
        public static String COMPLETE_DB_ID = TABLE_NAME + "." + DB_ID;

        public static String BUSINESS_UNIT = "BUSINESS_UNIT";

        public static String SCHEDULED_ONLINE_DATE = "SCHEDULED_ONLINE_DATE";
        public static String WELL_CODE = "WELL_CODE";
        public static String SDE_NUMBER = "SDE_NUMBER";
        public static String SDE_STATUS = "SDE_STATUS";
        public static String SUBMITTED = "SUBMITTED";
        public static String INITIATED = "INITIATED";
        public static String WELL_TEMPLATE = "WELL_TEMPLATE";
        public static String WELL_CATEGORY_PRIMARY = "WELL_CATEGORY_PRIMARY";
        public static String WELL_FULL_NAME = "WELL_FULL_NAME";
        public static String REQUEST_TYPE = "REQUEST_TYPE";
        public static String BWD_STATUS = "BWD_STATUS";
        public static String WELL_DATA_STATUS = "WELL_DATA_STATUS";
        public static String ASSIGNED_RO = "ASSIGNED_RO";
        public static String DATE_WELL_IDENTIFIED = "DATE_WELL_IDENTIFIED";
        public static String ACTUAL_ONLINE_DATE = "ACTUAL_ONLINE_DATE";
        public static String ON_HOLD = "ON_HOLD";

        public static String getCompleteAttribut(final String attribut)
        {
            return TABLE_NAME + "." + attribut;
        }
    }

    public final static class TableWellInfo {

        public static String TABLE_NAME = "BASIC_WELL_INFO";

        public static String BWI_ID = "BWI_ID";
        public static String COMPLETE_BWI_ID = TABLE_NAME + "." + BWI_ID;
        public static String BWI_DB_ID = "BWI_DB_ID";

        public static String LINK_TO_FATHER = TableDashBoard.COMPLETE_DB_ID + "=" + COMPLETE_BWI_ID;

        public static String SDE_INTEGER = "SDE_INTEGER";
        public static String COMPLETE_SDE_NUMBER = TABLE_NAME + "." + SDE_INTEGER;
        // public static String COMPLETE_BWI_ID = "\""+ TABLE_NAME+"\"" + "." + "\"BWI_ID\"";
        public static String BUSINESS_UNIT = "BUSINESS_UNIT";

        public static String WELL_FULL_NAME = "WELL_FULL_NAME";

        public static String WELL_CODE = "WELL_CODE";
        public static String COMPLETE_WELL_CODE = TABLE_NAME + "." + WELL_CODE;

        public static String AREA_NUMBER = "AREA_NUMBER";

        public static String WELL_CATEGORY_PRIMARY = "WELL_CATEGORY_PRIMARY";
        public static String WELL_CATEGORY_SECONDARY_1 = "WELL_CATEGORY_SECONDARY_1";
        public static String WELL_CATEGORY_SECONDARY_2 = "WELL_CATEGORY_SECONDARY_2";
        public static String WELL_CATEGORY_SECONDARY_3 = "WELL_CATEGORY_SECONDARY_3";

        public static String FIELD_NAME = "FIELD_NAME";
        public static String PERMIT_SURFACE = "PERMIT_SURFACE";
        public static String PERMIT_BOTTOM_HOLE = "PERMIT_BOTTOM_HOLE";
        public static String PAD_NAME = "PAD_NAME";
        public static String JOINT_VENTURE_NAME = "JOINT_VENTURE_NAME";
        public static String SURFACE_LATITUDE = "SURFACE_LATITUDE";
        public static String SURFACE_LONGITUDE = "SURFACE_LONGITUDE";
        public static String BOTTOM_HOLE_LATITUDE = "BOTTOM_HOLE_LATITUDE";
        public static String BOTTOM_HOLE_LONGITUDE = "BOTTOM_HOLE_LONGITUDE";
        public static String COMPLETION_TYPE = "COMPLETION_TYPE";
        public static String WELL_ALIAS = "WELL_ALIAS";

    }

    public final static class TableAssumptions
    {

        public static String TABLE_NAME = "ASSUMPTIONS";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + "= AS_BWI_ID";
        public static String AS_BWI_ID = "AS_BWI_ID";
        public static String AS_ID = "AS_ID";
    }

    public final static class TableComposition
    {

        public static String TABLE_NAME = "COMPOSITION";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + " = COM_BWI_ID";
        public static String COM_BWI_ID = "COM_BWI_ID";
        public static String COM_ID = "COM_ID";
    }

    public final static class TableWellTestInfo
    {

        public static String TABLE_NAME = "WELL_TEST_INFO";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + " =  WTI_BWI_ID";
        public static String WTI_BWI_ID = "WTI_BWI_ID";
        public static String WTI_ID = "WTI_ID";
    }

    public final static class TableCapacityInfo {

        public static String TABLE_NAME = "CAPACITY_INFO";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + " = CI_BWI_ID";
        public static String CI_BWI_ID = "CI_BWI_ID";
        public static String CI_ID = "CI_ID";
    }

    public final static class TableProdAlloc {

        public static String TABLE_NAME = "PROD_ALLOC";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + " = PA_BWI_ID";
        public static String PA_BWI_ID = "PA_BWI_ID";
        public static String PA_ID = "PA_ID";
    }

    public final static class TableRMU {

        public static String TABLE_NAME = "RMU";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + " = RMU_BWI_ID";
        public static String RMU_BWI_ID = "RMU_BWI_ID";
        public static String RMU_ID = "RMU_ID";
    }

    public final static class TableProdAllocTag {

        public static String TABLE_NAME = "PROD_ALLOC_TAG";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + " = PAT_BWI_ID";
        public static String PAT_BWI_ID = "PAT_BWI_ID";
        public static String PAT_ID = "PAT_ID";
    }
    
    public final static class TableSalasGWS {

        public static String TABLE_NAME = "SALAS_GWS";
        public static String LINK_TO_FATHER = TableWellInfo.COMPLETE_BWI_ID + " = SG_BWI_ID";
        public static String SG_BWI_ID = "SG_BWI_ID";
        public static String SG_ID = "SG_ID";
    }

    public final static class TableRWellList {

        public static String TABLE_NAME = "R_WELL_LIST";
        public static String UWI = "UWI";
        public static String WELL_FULL_NAME = "WELL_FULL_NAME";
        public static String BUSINESS_UNIT = "BUSINESS_UNIT";
        public static String FIELD_NAME = "FIELD_NAME";

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* get synthesis information from a list of SdeNumber */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * key of the information is the number plus the status
     *
     * @author pierre-yves
     */
    public static class SdeNumberStatus {

        public Long sdeNumber;
        public Long sdeStatus;

        public String getKey()
        {
            return sdeStatus + "#" + sdeNumber;
        };

        @Override
        public String toString() {
            return getKey();
        }

        public static SdeNumberStatus getInstance(final long sdeNumber, final long sdeStatus)
        {
            final SdeNumberStatus sdeNumberStatus = new SdeNumberStatus();
            sdeNumberStatus.sdeNumber = sdeNumber;
            sdeNumberStatus.sdeStatus = sdeStatus;
            return sdeNumberStatus;
        }

    }

    public static class SdeResult {

        public String status;
        public String errorstatus;
        public Map<SdeNumberStatus, Map<String, Object>> listSdeInformation = new HashMap<SdeNumberStatus, Map<String, Object>>();
        public List<Map<String, Object>> listRecords = new ArrayList<Map<String, Object>>();

        public void add(final SdeNumberStatus sdeNumberStatus, final Map<String, Object> record)
        {
            listSdeInformation.put(sdeNumberStatus, record);
        }

        public static String getKeySdeNumberResult(final Long sdeNumber, final Long requestStatus)
        {
            return requestStatus + "#" + sdeNumber;
        }

    }

    public static class SdeParameter
    {

        public boolean allDashboardRecords = false;
        public boolean allowDirectConnection = false;
        public boolean formatDateJson = true;

        public boolean manageTableWellDashBoard = true;
        public boolean manageTableWellinfo = true;

        public boolean tableNameUpperCase = true;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        /**
         * if true, only record with SCHEDULED_ONLINE_DATE more than numberOfDaysInAdvance is shows
         * SqlRequest is DASHBOARD.SCHEDULED_ONLINE_DATE - CURRENT_DATE > numberOfDaysInAdvance
         */
        public boolean scheduledOnlineDateInFutur = false;
        public int NumberOfDaysInAdvance = 14;

        /**
         * if true, then the SDE Resquest with status 2 are ignored.
         * In fact, the table has 2 records:
         * sde_status=9 / sde_number=999 => Line to be display
         * sde_status=2 / sde_number=999 => well is close, and the line 9 should not be display
         * So, if this boolean is true, the second filter is added.
         */
        public boolean filterOnStatus2 = true;

        public boolean completeWithLists = true;

        public static SdeParameter getInstanceAllTable()
        {
            final SdeParameter sdeParameter = new SdeParameter();
            return sdeParameter;
        }
    };

    /**
     * get the list of information
     *
     * @param listSdeNumber
     * @param sdeParameter
     * @return
     */
    public SdeResult getSynthesisListSdeInformation(final List<SdeNumberStatus> listSdeNumber, final SdeParameter sdeParameter)
    {
        final SdeResult sdeResult = new SdeResult();
        Connection con = getConnection(sdeParameter.allowDirectConnection);
        if (con == null)
        {
            sdeResult.status = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            sdeResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            return sdeResult;
        }
        try
        {
            final DataModel dashBoard = new DataModel(TableDashBoard.TABLE_NAME, null, null, null, false, TableDashBoard.DB_ID);
            final DataModel wellInfo = new DataModel(TableWellInfo.TABLE_NAME, TableDashBoard.TABLE_NAME, TableDashBoard.DB_ID, TableWellInfo.BWI_DB_ID, false,
                    TableWellInfo.BWI_ID);

            final List<Object> listDataValue = new ArrayList<Object>();

            String sqlRequest = "select * from "
                    + dashBoard.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName) + ", "
                    + wellInfo.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName)
                    + " where  " + wellInfo.getLinkToFather();
            sqlRequest += " and " + TableDashBoard.getCompleteAttribut(TableDashBoard.SDE_STATUS) + " in (9) ";
            if (sdeParameter.scheduledOnlineDateInFutur)
            {
                // Only data with DASHBOARD.SCHEDULED_ONLINE_DATE - CURRENT_DATE >14 are display
                // issue is that in ORACLE current_date is sysdate, so to avoir that, give the date as parameters

                listDataValue.add(new Date());
                listDataValue.add(Integer.valueOf(sdeParameter.NumberOfDaysInAdvance));
                sqlRequest += " and " + TableDashBoard.getCompleteAttribut(TableDashBoard.SCHEDULED_ONLINE_DATE) + " - ? > ?";

            }
            if (sdeParameter.filterOnStatus2)
            {
                // DASHBOARD.DB_ID not in (select d9.DB_ID from dashboard d2, dashboard d9 where d9.sde_status=9 and d2.sde_status=2 and d2.sde_number=d9.sde_number )
                sqlRequest += " and " + TableDashBoard.COMPLETE_DB_ID
                        + " not in (select d9." + TableDashBoard.DB_ID
                        + " from " + TableDashBoard.TABLE_NAME + " d2, " + TableDashBoard.TABLE_NAME + " d9 "
                        + " where d9." + TableDashBoard.SDE_STATUS + "=9 "
                        + " and (d2." + TableDashBoard.SDE_STATUS + "=2 "
                        + " or d2." + TableDashBoard.SDE_STATUS + "=1) "
                        + " and d2." + TableDashBoard.SDE_NUMBER + " = d9." + TableDashBoard.SDE_NUMBER + ")";

            }
            if (!sdeParameter.allDashboardRecords)
            {
                sqlRequest += " and " + TableDashBoard.getCompleteAttribut(TableDashBoard.WELL_CODE) + " in (";

                int count = 0;
                for (final SdeNumberStatus sdeNumberStatus : listSdeNumber)
                {
                    if (count > 0) {
                        sqlRequest += ",";
                    }
                    sqlRequest += "'" + sdeNumberStatus.sdeNumber + "'";
                    count++;
                }
                sqlRequest += ")";
            }
            sqlRequest += " order by " + TableDashBoard.SDE_NUMBER + "," + TableDashBoard.SDE_STATUS;

            logger.info("Execute the request [" + sqlRequest + "] parameters onlyNewRecord[" + sdeParameter.scheduledOnlineDateInFutur + "]");
            final PreparedStatement preparedStatement = con.prepareStatement(sqlRequest);

            for (int i = 0; i < listDataValue.size(); i++) {
                if (listDataValue.get(i) instanceof Date) {
                    final java.sql.Date dateSql = new java.sql.Date(((Date) listDataValue.get(i)).getTime());
                    preparedStatement.setDate(i + 1, dateSql);
                } else {
                    preparedStatement.setObject(i + 1, listDataValue.get(i));
                }
            }

            final ResultSet rs = preparedStatement.executeQuery();
            final ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next())
            {
                final HashMap<String, Object> record = new HashMap<String, Object>();

                final int count = rsmd.getColumnCount();
                for (int i = 1; i <= count; i++)
                {
                    String key = rsmd.getColumnName(i);
                    key = key.toUpperCase();
                    if (rs.getObject(i) instanceof Date && sdeParameter.formatDateJson) {
                        final Date date = rs.getDate(i);
                        record.put(key, dashboardDateFormat.format(date));                        
                        continue;
                    }

                    record.put(key, rs.getObject(i));
                }
                logger.info("Read  [" + record + "]");
                final Long sdeNumber = Long.valueOf(Toolbox.getInteger(record.get(TableDashBoard.SDE_NUMBER), null));
                final Long sdeStatus = Long.valueOf(Toolbox.getInteger(record.get(TableDashBoard.SDE_STATUS), null));

                sdeResult.add(SdeNumberStatus.getInstance(sdeNumber, sdeStatus), record);
            }
            preparedStatement.close();
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the sql request call " + e.toString() + " at " + sw.toString());
            sdeResult.status = "FAILED";
            sdeResult.errorstatus = "Error during query the table";
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }
        return sdeResult;
    }

    /**
     * call to get the list
     *
     * @param listSdeNumber
     * @return
     */
    public SdeResult getSynthesisListSdeInformationSimulation(final List<SdeNumberStatus> listSdeNumberStatus)
    {
        // simulation at this moment
        int count = 00;
        final SdeResult sdeResult = new SdeResult();
        for (final SdeNumberStatus sdeNumberStatus : listSdeNumberStatus)
        {
            count++;
            final Map<String, Object> record = new HashMap<String, Object>();
            record.put(TableDashBoard.WELL_CODE, sdeNumberStatus.sdeNumber);
            record.put(TableDashBoard.SDE_STATUS, sdeNumberStatus.sdeStatus);
            record.put(TableDashBoard.SCHEDULED_ONLINE_DATE, null);
            record.put(TableDashBoard.WELL_CODE, " " + System.currentTimeMillis());
            record.put(TableDashBoard.WELL_FULL_NAME, "Well Full Name");
            record.put(TableDashBoard.REQUEST_TYPE, "New ");
            if (count % 3 == 0) {
                record.put(TableDashBoard.BWD_STATUS, "GREEN");
            } else if (count % 3 == 1) {
                record.put(TableDashBoard.BWD_STATUS, "RED");
            } else {
                record.put(TableDashBoard.BWD_STATUS, "ORANGE");
            }

            record.put(TableDashBoard.ASSIGNED_RO, "Walter.Bates");
            record.put(TableWellInfo.BUSINESS_UNIT, "EABU");

            sdeResult.add(sdeNumberStatus, record);
        }
        return sdeResult;

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* WellList */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class WellListParameter
    {

        public String filterUWI = null;
        public String filterWellFullName;
        public String filterBusinessUnit;
        public String filterFieldName;
        public boolean allowDirectConnection = false;

        public boolean resultAList = false;
        public int maxRecord = 100;
        public String orderByField = TableRWellList.UWI;
        public boolean formatDateJson = true;
        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static WellListParameter getFromJson(final String jsonSt)
        {
            final WellListParameter wellListParameter = new WellListParameter();
            if (jsonSt == null) {
                return wellListParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return wellListParameter;
            }

            wellListParameter.filterUWI = (String) jsonHash.get("filteruwi");
            wellListParameter.filterWellFullName = (String) jsonHash.get("filterwellfullname");
            wellListParameter.filterBusinessUnit = (String) jsonHash.get("filterbusinessunit");
            wellListParameter.filterFieldName = (String) jsonHash.get("filterfieldname");
            wellListParameter.resultAList = Toolbox.getBoolean(jsonHash.get("resultalist"), false);
            return wellListParameter;
        }

    };

    /**
     * get the list of information
     *
     * @param listSdeNumber
     * @param sdeParameter
     * @return
     */
    public SdeResult getWellList(final WellListParameter wellListParameter)
    {
        final SdeResult sdeResult = new SdeResult();
        Connection con = getConnection(wellListParameter.allowDirectConnection);
        if (con == null)
        {
            sdeResult.status = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            sdeResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";

            return sdeResult;
        }
        String sqlRequest = "";
        try
        {
            final DataModel wellList = new DataModel(TableRWellList.TABLE_NAME, null, null, null, false, null);

            sqlRequest = "select * from "
                    + wellList.getTableName(wellListParameter.tableNameUpperCase, wellListParameter.enquoteTableName)
                    + " where  1=1 ";
            if (wellListParameter.filterUWI != null && wellListParameter.filterUWI.trim().length() > 0)
            {
                sqlRequest += " and " + TableRWellList.UWI + " = '" + wellListParameter.filterUWI + "'";
            }
            if (wellListParameter.filterWellFullName != null && wellListParameter.filterWellFullName.trim().length() > 0)
            {
                sqlRequest += " and " + TableRWellList.WELL_FULL_NAME + " like '%" + wellListParameter.filterWellFullName + "%'";
            }
            if (wellListParameter.filterBusinessUnit != null && wellListParameter.filterBusinessUnit.trim().length() > 0)
            {
                sqlRequest += " and " + TableRWellList.BUSINESS_UNIT + " like '%" + wellListParameter.filterBusinessUnit + "%'";
            }
            if (wellListParameter.filterFieldName != null && wellListParameter.filterFieldName.trim().length() > 0)
            {
                sqlRequest += " and " + TableRWellList.FIELD_NAME + " like '%" + wellListParameter.filterFieldName + "%'";
            }
            sqlRequest += " order by " + wellListParameter.orderByField + " desc ";

            logger.info("Execute the request [" + sqlRequest + "] parameters");
            final Statement stmt = con.createStatement();
            int numberOfRecords = 0;
            final ResultSet rs = stmt.executeQuery(sqlRequest);
            final ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next() && numberOfRecords < wellListParameter.maxRecord)
            {
                numberOfRecords++;
                final HashMap<String, Object> record = new HashMap<String, Object>();

                final int count = rsmd.getColumnCount();
                for (int i = 1; i <= count; i++)
                {
                    String key = rsmd.getColumnName(i);
                    key = key.toUpperCase();
                    if (rs.getObject(i) instanceof Date && wellListParameter.formatDateJson) {
                        final Date date = rs.getDate(i);
                        record.put(key, date.getTime());
                        continue;
                    }

                    record.put(key, rs.getObject(i));
                }
                logger.info("Read  [" + record + "]");
                sdeResult.listRecords.add(record);

            }
            sdeResult.status = "OK";

            stmt.close();
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            sdeResult.status = "Fail";
            sdeResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            sdeResult.listRecords = null;
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }
        return sdeResult;
    }

    /**
     * create the Well data
     */
    public static class CreateWellParameter
    {

        public String uwi;
        public String wellFullName;
        public String businessUnit;
        public String fieldName;
        public boolean allowDirectConnection = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static CreateWellParameter getFromJson(final String jsonSt)
        {
            final CreateWellParameter createWellParameter = new CreateWellParameter();
            if (jsonSt == null) {
                return createWellParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return createWellParameter;
            }

            createWellParameter.uwi = (String) jsonHash.get("UWI");
            createWellParameter.wellFullName = (String) jsonHash.get("WELLFULLNAME");
            createWellParameter.businessUnit = (String) jsonHash.get("BUSINESSUNIT");
            createWellParameter.fieldName = (String) jsonHash.get("FIELDNAME");

            return createWellParameter;
        }
    }

    public SdeResult createWellList(final CreateWellParameter createWellParameter)
    {
        final SdeResult sdeResult = new SdeResult();
        Connection con = getConnection(createWellParameter.allowDirectConnection);
        if (con == null)
        {
            sdeResult.status = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            sdeResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            return sdeResult;
        }
        String sqlRequest;
        try
        {
            final DataModel dashBoard = new DataModel(TableDashBoard.TABLE_NAME, null, null, null, false, TableDashBoard.DB_ID);
            final DataModel wellInfo = new DataModel(TableWellInfo.TABLE_NAME, TableDashBoard.TABLE_NAME, TableDashBoard.DB_ID, TableWellInfo.BWI_DB_ID, false,
                    TableWellInfo.BWI_ID);
            for (int i = 0; i < 2; i++)
            {
                final Integer sdeStatus = Integer.valueOf(i == 0 ? 0 : 9);

                Map<String, Object> value = new HashMap<String, Object>();
                value.put(TableDashBoard.WELL_FULL_NAME, createWellParameter.wellFullName);
                value.put(TableDashBoard.BUSINESS_UNIT, createWellParameter.businessUnit);
                value.put(TableDashBoard.SDE_NUMBER, Toolbox.getInteger(createWellParameter.uwi, null));
                value.put(TableDashBoard.SDE_STATUS, sdeStatus);

                insertData(con, dashBoard.getTableName(createWellParameter.tableNameUpperCase, createWellParameter.enquoteTableName), value);

                // now search the TableDashBoard.DB_ID
                sqlRequest = "select " + TableDashBoard.DB_ID + " from "
                        + dashBoard.getTableName(createWellParameter.tableNameUpperCase, createWellParameter.enquoteTableName)
                        + " where " + TableDashBoard.WELL_FULL_NAME + "= '" + createWellParameter.wellFullName + "' "
                        + " and " + TableDashBoard.BUSINESS_UNIT + " = '" + createWellParameter.businessUnit + "' "
                        + " and " + TableDashBoard.SDE_NUMBER + " = '" + createWellParameter.uwi + "' "
                        + " and " + TableDashBoard.SDE_STATUS + "=" + sdeStatus;
                final PreparedStatement preparedStatement = con.prepareStatement(sqlRequest);
                final ResultSet rs = preparedStatement.executeQuery();
                Integer dbId = null;
                if (rs.next()) {
                    dbId = Toolbox.getInteger(rs.getObject(1), null);
                }
                if (dbId != null)
                {
                    // insert inh WellInfo
                    value = new HashMap<String, Object>();
                    value.put(TableWellInfo.WELL_FULL_NAME, createWellParameter.wellFullName);
                    value.put(TableWellInfo.BUSINESS_UNIT, createWellParameter.businessUnit);
                    value.put(TableWellInfo.FIELD_NAME, createWellParameter.fieldName);
                    value.put(TableWellInfo.BWI_DB_ID, dbId);

                    insertData(con, wellInfo.getTableName(createWellParameter.tableNameUpperCase, createWellParameter.enquoteTableName), value);

                }

            } // end if
            con.commit();
        } catch (final Exception e)
        {
            logger.severe("Error during create " + e.toString());
            sdeResult.status = "FAIL";
            sdeResult.errorstatus = "Error during create " + e.toString();
            try {
                con.rollback();
            } catch (final SQLException se) {
            };
        }
        if (con != null) {
            con = null;
        }
        return sdeResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* AssignRO */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class AssignROParameter
    {

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean allowDirectConnection = false;

        public List<Map<String, Object>> listUpdate;

        public static AssignROParameter getFromJson(final String jsonSt) {
            final AssignROParameter assignRO = new AssignROParameter();
            if (jsonSt == null) {
                return assignRO;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return assignRO;
            }

            assignRO.listUpdate = (List) jsonHash.get("list");
            return assignRO;
        }
    }

    public SdeResult updateAssignRo(final SdeBusinessAccess.AssignROParameter parameter)
    {
        final SdeResult sdeResult = new SdeResult();
        final DataModel dataModel = new DataModel(TableDashBoard.TABLE_NAME, null, null, null, false, null);

        final Connection con = getConnection(parameter.allowDirectConnection);
        if (con == null)
        {
            sdeResult.status = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            sdeResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            return sdeResult;
        }
        String sqlRequest = "";
        try
        {

            for (final Map<String, Object> oneRecord : parameter.listUpdate)
            {
                sqlRequest = "update " +
                        dataModel.getTableName(parameter.tableNameUpperCase, parameter.enquoteTableName)
                        + " set " + TableDashBoard.ASSIGNED_RO + " = ?"
                        + " where " + TableDashBoard.SDE_NUMBER + " = ? "
                        + " and " + TableDashBoard.SDE_STATUS + "= 9";
                logger.info("Update AssignRO [" + sqlRequest + "]");

                final PreparedStatement pstmt = con.prepareStatement(sqlRequest);
                pstmt.setObject(1, oneRecord.get("ASSIGNED_RO"));
                pstmt.setObject(2, oneRecord.get("SDENUMBER"));
                pstmt.executeUpdate();
                pstmt.close();
            }
            con.commit();

            sdeResult.status = "update " + parameter.listUpdate.size() + " records";
            sdeResult.errorstatus = "";

        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());

            sdeResult.status = "";
            sdeResult.errorstatus = "Error during create " + e.toString();

        }

        return sdeResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* PADashboard */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class PADashboardParameter {

        public String filteruwi = null;
        public String filterwellfullname = null;
        public String filtersdenumber = null;
        public String filterbusinessunit = null;
        public String filteroriginator = null;
        public String filterrequesttype = null;
        public String filteronlinedatefrom = null;
        public String filteronlinedateto = null;

        public boolean allowDirectConnection = false;

        public int maxRecord = 100;
        public String orderByField = TableDashBoard.SDE_NUMBER;
        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static PADashboardParameter getFromJson(final String jsonSt)
        {
            final PADashboardParameter paDashboardParameter = new PADashboardParameter();
            if (jsonSt == null) {
                return paDashboardParameter;
            }

            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return paDashboardParameter;
            }

            paDashboardParameter.filteruwi = (String) jsonHash.get("filteruwi");
            paDashboardParameter.filterwellfullname = (String) jsonHash.get("filterwellfullname");
            paDashboardParameter.filtersdenumber = (String) jsonHash.get("filtersdenumber");
            paDashboardParameter.filterbusinessunit = (String) jsonHash.get("filterbusinessunit");
            paDashboardParameter.filteroriginator = (String) jsonHash.get("filteroriginator");
            paDashboardParameter.filterrequesttype = (String) jsonHash.get("filterrequesttype");
            paDashboardParameter.filteronlinedatefrom = (String) jsonHash.get("filteronlinedatefrom");
            paDashboardParameter.filteronlinedateto = (String) jsonHash.get("filteronlinedateto");

            return paDashboardParameter;
        }

    };

    public static class PADashboardResult {

        public boolean allowDirectConnection = false;

        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static Map<String, Object> setPADashboard(final String jsonSt) {

            String SDE_NUMBER = null;
            String ON_HOLD = null;
            String DO_NOT_LOAD = null;
            String UPDATE_EC = null;

            final SdeResult sdeResult = new SdeResult();
            final HashMap<String, Object> result = new HashMap<String, Object>();

            sdeResult.listRecords = null;
            String sqlRequest = "";
            Connection con = null;

            try {

                sdeResult.status = "OK";
                if (jsonSt == null) {
                    sdeResult.errorstatus = "No input parameter has been received.";
                    sdeResult.status = "Fail";
                    return result;
                }

                final JSONArray jsonNArray = (JSONArray) JSONValue.parse(jsonSt);
                if (jsonNArray == null) {
                    sdeResult.errorstatus = "Could not parse JSON parameter: " + jsonSt;
                    sdeResult.status = "Fail";
                    return result;
                }

                con = new SdeBusinessAccess().getConnection(false);
                //                con = main.Main.sdeConnection();

                if (con == null) {
                    sdeResult.status = "Can't access the datasource [" + DATASOURCE_NAME + "]";
                    sdeResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";

                    return result;
                }

                for (int idx = 0; idx < jsonNArray.size(); idx++) {

                    final JSONObject o = (JSONObject) jsonNArray.get(idx);

                    SDE_NUMBER = o.get("SDE_NUMBER") != null ? o.get("SDE_NUMBER").toString() : null;

                    ON_HOLD = convertBooleanToBit((Boolean) o.get("ON_HOLD"));
                    DO_NOT_LOAD = convertBooleanToBit((Boolean) o.get("DO_NOT_LOAD"));
                    UPDATE_EC = convertBooleanToBit((Boolean) o.get("UPDATE_EC"));

                    String sqlON_HOLD = "";
                    if (ON_HOLD != null) {
                        if (ON_HOLD.equalsIgnoreCase("Y")) {
                            sqlON_HOLD = " ON_HOLD='" + ON_HOLD + "', EC_STATUS='R' , EC_DATE=SYSDATE, ";
                        } else {
                            sqlON_HOLD = " ON_HOLD='" + ON_HOLD + "', ";
                        }
                    } else {
                        sqlON_HOLD = " ON_HOLD=null,";
                    }
                    String sqlDO_NOT_LOAD = "";
                    if (DO_NOT_LOAD != null) {
                        if (DO_NOT_LOAD.equalsIgnoreCase("Y")) {
                            sqlDO_NOT_LOAD = " DO_NOT_LOAD='" + DO_NOT_LOAD + "', EC_STATUS='M' , EC_DATE=SYSDATE, ";
                        } else {
                            sqlDO_NOT_LOAD = " DO_NOT_LOAD='" + DO_NOT_LOAD + "', ";
                        }
                    } else {
                        sqlDO_NOT_LOAD = " DO_NOT_LOAD=null,";
                    }
                    String sqlUPDATE_EC = "";
                    if (UPDATE_EC != null) {
                        sqlUPDATE_EC = " UPDATE_EC='" + UPDATE_EC + "'";
                    } else {
                        sqlUPDATE_EC = " UPDATE_EC=null";
                    }

                    sqlRequest = "update SDE.DASHBOARD set "
                            + sqlON_HOLD + ""
                            + sqlDO_NOT_LOAD + ""
                            + sqlUPDATE_EC + ""
                            + " where SDE_STATUS=9 and " + TableDashBoard.SDE_NUMBER + " = '" + SDE_NUMBER + "'";

                    final Statement stmt = con.prepareStatement(jsonSt);
                    stmt.executeUpdate(sqlRequest);
                    stmt.close();
                }

                con.commit();                

            } catch (final Exception e) {
                final StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
                sdeResult.status = "Fail";
                sdeResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";

            } finally {
                if (con != null) {
                    con = null; // finish to use the connection
                }
            }
            result.put("STATUS", sdeResult.status);
            result.put("ERRORSTATUS", sdeResult.errorstatus);
            return result;
        }

    };

    private static String convertBooleanToBit(final Boolean input) {

        if (input == null) {
            return null;
        }
        if (input) {
            return "Y";
        }
        if (input == false) {
            return "N";
        }
        return null;
    }

    private Boolean convertBitToBoolean(final String input) {

        if (input == null) {
            return null;
        }
        if (input.equalsIgnoreCase("Y")) {
            return true;
        }
        if (input.equalsIgnoreCase("N")) {
            return false;
        }
        return null;

    }

    /** get the list of the PADashboard */
    public SdeResult getListPaDashboard(final PADashboardParameter paDashboardParameter, final APISession session, final ProcessAPI processAPI)
    {
        final SdeResult sdeResult = new SdeResult();
        Connection con = getConnection(paDashboardParameter.allowDirectConnection);
        if (con == null)
        {
            sdeResult.status = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            sdeResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";

            return sdeResult;
        }
        String sqlRequest = "";
        try
        {

            // SANTOS : list to get all informations

            final DataModel dataModel = new DataModel(TableDashBoard.TABLE_NAME, null, null, null, false, null);

            // old version
            sqlRequest = "select * from "
                    + dataModel.getTableName(paDashboardParameter.tableNameUpperCase, paDashboardParameter.enquoteTableName)
                    + " where  1=1 and (DO_NOT_LOAD ='N' or DO_NOT_LOAD is null) and (SDE_STATUS in (1) and SDE_NUMBER not in (select SDE_NUMBER from  DASHBOARD where SDE_STATUS = 2 or SDE_STATUS = 8)) ";

            // new version to pickup status = 9
            sqlRequest = "select * from DASHBOARD where  SDE_STATUS <> 0 and SDE_STATUS <> 1 and (DO_NOT_LOAD ='N' or DO_NOT_LOAD is null) and " +
                    "SDE_NUMBER in (" +
                    "select SDE_NUMBER from  DASHBOARD where (SDE_STATUS = 1) " +
                    "minus " +
                    "select SDE_NUMBER from  DASHBOARD where SDE_STATUS = 2 or SDE_STATUS = 8) ";

            if (paDashboardParameter.filtersdenumber != null && paDashboardParameter.filtersdenumber.trim().length() > 0) {
                sqlRequest += " and " + "SDE_NUMBER" + " = '" + paDashboardParameter.filtersdenumber + "'";
            }
            if (paDashboardParameter.filteruwi != null && paDashboardParameter.filteruwi.trim().length() > 0) {
                sqlRequest += " and " + "WELL_CODE" + " = '" + paDashboardParameter.filteruwi + "'";
            }
            if (paDashboardParameter.filterbusinessunit != null && paDashboardParameter.filterbusinessunit.trim().length() > 0) {
                sqlRequest += " and " + "BUSINESS_UNIT" + " = '" + paDashboardParameter.filterbusinessunit + "'";
            }
            if (paDashboardParameter.filterwellfullname != null && paDashboardParameter.filterwellfullname.trim().length() > 0) {
                sqlRequest += " and " + "WELL_FULL_NAME" + " = '" + paDashboardParameter.filterwellfullname + "'";
            }
            if (paDashboardParameter.filterrequesttype != null && paDashboardParameter.filterrequesttype.trim().length() > 0) {
                sqlRequest += " and " + "REQUEST_TYPE" + " = '" + paDashboardParameter.filterrequesttype + "'";
            }
            if (paDashboardParameter.filteroriginator != null && paDashboardParameter.filteroriginator.trim().length() > 0) {
                sqlRequest += " and " + "ASSIGNED_RO" + " = '" + paDashboardParameter.filteroriginator + "'";
            }
            if (paDashboardParameter.filteronlinedatefrom != null && paDashboardParameter.filteronlinedatefrom.trim().length() > 0) {
                sqlRequest += " and " + "(to_date(Scheduled_Online_Date,'DD-MM-YY') >= to_date('" + paDashboardParameter.filteronlinedatefrom
                        + "','DD-MM-YY'))";
            }
            if (paDashboardParameter.filteronlinedateto != null && paDashboardParameter.filteronlinedateto.trim().length() > 0) {
                sqlRequest += " and " + "(to_date(Scheduled_Online_Date,'DD-MM-YY') <= to_date('" + paDashboardParameter.filteronlinedateto + "','DD-MM-YY'))";
            }

            sqlRequest += " order by " + paDashboardParameter.orderByField + " desc ";

            logger.info("Execute the request [" + sqlRequest + "] parameters");

            final Statement stmt = con.createStatement();
            int numberOfRecords = 0;
            final ResultSet rs = stmt.executeQuery(sqlRequest);
            final ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next() && numberOfRecords < paDashboardParameter.maxRecord)
            {
                numberOfRecords++;
                final HashMap<String, Object> record = new HashMap<String, Object>();

                final int count = rsmd.getColumnCount();
                for (int i = 1; i <= count; i++)
                {
                    String key = rsmd.getColumnName(i);
                    key = key.toUpperCase();

                    if (rs.getObject(i) instanceof Date && paDashboardParameter.formatDateJson) {
                        final Date date = rs.getDate(i);                        
                        record.put(key, dashboardDateFormat.format(date));
                        continue;
                    }
                    if (key.equalsIgnoreCase("ON_HOLD")) {
                        System.out.println(key);
                        record.put(key, convertBitToBoolean(rs.getString(i)));
                        continue;
                    }
                    if (key.equalsIgnoreCase("DO_NOT_LOAD")) {
                        System.out.println(key);
                        record.put(key, convertBitToBoolean(rs.getString(i)));
                        continue;
                    }
                    if (key.equalsIgnoreCase("UPDATE_EC")) {
                        System.out.println(key);
                        record.put(key, convertBitToBoolean(rs.getString(i)));
                        continue;
                    }

                    record.put(key, rs.getObject(i));
                }

                SdePAProcessInfo.getHumanTasksForSDENumber(record, new SdeAccess.ListCasesParameter(), session, TenantAPIAccessor.getProcessAPI(session),
                        TenantAPIAccessor.getIdentityAPI(session));

                if ((boolean) record.get("KEEP_RECORD") == true) {
                    logger.info("SdeBusinessAccess.getListPaDashboard :: Read  [" + record + "]");
                    sdeResult.listRecords.add(record);
                }
                else {
                    logger.info("SdeBusinessAccess.getListPaDashboard :: Skipping record [" + record + "]");
                }

            }
            sdeResult.status = "OK";

            stmt.close();

            // sdeResult.listRecords.add(new HashMap<String, Object>());

            sdeResult.status = "OK";

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            sdeResult.status = "Fail";
            sdeResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            sdeResult.listRecords = null;
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }
        return sdeResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* SystemSummary */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class SystemSummaryParameter {

        public String filterUWI = null;
        public String filterSdeNumber = null;
        public String filterSdeStatus = null;
        public String filterWellTemplate = null;
        public String filterWellCategory = null;
        public String filterWellFullName = null;;
        public String filterBusinessUnit = null;;
        public String filterRequestType = null;;

        public Date filterOnlineDateFrom = null;;
        public Date filterOnlineDateTo = null;;

        public String processName = "SDEDemo";
        public String processVersion;
        public String paTaskName = "Modify and Validate Data PA";

        public boolean allowDirectConnection = false;

        public boolean resultAList = false;
        public int maxRecord = 100;
        public String orderByField = TableDashBoard.SCHEDULED_ONLINE_DATE;
        public boolean formatDateJson = true;
        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public static SystemSummaryParameter getFromJson(final String jsonSt)
        {
            final SystemSummaryParameter systemSummaryParameter = new SystemSummaryParameter();
            if (jsonSt == null) {
                return systemSummaryParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return systemSummaryParameter;
            }

            systemSummaryParameter.filterUWI = (String) jsonHash.get("filteruwi");
            systemSummaryParameter.filterWellFullName = (String) jsonHash.get("filterwellfullname");
            systemSummaryParameter.filterSdeStatus = (String) jsonHash.get("filtersdestatus");
            systemSummaryParameter.filterBusinessUnit = (String) jsonHash.get("filterbusinessunit");
            systemSummaryParameter.filterSdeNumber = (String) jsonHash.get("filtersdenumber");
            systemSummaryParameter.filterRequestType = (String) jsonHash.get("filterrequesttype");
            systemSummaryParameter.filterWellTemplate = (String) jsonHash.get("filterwelltemplate");
            systemSummaryParameter.filterWellCategory = (String) jsonHash.get("filterwellcategory");
            systemSummaryParameter.filterOnlineDateFrom = (Date) jsonHash.get("filteronlinedatefrom");
            systemSummaryParameter.filterOnlineDateTo = (Date) jsonHash.get("filteronlinedateto");;

            systemSummaryParameter.resultAList = Toolbox.getBoolean(jsonHash.get("resultalist"), false);

            return systemSummaryParameter;
        }

    }

    /**
     * get the list of information
     *
     * @param listSdeNumber
     * @param sdeParameter
     * @return
     */
    public SdeResult getListSummary(final SystemSummaryParameter systemSummaryParameter)
    {
        final SdeResult sdeResult = new SdeResult();
        Connection con = getConnection(systemSummaryParameter.allowDirectConnection);
        if (con == null)
        {
            sdeResult.status = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            sdeResult.errorstatus = "Can't access the datasource [" + DATASOURCE_NAME + "]";

            return sdeResult;
        }
        String sqlRequest = "";
        try
        {
            final DataModel dashBoard = new DataModel(TableDashBoard.TABLE_NAME, null, null, null, false, TableDashBoard.DB_ID);
            final DataModel wellInfo = new DataModel(TableWellInfo.TABLE_NAME, TableDashBoard.TABLE_NAME, TableDashBoard.DB_ID, TableWellInfo.BWI_DB_ID, false,
                    TableWellInfo.BWI_ID);

            final List<Object> listDataValue = new ArrayList<Object>();

            final String dashBoardTable = dashBoard.getTableName(systemSummaryParameter.tableNameUpperCase, systemSummaryParameter.enquoteTableName);
            sqlRequest = "select "
                    + " (select 'Y' from " + dashBoardTable + " d1 where d1.sde_number=" + dashBoardTable + ".sde_number and d1.sde_status=2) as sde_status2,"
                    + " (select 'Y' from " + dashBoardTable + " d1 where d1.sde_number=" + dashBoardTable + ".sde_number and d1.sde_status=8) as sde_status8,"
                    + " * "
                    + " from "
                    + dashBoardTable + ", "
                    + wellInfo.getTableName(systemSummaryParameter.tableNameUpperCase, systemSummaryParameter.enquoteTableName)
                    + " where  " + wellInfo.getLinkToFather();

            final List<Object> listRequestObject = new ArrayList<Object>();
            sqlRequest += addFilter(systemSummaryParameter.filterUWI, TableDashBoard.WELL_CODE);
            sqlRequest += addFilter(systemSummaryParameter.filterSdeNumber, TableDashBoard.SDE_NUMBER);
            sqlRequest += addFilter(systemSummaryParameter.filterSdeStatus, TableDashBoard.SDE_STATUS);
            sqlRequest += addFilter(systemSummaryParameter.filterWellTemplate, TableDashBoard.WELL_TEMPLATE);
            sqlRequest += addFilter(systemSummaryParameter.filterWellCategory, TableDashBoard.WELL_CATEGORY_PRIMARY);

            sqlRequest += addFilter(systemSummaryParameter.filterWellFullName, TableDashBoard.WELL_FULL_NAME);
            sqlRequest += addFilter(systemSummaryParameter.filterBusinessUnit, TableDashBoard.BUSINESS_UNIT);
            sqlRequest += addFilter(systemSummaryParameter.filterRequestType, TableDashBoard.REQUEST_TYPE);

            if (systemSummaryParameter.filterOnlineDateFrom != null)
            {
                sqlRequest += " and " + TableDashBoard.SCHEDULED_ONLINE_DATE + " > ? ";
                listRequestObject.add(systemSummaryParameter.filterOnlineDateFrom);
            }
            if (systemSummaryParameter.filterOnlineDateTo != null)
            {
                sqlRequest += " and " + TableDashBoard.SCHEDULED_ONLINE_DATE + " < ? ";
                listRequestObject.add(systemSummaryParameter.filterOnlineDateTo);
            }

            sqlRequest += " order by " + systemSummaryParameter.orderByField + " desc ";

            logger.info("Execute the request [" + sqlRequest + "] parameters");
            final PreparedStatement pstmt = con.prepareStatement(sqlRequest);
            for (int i = 0; i < listRequestObject.size(); i++)
            {
                final Object o = listRequestObject.get(i);
                pstmt.setObject(i + 1, o);
            }

            int numberOfRecords = 0;
            final ResultSet rs = pstmt.executeQuery();
            final ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next() && numberOfRecords < systemSummaryParameter.maxRecord)
            {
                numberOfRecords++;
                final HashMap<String, Object> record = new HashMap<String, Object>();

                final int count = rsmd.getColumnCount();
                for (int i = 1; i <= count; i++)
                {
                    String key = rsmd.getColumnName(i);
                    key = key.toUpperCase();
                    if (rs.getObject(i) instanceof Date && systemSummaryParameter.formatDateJson) {
                        final Date date = rs.getDate(i);
                        record.put(key, date.getTime());
                        continue;
                    }

                    record.put(key, rs.getObject(i));
                }
                logger.info("Read  [" + record + "]");
                sdeResult.listRecords.add(record);

            }
            sdeResult.status = "OK";

            pstmt.close();
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            sdeResult.status = "Fail";
            sdeResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            sdeResult.listRecords = null;
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }
        return sdeResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* private method */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * @param con
     * @param tableName
     * @param value
     * @throws SQLException
     */

    private void insertData(final Connection con, final String tableName, final Map<String, Object> value) throws SQLException
    {
        String sqlRequest = "insert into " + tableName;
        String listFields = "";
        String listValues = "";
        final List<Object> listValuesObject = new ArrayList<Object>();
        for (final String key : value.keySet())
        {
            listFields += key + ",";
            listValues += "?,";
            listValuesObject.add(value.get(key));
        }

        listFields = listFields.substring(0, listFields.length() - 1);
        listValues = listValues.substring(0, listValues.length() - 1);

        sqlRequest += "(" + listFields + ") values (" + listValues + ")";
        final PreparedStatement preparedStatement = con.prepareStatement(sqlRequest);
        for (int i = 0; i < listValuesObject.size(); i++) {
            preparedStatement.setObject(i + 1, listValuesObject.get(i));
        }

        // execute insert SQL statement
        preparedStatement.executeUpdate();

    }

    public enum SdeDataStatus {
        OK, NOTFOUND, ERROR, NODATABASECONNECTION, SQLERROR
    };

    /* ******************************************************************************** */
    /*                                                                                  */
    /* SdeData */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class SdeData {

        public SdeDataStatus status;
        public String statusdetails = "";
        public Map<String, Object> data = new HashMap<String, Object>();
        public Map<String, Object> listsValue = new HashMap<String, Object>();

        public Map<String, Object> pointerToData;

        public String getJsonFormat()
        {
            final HashMap<String, Object> jsonValue = new HashMap<String, Object>();

            jsonValue.put("sdeData", transformDates(data));
            jsonValue.put("sdeLists", transformDates(listsValue));

            // now, transform all DATES because the json can't do that
            // transformDates(jsonValue);
            final String jsonValueSt = JSONValue.toJSONString(jsonValue);
            // logger.info("SdeData to Json=" + jsonValueSt);
            return jsonValueSt;
        }

        // we have to translate all Date to a Sdf format...
        private Object transformDates(final Object jsonObject)
        {
            if (jsonObject instanceof List)
            {
                final ArrayList<Object> newList = new ArrayList<Object>();
                for (final Object oneItem : (List<Object>) jsonObject) {
                    newList.add(transformDates(oneItem));
                }
                return newList;
            }
            if (jsonObject instanceof Map)
            {
                final HashMap<String, Object> newMap = new HashMap<String, Object>();
                for (final String key : ((Map<String, Object>) jsonObject).keySet()) {
                    final Object value = ((Map) jsonObject).get(key);
                    newMap.put(key, transformDates(value));
                }
                return newMap;
            }
            if (jsonObject instanceof Date) {
                return ((Date) jsonObject).getTime();
            }
            return jsonObject;

        }

        public static SdeData getInstanceFormJson(final String jsonSt)
        {
            // we may have the level"sdeData" or directly "dashboard" : accept both
            final HashMap<String, Object> jsonHash = (HashMap<String, Object>) JSONValue.parse(jsonSt);
            return getInstanceFromMap(jsonHash);
        }

        public static SdeData getInstanceFromMap(final Map<String, Object> jsonHash)
        {
            final SdeData sdeData = new SdeData();
            if (jsonHash.containsKey("dashboard"))
            {
                sdeData.data = jsonHash;
            } else if (jsonHash.containsKey("sdeData"))
            {
                sdeData.data = (Map) jsonHash.get("sdeData");
            }
            else {
                sdeData.data = new HashMap<String, Object>();
                sdeData.data.put("dashboard", jsonHash);
            }

            if (jsonHash.containsKey("sdeLists"))
            {
                sdeData.listsValue = (Map) jsonHash.get("sdeLists");
            }
            return sdeData;

        }

        /**
         * to allow the load recursively based on the DataModel, the currentData in progress to load should be kept
         * This is the goal of theses methode
         *
         * @param currentData
         */
        public void setPointerData(final Map<String, Object> currentData)
        {
            pointerToData = currentData;
        };

        public Map<String, Object> getPointerData()
        {
            return pointerToData == null ? data : pointerToData;
        };

        // different attributes on the object
        public String getAttributBusinessUnit() {
            if (data == null) {
                return null;
            }
            final Map<String, Object> dashBoard = (Map<String, Object>) data.get("dashboard");
            if (dashBoard == null) {
                return null;
            }

            return (String) dashBoard.get(TableDashBoard.BUSINESS_UNIT);

        }

        public String getAttributRequestType() {
            if (data == null) {
                return null;
            }
            final Map<String, Object> dashBoard = (Map<String, Object>) data.get("dashboard");
            if (dashBoard == null) {
                return null;
            }

            return (String) dashBoard.get(TableDashBoard.REQUEST_TYPE);

        }
    }

    public static class DataModel
    {

        private final String tableName;

        private final String fatherTableName;
        private final String fatherColLink;
        private final String localColLink;

        private final boolean isMultiple;
        private final String colKey;
        private final List<DataModel> childs = new ArrayList<DataModel>();

        public DataModel(final String tableName, final String fatherTableName, final String fatherColLink, final String localColLink, final boolean isMultiple,
                final String colKey)
        {
            this.tableName = tableName;
            this.fatherTableName = fatherTableName;
            this.fatherColLink = fatherColLink;
            this.localColLink = localColLink;

            this.isMultiple = isMultiple;
            this.colKey = colKey;

        }

        public List<DataModel> getChilds() {
            return childs;
        }

        public void addChild(final DataModel child)
        {
            childs.add(child);
        }

        public String getSdeDataName()
        {
            return tableName.toLowerCase();
        }

        public String getTableName(final boolean tableNameUpperCase, final boolean enquoteTableName)
        {
            String calculateTableName = tableNameUpperCase ? tableName.toUpperCase() : tableName.toLowerCase();
            if (enquoteTableName) {
                calculateTableName = "\"" + calculateTableName + "\"";
            }
            return calculateTableName;
        }

        public String getLinkToFather() {
            return fatherTableName + "." + fatherColLink + " = " + tableName + "." + localColLink;
        }

        public String getFatherTableLink() {
            return fatherTableName;
        }

        public String getFatherColLink() {
            return fatherColLink;
        }

        public String getLocalColLink() {
            return localColLink;
        }

        public boolean isMultiple() {
            return isMultiple;
        }

        public String getColKey(final boolean colNameUpperCase) {
            return colNameUpperCase ? colKey.toUpperCase() : colKey.toLowerCase();
        }
    }

    /**
     * get the SDE dataModel
     *
     * @return
     */
    public DataModel getDataModel()
    {
        final DataModel dashBoard = new DataModel(TableDashBoard.TABLE_NAME, null, null, null, false, TableDashBoard.DB_ID);
        final DataModel wellInfo = new DataModel(TableWellInfo.TABLE_NAME, TableDashBoard.TABLE_NAME, TableDashBoard.DB_ID, TableWellInfo.BWI_DB_ID, false,
                TableWellInfo.BWI_ID);
        dashBoard.addChild(wellInfo);

        final DataModel assumption = new DataModel(TableAssumptions.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID, TableAssumptions.AS_BWI_ID,
                false, TableAssumptions.AS_ID);
        wellInfo.addChild(assumption);

        final DataModel composition = new DataModel(TableComposition.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID, TableComposition.COM_BWI_ID,
                false, TableComposition.COM_ID);
        wellInfo.addChild(composition);

        final DataModel wellTestInfo = new DataModel(TableWellTestInfo.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID,
                TableWellTestInfo.WTI_BWI_ID, false, TableWellTestInfo.WTI_ID);
        wellInfo.addChild(wellTestInfo);

        final DataModel capacityInfo = new DataModel(TableCapacityInfo.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID, TableCapacityInfo.CI_BWI_ID,
                false, TableCapacityInfo.CI_ID);
        wellInfo.addChild(capacityInfo);

        final DataModel prodAlloc = new DataModel(TableProdAlloc.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID, TableProdAlloc.PA_BWI_ID, false,
                TableProdAlloc.PA_ID);
        wellInfo.addChild(prodAlloc);

        final DataModel rmu = new DataModel(TableRMU.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID, TableRMU.RMU_BWI_ID, true, TableRMU.RMU_ID);
        wellInfo.addChild(rmu);

        final DataModel prodAllocTag = new DataModel(TableProdAllocTag.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID,
                TableProdAllocTag.PAT_BWI_ID, true, TableProdAllocTag.PAT_ID);
        wellInfo.addChild(prodAllocTag);

        final DataModel salasGWS = new DataModel(TableSalasGWS.TABLE_NAME, TableWellInfo.TABLE_NAME, TableWellInfo.BWI_ID,
                TableSalasGWS.SG_BWI_ID, true, TableSalasGWS.SG_BWI_ID);
        wellInfo.addChild(salasGWS);        
        
        return dashBoard;

    }

    /**
     * @param sdeNumber
     * @param sdeStatus
     * @param sdeParameter
     * @return
     */
    public SdeData readSdeData(final Long sdeNumber, final Long sdeStatus, final SdeParameter sdeParameter)
    {
        logger.info("~~~~~~~~~~~~~~~~ SdeBusinessAccess.readSdeData SdeNumber[" + sdeNumber + "] SdeStatus[" + sdeStatus + "]");
        final SdeData sdeData = new SdeData();
        Connection con = getConnection(sdeParameter.allowDirectConnection);
        if (con == null)
        {
            sdeData.status = SdeDataStatus.NODATABASECONNECTION;
            sdeData.statusdetails = "Can't access the datasource [" + DATASOURCE_NAME + "]";

            // complete with the defaut list for debug usage
            if (sdeParameter.completeWithLists) {
                try
                {
                    loadLists(sdeData, null);
                } catch (final Exception e)
                {
                };
            }

            logger.severe("~~~~~~~~~~~~~~~~ SdeBusinessAccess.readSdeData : END No access to datasource--------------------------");

            return sdeData;
        }
        Statement stmt = null;
        try
        {

            // set the initial pointer to the father
            sdeData.setPointerData(sdeData.data);
            stmt = con.createStatement();
            final String filterSql = TableDashBoard.getCompleteAttribut(TableDashBoard.SDE_NUMBER) + " = " + sdeNumber
                    + " and " + TableDashBoard.SDE_STATUS + " = " + sdeStatus;
            query(sdeData, getDataModel(),
                    null,
                    filterSql, sdeParameter, stmt);

            if (sdeParameter.completeWithLists)
            {
                loadLists(sdeData, stmt);
            }

            stmt.close();
            if (sdeData.data.get(getDataModel().getSdeDataName()) == null) {
                sdeData.status = SdeDataStatus.NOTFOUND;
                sdeData.statusdetails = "No data found with sqlRequest[" + filterSql + "]";

            }
            else {

                // place in the result a special variable forthecontract to let the UI Designer contract
                final Map<String, Object> mainData = (Map<String, Object>) sdeData.data.get(getDataModel().getSdeDataName());
                mainData.put("forthecontract", "A");
                sdeData.status = SdeDataStatus.OK;
            }

        } catch (final SQLException e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("~~~~~~~~~~~~~~~~ SdeBusinessAccess.readSdeData SdeNumber[" + sdeNumber + "] SdeStatus[" + sdeStatus + "] Error Sqlrequest call "
                    + e.toString() + " at " + sw.toString());
            sdeData.status = SdeDataStatus.SQLERROR;
            sdeData.statusdetails = "Error " + e.toString() + " at " + sw.toString();

        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("~~~~~~~~~~~~~~~~ SdeBusinessAccess.readSdeData SdeNumber[" + sdeNumber + "] SdeStatus[" + sdeStatus + "] Error during Read "
                    + e.toString() + " at " + sw.toString());
            sdeData.status = SdeDataStatus.ERROR;
            sdeData.statusdetails = "Error " + e.toString() + " at " + sw.toString();
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }

        logger.info("~~~~~~~~~~~~~~~~ SdeBusinessAccess.readSdeData SdeNumber[" + sdeNumber + "] SdeStatus[" + sdeStatus + "] END  status:[" + sdeData.status
                + "] details[" + sdeData.statusdetails + "]");
        return sdeData;
    }

    /**
     * @param sdeData
     * @param sdeParameter
     * @return the same sdeData, but with status and statusDetails fullfill
     */
    public SdeData writeSdeData(final SdeData sdeData, final SdeParameter sdeParameter)
    {
        final Connection con = getConnection(sdeParameter.allowDirectConnection);
        if (con == null)
        {
            sdeData.status = SdeDataStatus.NODATABASECONNECTION;
            sdeData.statusdetails = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            logger.severe("~~~~~~~~~~~~~~~~ SdeBusinessAccess.writeSdeData : No Datasource [" + DATASOURCE_NAME + "]");
        }
        Object sdeNumber = null;
        Object sdeStatus = null;

        try
        {

            final HashMap<String, Object> dashboard = (HashMap<String, Object>) sdeData.data.get("dashboard");

            sdeNumber = dashboard.get(TableDashBoard.SDE_NUMBER);
            sdeStatus = dashboard.get(TableDashBoard.SDE_STATUS);
            logger.info("~~~~~~~~~~~~~~~~ SdeBusinessAccess.writeSdeData SdeNumber[" + sdeNumber + "] SdeStatus[" + sdeStatus + "]");

            sdeData.setPointerData(dashboard);

            String whereCondition;
            if (dashboard.get(TableDashBoard.DB_ID) != null)
            {
                // ok, we have the ID, so it's clearly a modification. Delete is based on this ID
                whereCondition = TableDashBoard.COMPLETE_DB_ID + " = " + dashboard.get(TableDashBoard.DB_ID);
            } else {
                whereCondition = TableDashBoard.getCompleteAttribut(TableDashBoard.SDE_NUMBER) + " = " + dashboard.get(TableDashBoard.SDE_NUMBER)
                        + " and " + TableDashBoard.getCompleteAttribut(TableDashBoard.SDE_STATUS) + " = " + dashboard.get(TableDashBoard.SDE_STATUS);
            }

            delete(sdeData, getDataModel(), null, whereCondition, con, sdeParameter);

            // set again
            sdeData.setPointerData(dashboard);
            insert(sdeData, getDataModel(), con, sdeParameter);
            con.commit();
            logger.info("~~~~~~~~~~~~~~~~ SdeBusinessAccess.writeSdeData : OK SdeNumber[" + sdeNumber + "] SdeStatus[" + sdeStatus + "]");
            sdeData.status = SdeDataStatus.OK;
            return sdeData;
        } catch (final Exception e)
        {
            // logger.severe("Error " + e.toString());
            try {
                con.rollback();
            } catch (final Exception e2) {
            };
            sdeData.status = e instanceof SQLException ? SdeDataStatus.SQLERROR : SdeDataStatus.ERROR;
            sdeData.statusdetails = "Write failed [" + e.toString() + "]";

            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();

            logger.severe("~~~~~~~~~~~~~~~~ SdeBusinessAccess.writeSdeData SdeNumber[" + sdeNumber + "] SdeStatus[" + sdeStatus + "]" + e.toString() + " at "
                    + exceptionDetails);
            return sdeData;
        }
    }

    /**
     * @param sdeNumber
     * @param sdeStatus
     * @param submit
     * @param sdeParameter
     * @return
     */
    public SdeData updateSubmitStatus(final Long sdeNumber, final Long sdeStatus, final String submitValue, final SdeParameter sdeParameter)
    {
        return updateAttribut(sdeNumber, sdeStatus, TableDashBoard.SUBMITTED, submitValue, sdeParameter);
    }

    public SdeData updateInitiatedStatus(final Long sdeNumber, final Long sdeStatus, final String initatedValue, final SdeParameter sdeParameter)
    {
        return updateAttribut(sdeNumber, sdeStatus, TableDashBoard.INITIATED, initatedValue, sdeParameter);
    }

    /**
     * @param sdeNumber
     * @param sdeStatus
     * @param submit
     * @param sdeParameter
     * @return
     */
    public SdeData updateAttribut(final Long sdeNumber, final Long sdeStatus, final String colName, final String colValue, final SdeParameter sdeParameter)
    {
        // logger.info("SdeBusinessAccess : updateSdeSantos --------------------------");
        final SdeData sdeData = new SdeData();
        final Connection con = getConnection(sdeParameter.allowDirectConnection);
        if (con == null)
        {
            sdeData.status = SdeDataStatus.NODATABASECONNECTION;
            sdeData.statusdetails = "Can't access the datasource [" + DATASOURCE_NAME + "]";
            logger.severe("SdeBusinessAccess : writeSdeData END No access to datasource--------------------------");
            return sdeData;
        }
        try
        {
            final DataModel dashBoard = new DataModel(TableDashBoard.TABLE_NAME, null, null, null, false, TableDashBoard.DB_ID);

            final String sqlRequest = "update " + dashBoard.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName)
                    + " set " + colName + "='" + colValue + "'"
                    + " where " + TableDashBoard.SDE_NUMBER + " = '" + sdeNumber + "' and " + TableDashBoard.SDE_STATUS + "='" + sdeStatus + "'";
            final Statement stmt = con.createStatement();
            logger.info("updateAttribut.updateSubmitStatus:SqlRequest [" + sqlRequest + "]");

            final int numberOfRow = stmt.executeUpdate(sqlRequest);
            con.commit();
            sdeData.status = numberOfRow == 1 ? SdeDataStatus.OK : SdeDataStatus.NOTFOUND;

            // TODO
            // Replicate trigger logic here
            // Clone final Long sdeNumber and set sde_status to

            return sdeData;

        } catch (final Exception e)
        {
            // logger.severe("Error " + e.toString());
            try {
                con.rollback();
            } catch (final Exception e2) {
            };
            sdeData.status = e instanceof SQLException ? SdeDataStatus.SQLERROR : SdeDataStatus.ERROR;
            sdeData.statusdetails = "Write failed [" + e.toString() + "]";

            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();

            logger.severe("SdeBusinessAccess.updateAttribut End FAIL --------------------------" + e.toString() + " at " + exceptionDetails);
            return sdeData;
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* private operation On Database */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private static class ListDefinition
    {

        public String name;
        public String colKey;
        public String colValue;
        public String table;
        public String whereClause;

        public ListDefinition(final String name, final String colKey, final String colValue, final String table, final String whereClause)
        {
            this.name = name;
            this.colKey = colKey;
            this.colValue = colValue;
            this.table = table;
            this.whereClause = whereClause;
        }

    }


    /**
     * load all lists
     *
     * @param sdeData
     * @param stmt
     * @throws SQLException
     */
    private void loadLists(final SdeData sdeData, final Statement stmt) throws SQLException
    {
        logger.info("   / loadList ----------------");
        String logDetails = "";
        final List<ListDefinition> lists = new ArrayList<ListDefinition>();

        // final String name, final String colKey, final String colValue, final String table
        lists.add(new ListDefinition("r_area_numbers_by_field", "area_number", "area_name", "r_area_numbers_by_field", null));
        lists.add(new ListDefinition("r_genset_make_models", "energy_input_gj_hr", "engine_make_and_model", "r_genset_make_models", null));
        lists.add(new ListDefinition("ov_well_hole", "distinct op_fcty_1_code", "op_fcty_1_code", "ov_well_hole", null));
        lists.add(new ListDefinition("r_prod_alloc_tag_glng", "distinct ec_template_code", "ec_template_code", "r_prod_alloc_tag", "well_template = 'GLNG'"));
        lists.add(new ListDefinition("r_well_group", "distinct value", "value", "r_form_data", "type = 'well_group'"));
        lists.add(new ListDefinition("r_well_category", "value", "value", "r_form_data", "type='well_cat'"));
        lists.add(new ListDefinition("r_gas_inlet", "value", "value", "r_form_data", "type='gas_inlet'"));
        lists.add(new ListDefinition("r_wtr_dis", "value", "value", "r_form_data", "type='wtr_dis'"));
        lists.add(new ListDefinition("r_well_lcyc", "value", "value", "r_form_data", "type='well_lcyc'"));
        lists.add(new ListDefinition("r_well_opst", "value", "value", "r_form_data", "type='well_opst'"));
        // --------- artificial_lift_glng
        lists.add(new ListDefinition("artificial_lift_glng", "value", "value", "r_form_data", "type='artsys_code' and business_unit='GLNG'"));
        // operator area
        lists.add(new ListDefinition("operator_area_glng", "value", "value", "r_form_data", "type='operator_area' and business_unit='GLNG'"));
        // r_pool
        //  xxxxxxxxx
        lists.add(new ListDefinition("r_pool_layer_short", "RMU", "LAYER_SHORT", "r_pool", null));
        lists.add(new ListDefinition("r_pool_formation", "RMU", "FORMATION", "r_pool", null));
        // Select distinct(OP_FCTY_1_CODE) from SDE.OV_WELL_HOLE order by OP_FCTY_1_CODE;

        // template list
        final String businessUnit = sdeData.getAttributBusinessUnit();

        List<Map<String, Object>> listStaticValues = new ArrayList<Map<String, Object>>();
        sdeData.listsValue.put("template_list", listStaticValues);
        if ("GLNG".equals(businessUnit))
        {
            addInList(listStaticValues, "GLNG", "GLNG");
            // for test only
            addInList(listStaticValues, "Cooper Gas", "Cooper Gas");
            addInList(listStaticValues, "Cooper Oil", "Cooper Oil");
            addInList(listStaticValues, "Unit Oil", "Unit Oil");

        } else if ("EABU".equals(businessUnit))
        {
            addInList(listStaticValues, "Cooper Gas", "Cooper Gas");
            addInList(listStaticValues, "Cooper Oil", "Cooper Oil");
            addInList(listStaticValues, "Unit Oil", "Unit Oil");
        }
        else
        {
            // for test only
            addInList(listStaticValues, "GLNG", "GLNG");
            addInList(listStaticValues, "Cooper Gas", "Cooper Gas");
            addInList(listStaticValues, "Cooper Oil", "Cooper Oil");
            addInList(listStaticValues, "Unit Oil", "Unit Oil");

        }

        // WellHookList
        if ("GLNG".equals(businessUnit))
        {
            listStaticValues = new ArrayList<Map<String, Object>>();
            sdeData.listsValue.put("well_hook_up", listStaticValues);
        }
        else {
            lists.add(new ListDefinition("well_hook_up", "value", "value", "r_form_data", "type='whup_code' and business_unit='EABU'"));
        }

        // --------- well type list
        final String requestType = sdeData.getAttributRequestType();
        if ("GLNG".equals(businessUnit) && "NEW".equals(requestType))
        {
            logDetails += "Well_type: businessUnit=GLNG & requestType=NEW : select type='wtype' and business_unit='GLNG';";
            lists.add(new ListDefinition("well_type", "value", "value", "r_form_data", "type='wtype' and business_unit='GLNG'"));

        }
        else if ("GLNG".equals(businessUnit) && "UPDATE".equals(requestType))
        {
            logDetails += "Well_type: businessUnit=GLNG & requestType=UPDATE : select type='wtype' and business_unit like 'GLNG%';";
            lists.add(new ListDefinition("well_type", "value", "value", "r_form_data", "type='wtype' and business_unit like 'GLNG%'"));

        }
        else if ("EABU".equals(businessUnit))
        {
            logDetails += "Well_type: businessUnit=EABU : select value from SDE.r_form_data where type='ec_wtype' and business_unit='EABU'";
            lists.add(new ListDefinition("well_type", "value", "value", "r_form_data", "type='ec_wtype' and business_unit='EABU'"));

        }
        else
        {
            logDetails += "Well_type: businessUnit[" + businessUnit + "] requestType[" + requestType + "] : emptyList (expect GLNG/NEW or GLNG/UPDATE);";
            listStaticValues = new ArrayList<Map<String, Object>>();
            sdeData.listsValue.put("well_type", listStaticValues);
        }

        // --------- ec_well_type
        lists.add(new ListDefinition("ec_well_type", "value", "value", "r_form_data", "type='ec_wtype' and business_unit='BOTH'"));

        // --------- artificial_lift
        lists.add(new ListDefinition("artificial_lift", "value", "value", "r_form_data", "type='artsys_code' and business_unit='EABU'"));

        // --------- rmu_interval_name         
        // Changing reference table from 'r_form_data' to 'r_pool'
        //lists.add(new ListDefinition("rmu_interval_name", "value", "value", "r_form_data", "type='rmu' and business_unit='BOTH'"));
        lists.add(new ListDefinition("rmu_interval_name", "RMU", "RMU", "r_pool", null));

        // String suffix
        listStaticValues = new ArrayList<Map<String, Object>>();
        sdeData.listsValue.put("string_suffix", listStaticValues);
        addInList(listStaticValues, "A", "A");
        addInList(listStaticValues, "X", "X");

        // production calculatation
        listStaticValues = new ArrayList<Map<String, Object>>();
        sdeData.listsValue.put("production_calculation", listStaticValues);
        if ("GLNG".equals(businessUnit))
        {
            addInList(listStaticValues, "Telemetry", "Telemetry");

        }
        else if ("EABU".equals(businessUnit))
        {
            addInList(listStaticValues, "Telemetry", "Telemetry");
            addInList(listStaticValues, "Well Test", "Well Test");

        }

        // ltap well

        listStaticValues = new ArrayList<Map<String, Object>>();
        sdeData.listsValue.put("ltap_well", listStaticValues);
        addInList(listStaticValues, "Yes", "Yes");
        addInList(listStaticValues, "No", "No");

        // basic_well_info.business_unit

        for (final ListDefinition definition : lists)
        {
            final String sqlRequest = "select " + definition.colKey + " as valuekey," + definition.colValue + " as valuelabel"
                    + " from " + definition.table
                    + (definition.whereClause != null ? " where " + definition.whereClause : "")
                    + " order by "
                    + definition.colValue;

            final List<Map<String, Object>> listValues = new ArrayList<Map<String, Object>>();
            try
            {
                if (stmt != null)
                {
                    final ResultSet rs = stmt.executeQuery(sqlRequest);
                    while (rs.next())
                    {
                        final Map<String, Object> oneValue = new HashMap<String, Object>();
                        final Object key = rs.getObject(1);
                        final Object value = rs.getObject(2);
                        if (key != null && value != null)
                        {
                                oneValue.put("key", key); // keep the same type for the key
                                oneValue.put("value", value.toString());                            
                                listValues.add(oneValue);
                        }
                        
                    }
                }
                logger.info(" list[" + definition.name + "] nbResult(" + listValues.size() + ") by sqlRequest [" + sqlRequest + "]");

                sdeData.listsValue.put(definition.name, listValues);

            } catch (final SQLException e)
            {
                logger.severe("Error during calculating list [" + definition.name + "] by SqlRequest[" + sqlRequest + "] " + e.toString());
                logDetails += "Error: list[" + definition.name + "]: by SqlRequest[" + sqlRequest + "] SqlState:" + e.getSQLState() + ";";
            }
        }

        // list value Composition

        final Map<String, Map<String, Object>> listFWSComposition = new HashMap<String, Map<String, Object>>();
        final String sqlRequestComposition = "select r.rmu_name as RmuName, UPPER(c.reservoir), c.CO2 * 100 as CO2, c.N2 * 100 as N2, "
                + "c.C1 * 100 as C1,"
                + "c.C2 * 100 as C2,"
                + "c.C3 * 100 as C3,"
                + "c.ic4 * 100 as IC4,"
                + "c.nC4 * 100 as NC4,"
                + "c.NC5 * 100 as NC5,"
                + "c.IC5 * 100 as IC5,"
                + "c.C6 * 100 as C6,"
                + "c.C7PLUS * 100 as C7PLUS,"

                + "c.C8PLUS * 100 as C8PLUS"

                + " from rmu_composition c, r_reservoir_management_unit r  "
                + " where c.year_end in (select max(year_end) from rmu_composition) and c.product='Raw Gas' and c.scenario='1P' and UPPER(r.rmu_name)=UPPER(c.reservoir)";
        final ResultSet rs = stmt.executeQuery(sqlRequestComposition);
        final ResultSetMetaData rsmd = rs.getMetaData();
        String message = "";

        while (rs.next())
        {
            final Map<String, Object> record = new HashMap<String, Object>();

            final int count = rsmd.getColumnCount();
            for (int i = 1; i <= count; i++)
            {
                String key = rsmd.getColumnName(i);
                key = key.toUpperCase();
                record.put(key, rs.getObject(i));
                if (rs.getObject(i) == null)
                {
                    message += "Rmu[" + (String) record.get("RMUNAME") + "] value[" + key + "] is null;";
                }
            }

            listFWSComposition.put((String) record.get("RMUNAME"), record);

        }

        sdeData.listsValue.put("FWSComposition", listFWSComposition);
        sdeData.listsValue.put("FWSCompositionMessage", message);

        sdeData.listsValue.put("status", logDetails);

        logger.info("   / loadList END ---------------- businessUnit[" + businessUnit + "]");
        for (final String key : sdeData.listsValue.keySet()) {
            logger.info("   List [" + key + "]= " + sdeData.listsValue.get(key).toString());
        }

    }

    //  query(sdeData, getDataModel(), TableDashBoard.COMPLETE_WELL_CODE + " = '" + sdeNumber + "'", true, sdeParameter, con);
    /**
     * recursve load, following the dataModel
     * Load the current DataMoldel in the sdeData.getCurrentPointer() by the condition given.
     * If the datamodel is consider as List, then we save in the sdeData a List of HashMap, else directly the value. The name of attribute is the name of the
     * tablenamle
     * Then, for all children of the datamodel we call:
     * - the pointer to the current Hashmap. If we load a LIST of value, then we call the sub datamodel for each item of the list
     * ex : OrderH => List of OrderL => OrderDelivery. We call OrderDelivery for each OrderL
     * - the condition is build by the childDataLModel.linkToFather AND dataModel.key = PointerHashmap.get( dataModel.key )
     * ex : in OrderL, key is "IdL", then the condition is OrderDelivery.fatherL= OrderL.keyL AND OrderL.IdL=34
     *
     * @param sdeData
     * @param dataModel
     * @param queryCondition
     * @param sdeParameter
     * @param con
     * @throws SQLException
     */
    private void query(final SdeData sdeData,
            final DataModel dataModel,
            final String additionalQueryTables,
            final String queryCondition,
            final SdeParameter sdeParameter,
            final Statement stmt) throws SQLException
    {
        // load the current level
        final String sqlRequest = "select " + dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName) + ".* from "
                + dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName)
                + (additionalQueryTables == null ? "" : ", " + additionalQueryTables)
                + " where " + queryCondition
                + " order by " + dataModel.colKey;

        logger.info("Execute the request [" + sqlRequest + "]");

        final ResultSet rs = stmt.executeQuery(sqlRequest);
        final ResultSetMetaData rsmd = rs.getMetaData();

        final List<Map<String, Object>> listResult = new ArrayList<Map<String, Object>>();
        while (rs.next())
        {
            final Map<String, Object> record = new HashMap<String, Object>();

            final int count = rsmd.getColumnCount();
            for (int i = 1; i <= count; i++)
            {
                String key = rsmd.getColumnName(i);
                key = key.toUpperCase();

                if (rs.getObject(i) instanceof Date && sdeParameter.formatDateJson) {
                    final Date date = rs.getDate(i);
                    record.put(key, date.getTime());
                    continue;
                }
                record.put(key, rs.getObject(i));
            }
            listResult.add(record);
            logger.info("DATAMODEL " + dataModel.getSdeDataName() + "Read  [" + record + "]");
        }

        if (dataModel.isMultiple()) {
            sdeData.getPointerData().put(dataModel.getSdeDataName(), listResult);
        } else
        {

            // keep only the first one
            final Map<String, Object> currentMap = listResult.size() == 0 ? null : listResult.get(0);
            if (currentMap != null)
            {
                listResult.clear();
                listResult.add(currentMap);
            }
            sdeData.getPointerData().put(dataModel.getSdeDataName(), currentMap);
        }

        // load recursively child
        for (final DataModel child : dataModel.childs)
        {
            for (final Map<String, Object> oneDataMap : listResult)
            {
                sdeData.setPointerData(oneDataMap);
                // all information in the dataMap are in UpperCase
                final String condition = child.getLinkToFather()
                        + " and " + dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName) + "."
                        + dataModel.getColKey(sdeParameter.colNameUpperCase) + "=" + oneDataMap.get(dataModel.colKey.toUpperCase());
                query(sdeData, child, dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName), condition, sdeParameter, stmt);
            }
        }
    }

    /**
     * @param sdeData
     * @param dataModel
     * @param collectionTable
     * @param whereCondition
     * @param stmt
     * @param sdeParameter
     * @throws SQLException
     */
    private void delete(final SdeData sdeData, final DataModel dataModel, String collectionTable, String whereCondition, final Connection con,
            final SdeParameter sdeParameter) throws SQLException
    {

        // complete the collectionTable with this level
        final String localTableName = dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName);
        final String localColidName = localTableName + "." + dataModel.localColLink;
        collectionTable = (collectionTable == null ? "" : collectionTable + ", ") + localTableName;

        if (dataModel.getFatherTableLink() != null) {
            whereCondition += " and " + dataModel.getFatherTableLink() + "." + dataModel.getFatherColLink() + "=" + localColidName;
        }

        // first delete the sub level
        for (final DataModel dataModelChild : dataModel.getChilds())
        {
            delete(sdeData, dataModelChild, collectionTable, whereCondition, con, sdeParameter);
        }

        // now we can delete this level
        // request is
        // delete from basic_well_info where basic_well_info.BWI_DB_ID in (select basic_well_info.BWI_DB_ID from basic_well_info, DASHBOARD where DASHBOARD.sde_number = '3344' and DASHBOARD.request_status = '1' and DASHBOARD.DB_ID=basic_well_info.BWI_DB_ID);

        String sqlRequest = null;
        if (dataModel.getFatherTableLink() == null) {
            sqlRequest = "delete from " + localTableName + " where " + whereCondition;
        } else {
            sqlRequest = "delete from " + localTableName + " where " + localColidName + " in (select " + localColidName + " from " + collectionTable
                    + " where " + whereCondition + ")";
        }
        logger.info("  DELETE: " + localTableName + " Father " + dataModel.getFatherTableLink() + " sqlRequest [" + sqlRequest + "]");;
        if (con != null)
        {
            final Statement stmt = con.createStatement();
            stmt.execute(sqlRequest);
            stmt.close();
        }

    }

    /*
     * ResultSet tables;
     * tables = databaseMetaData.getTables(dataConnection.getConnection().getCatalog(), null, "%", null);
     */
    /**
     * recursive loop and update
     *
     * @param sdeData
     * @param dataModel
     * @param con
     * @param sdeParameter
     * @throws SQLException
     */
    private void insert(final SdeData sdeData, final DataModel dataModel, final Connection con, final SdeParameter sdeParameter) throws SQLException
    {
        logger.info(" -- manage level [" + dataModel.getSdeDataName() + "] Table["
                + dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName) + "]");
        // update the current level
        String sqlRequest = "insert into " + dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName);
        final DatabaseMetaData databaseMetaData = con.getMetaData();

        final ResultSet columns = databaseMetaData.getColumns(con.getCatalog(), null, dataModel.getTableName(sdeParameter.tableNameUpperCase, false), "%");
        final Set<String> listColumns = new HashSet<String>();
        final Set<String> listColumnsDate = new HashSet<String>();
        final Set<String> listColumnsLong = new HashSet<String>();
        final StringBuffer traceParameters = new StringBuffer();
        while (columns.next())
        {
            listColumns.add(columns.getString("COLUMN_NAME").toUpperCase());
            final int dataType = columns.getInt("DATA_TYPE");
            traceParameters.append("COL[" + columns.getString("COLUMN_NAME").toUpperCase() + "]datatype[" + dataType + "], ");

            if (dataType == java.sql.Types.DATE) {
                listColumnsDate.add(columns.getString("COLUMN_NAME").toUpperCase());
            }
            if (dataType == java.sql.Types.TIMESTAMP) {
                listColumnsDate.add(columns.getString("COLUMN_NAME").toUpperCase());
            }

            if (dataType == java.sql.Types.LONGVARBINARY || dataType == java.sql.Types.LONGNVARCHAR || dataType == java.sql.Types.INTEGER
                    || dataType == java.sql.Types.BIGINT) {
                listColumnsLong.add(columns.getString("COLUMN_NAME").toUpperCase());
            }

        }
        final HashSet<String> listChildsName = new HashSet<String>();
        for (final DataModel dataModelChild : dataModel.getChilds())
        {
            listChildsName.add(dataModelChild.getSdeDataName());
        }
        final Map<String, Object> dataThisLevel = sdeData.getPointerData();
        logger.info(dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName) + ": Columns:[" + listColumns + "] listColumnsLong["
                + listColumnsLong + "] listColumnsDate[" + listColumnsDate + "] dataThisLevel[" + dataThisLevel + "]");

        if (dataThisLevel == null || dataThisLevel.size() == 0)
        {
            // no data at this level ! Go back
            logger.info(dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName) + ": No data at this level");
            return;
        }

        final List<Object> listDataValue = new ArrayList<Object>();
        String listFieldKey = "";
        String listFieldValue = "";

        
        // RMU 
        // This field is custom widget in UI.
        // This field is created on the fly by the user, hence has no sequence generated for its primary key
        // The following if statement is a special handling for RMU data
        if(dataModel.getTableName(sdeParameter.tableNameUpperCase, sdeParameter.enquoteTableName).equalsIgnoreCase("RMU")){
            
            logger.info("SdeBusinessAccess.insert :: Found RMU.");
            
            // get sequence
            // TODO :: May need to externalise to a method if needed for other tables.
            Statement sequenceStatement = con.createStatement();
            ResultSet sequenceResultSet = sequenceStatement.executeQuery("select sde.rmu_seq.nextval from dual");            
            sequenceResultSet.next();
            int sequence = sequenceResultSet.getInt(1);            
            sequenceResultSet.close();
            sequenceStatement.close();

            logger.info("SdeBusinessAccess.insert :: Generated sequence for RMU: " + sequence);
            
            dataThisLevel.put("RMU_ID", sequence);            
            dataThisLevel.put("MODIFIED_BY", "DASH_ADMN");
            dataThisLevel.put("MODIFIED_DATE", new Date());
            // get Basic Well Information key, needed as foreign key
            Map<String, Object> dashboardMap = (Map<String, Object>) sdeData.data.get("dashboard");
            if(dashboardMap == null){                
                logger.severe("SdeBusinessAccess.insert :: Could not obtain 'dashboard' map.");
                return;
            }
            
            Map<String, Object> basic_well_infoMap = (Map<String, Object>) dashboardMap.get("basic_well_info");
            if(basic_well_infoMap == null){
                logger.severe("SdeBusinessAccess.insert :: Could not obtain 'basic_well_info' map.");
                return;
            }
            
            dataThisLevel.put("RMU_BWI_ID", basic_well_infoMap.get("BWI_ID"));
            
            // populate PERF_INTERVAL_CODE & PERF_INTERVAL_NAME
            /*
            // PERF_INTERVAL_CODE :
            // To construct the perf_interval_code: 
            // basic_well_info.well_bore_interval || / || r_pool.layer_short 
            String rmu = (String)dataThisLevel.get("RMU");
            String wellBoreInterval  = (String)basic_well_infoMap.get("WELL_BORE_INTERVAL");
            logger.info("SdeBusinessAccess.insert :: Obtained data [rmu="+rmu+"] [wellBoreInterval=" +wellBoreInterval+"]");            
            System.out.println("xxxxxxxxxxxxxx" + sdeData.listsValue.keySet());
            logger.info("xxxxxxxxxxxxxx" + sdeData.listsValue.keySet());
            Map<String, Object> r_pool_layer_short = (Map<String, Object>) sdeData.listsValue.get("r_pool_layer_short");            
            logger.info("r_pool_layer_short.size()"+ r_pool_layer_short.size() );
            logger.info("r_pool_layer_short.keySet()"+r_pool_layer_short.keySet());
            String layerShort = (String)r_pool_layer_short.get(rmu);
            // PERF_INTERVAL_NAME :
            // To construct the perf_interval_name as follows 
            // initCap(basic_well_info.well_full_name) || - || r_pool.formation             
            Map<String, Object> r_pool_formation = (Map<String, Object>) sdeData.listsValue.get("r_pool_formation");
            */
            
        }
            
        
        
        for (final String key : dataThisLevel.keySet())
        {
            if (listChildsName.contains(key)) {
                continue;
            }
            // keep only UPPERCASE name
            if (!listColumns.contains(key.toUpperCase())) {
                continue;
            }
            traceParameters.append("sav[" + key + "]");
            listFieldKey += " " + key + ",";
            listFieldValue += "?,";

            Object valueToSave = dataThisLevel.get(key);
            // maybe a long or a date ?
            if (listColumnsDate.contains(key) && valueToSave != null)
            {
                traceParameters.append("~date~");
                try
                {
                    final Long valueLong = Long.valueOf(valueToSave.toString());
                    valueToSave = new Date(valueLong);
                } catch (final Exception e)
                {
                }
                if (!(valueToSave instanceof Date))
                {
                    try
                    {
                        final Date date = sdf.parse(valueToSave.toString());
                        valueToSave = date;
                    } catch (final Exception e)
                    {
                    }

                };
            }
            if (listColumnsLong.contains(key) && valueToSave != null)
            {
                traceParameters.append("~long~");
                try
                {
                    valueToSave = Long.valueOf(valueToSave.toString());
                } catch (final Exception e)
                {
                    logger.info("Error transform long[" + valueToSave + "] key[" + key + "] datatable[" + dataModel.getSdeDataName() + "] Error["
                            + e.toString() + "]");
                }

            }
            traceParameters.append("[" + valueToSave + "] " + (valueToSave != null ? valueToSave.getClass().getName() : "null") + ", ");

            listDataValue.add(valueToSave);
        }
        if (listFieldKey.length() == 0)
        {
            // no field to save here ??

            logger.severe("INSERT : NO COLUMNS on model [" + dataModel.getSdeDataName() + "] TableName["
                    + dataModel.getTableName(sdeParameter.tableNameUpperCase, false) + "] tableNameUpperCase ? [" + sdeParameter.tableNameUpperCase
                    + "] parameters[" + traceParameters + "]");
            // don't try to save childs : if there are no data at this level.
            return;

        }
        listFieldKey = listFieldKey.substring(0, listFieldKey.length() - 1); //
        listFieldValue = listFieldValue.substring(0, listFieldValue.length() - 1);

        sqlRequest += "(" + listFieldKey + ") values (" + listFieldValue + ")";
        
        logger.info(" data[" + dataModel.getSdeDataName() + "] data[" + sdeData.getPointerData() + "] InsertRequest [" + sqlRequest + "] ListData "
                + listDataValue.toString() + "]");

        final PreparedStatement preparedStatement = con.prepareStatement(sqlRequest);

        for (int i = 0; i < listDataValue.size(); i++) {
            if (listDataValue.get(i) instanceof Date) {
                final java.sql.Date dateSql = new java.sql.Date(((Date) listDataValue.get(i)).getTime());
                preparedStatement.setDate(i + 1, dateSql);
            } else {
                preparedStatement.setObject(i + 1, listDataValue.get(i));
            }
        }

        // execute insert SQL stetement
        try
        {
            preparedStatement.executeUpdate();
        } catch (final SQLException e)
        {

            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();

            logger.severe("During write [" + dataModel.getSdeDataName() + "] by request [" + sqlRequest + "] : listData[" + traceParameters + "]: exception "
                    + e.toString() + " : " + exceptionDetails);
            throw e;
        }

        // so, now play the child.
        for (final DataModel dataModelChild : dataModel.getChilds())
        {
            // first, clean all data according this record

            // then, loop for each record
            if (dataModelChild.isMultiple())
            {
                final List<Map<String, Object>> listChildData = (List) dataThisLevel.get(dataModelChild.getSdeDataName());
                for (final Map<String, Object> childData : listChildData)
                {

                    sdeData.setPointerData(childData);
                    insert(sdeData, dataModelChild, con, sdeParameter);
                }
            }
            else
            {
                final Map<String, Object> childData = (Map) dataThisLevel.get(dataModelChild.getSdeDataName());
                sdeData.setPointerData(childData);
                insert(sdeData, dataModelChild, con, sdeParameter);
            }
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* basic operation */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /*
    *
    */

    private Connection getDirectConnection()
    {
        try {
            Class.forName("org.postgresql.Driver");

            Connection connection = null;
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/santosSDE", "bonita", "bonita");
            return connection;
        } catch (final ClassNotFoundException e) {
            logger.severe("error " + e.toString());

        } catch (final SQLException e) {
            logger.severe("error " + e.toString());
        }
        return null;
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

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Simulation */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public SdeData getADummyData()
    {
        final SdeData sdeData = new SdeData();
        final DataModel dataModel = getDataModel();

        generateId = 1;
        final HashMap<String, Object> dashBoard = new HashMap<String, Object>();
        sdeData.data.put("dashboard", dashBoard);
        sdeData.setPointerData(dashBoard);
        populateDummy(sdeData, 0, dataModel);
        return sdeData;
    }

    private void getDefaultValueForDummy(final String sdeName, final int index, final Map<String, Object> map)
    {
        map.put("modified_by", "Walter");
        map.put("modified_date", new Date());

        if (TableDashBoard.TABLE_NAME.equalsIgnoreCase(sdeName))
        {
            map.put(TableDashBoard.SDE_NUMBER, 3344L);
            map.put(TableDashBoard.SDE_STATUS, 1L);
            map.put("actual_online_date", new Date());
            map.put(TableDashBoard.WELL_CODE, "WellCode");
            map.put(TableDashBoard.WELL_FULL_NAME, "Well full name");
            map.put("business_unit", "BusUnit");
            map.put("request_type", "R");
            map.put("scheduled_online_date", new Date());
            map.put("bwd_status", "BwdStatus");
            map.put("sde_status", "SdeStatus");
            map.put("submitted", "S");
            map.put("date_well_identified", new Date());
        }
        if (TableWellInfo.TABLE_NAME.equalsIgnoreCase(sdeName))
        {
            map.put("WELL_CODE", "WellCode 55");
            map.put("WELL_FULL_NAME", "WellFulName 2");
            map.put("BUSINESS_UNIT", "BusinessUnit");
            map.put("AREA_NUMBER", "112");

            map.put(TableWellInfo.AREA_NUMBER, 532L);

        }
        if (TableComposition.TABLE_NAME.equalsIgnoreCase(sdeName))
        {
            map.put("com_data_source", "DataSource");
        }

        if (TableProdAllocTag.TABLE_NAME.equalsIgnoreCase(sdeName))
        {
            map.put("well_template", "TPL1");
            map.put("ec_template_code", "CODE");
            map.put("ec_attribute", "ATTR" + index);

        }
        if (TableRMU.TABLE_NAME.equalsIgnoreCase(sdeName))
        {
            map.put("rmu", "This is the RMU number " + index);
        }

    }

    /**
     * create an acceptable information
     *
     * @param sdeData
     * @param dataModel
     */
    private void populateDummy(final SdeData sdeData, final int index, final DataModel dataModel)
    {
        logger.info("Populate [" + dataModel.getSdeDataName() + "] databaseTableName[" + dataModel.getTableName(false, false) + "]");
        final Map<String, Object> mapAtThisLevel = sdeData.getPointerData();
        // maybe the ID is already set by the father ?
        Long oneLocalId = (Long) mapAtThisLevel.get(dataModel.getColKey(true));
        if (oneLocalId == null)
        {
            oneLocalId = getId(false);
            mapAtThisLevel.put(dataModel.getColKey(true), oneLocalId);
        }
        getDefaultValueForDummy(dataModel.getSdeDataName(), index, mapAtThisLevel);

        for (final DataModel childModel : dataModel.getChilds())
        {
            // reset the pointer here

            if (childModel.isMultiple())
            {
                final List<Map<String, Object>> listChildMap = new ArrayList<Map<String, Object>>();
                mapAtThisLevel.put(childModel.getSdeDataName(), listChildMap);
                // add 2 childs
                for (int i = 0; i < 3; i++)
                {
                    final Map<String, Object> childMap = new HashMap<String, Object>();
                    listChildMap.add(childMap);
                    sdeData.setPointerData(childMap);

                    // the childMap expect a special information to link to the father

                    Long idToLink = (Long) mapAtThisLevel.get(childModel.getFatherColLink());
                    if (idToLink == null)
                    {
                        idToLink = getId(true);
                        mapAtThisLevel.put(childModel.getFatherColLink(), idToLink);
                    }

                    mapAtThisLevel.put(childModel.getFatherColLink(), idToLink);
                    childMap.put(childModel.getLocalColLink(), idToLink);
                    logger.info("       " + dataModel.getSdeDataName() + " Key to [" + dataModel.getTableName(true, false) + "."
                            + childModel.getFatherColLink()
                            + "]  = child.["
                            + childModel.getTableName(true, false) + "." + childModel.getLocalColLink() + "] set to [" + idToLink + "]");

                    populateDummy(sdeData, i, childModel);

                }
            }
            else
            {
                final Map<String, Object> childMap = new HashMap<String, Object>();
                mapAtThisLevel.put(childModel.getSdeDataName(), childMap);
                sdeData.setPointerData(childMap);

                // the childMap expect a special information to link to the father
                // nota : maybe the father has already a value set to the ID
                Long idToLink = (Long) mapAtThisLevel.get(childModel.getFatherColLink());
                if (idToLink == null)
                {
                    idToLink = getId(true);
                    mapAtThisLevel.put(childModel.getFatherColLink(), idToLink);
                }
                childMap.put(childModel.getLocalColLink(), idToLink);
                logger.info("       " + dataModel.getSdeDataName() + " Key to [" + dataModel.getTableName(true, false) + "." + childModel.getFatherColLink()
                        + "]  = child.["
                        + childModel.getTableName(true, false) + "." + childModel.getLocalColLink() + "] set to [" + idToLink + "]");

                populateDummy(sdeData, 0, childModel);

            }
        }
    }

    long generateId = 0;

    private long getId(final boolean foreignKey)
    {
        generateId++;
        if (foreignKey) {
            return generateId + 1000;
        }
        return generateId;
    }

    /**
     * add a value in the list
     *
     * @param listValues
     * @param key
     * @param value
     */
    private void addInList(final List<Map<String, Object>> listValues, final String key, final String value)
    {
        final Map<String, Object> oneValue = new HashMap<String, Object>();
        oneValue.put("key", key);
        oneValue.put("value", value);
        listValues.add(oneValue);
    }

    /**
     * addFilter
     *
     * @param filter
     * @param attribut
     * @return
     */
    private String addFilter(final String filter, final String attribut)
    {
        if (filter == null || filter.trim().length() == 0) {
            return "";
        }
        return " and " + attribut + " like '%" + filter + "%'";
    }
}
