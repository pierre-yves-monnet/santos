package com.santos.gcdmaccess;

import java.io.PrintWriter;
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

import org.json.simple.JSONValue;

public class GcdmBusinessAccess {

    public static String DATASOURCE_NAME = "GCDM_DS";

    private static Logger logger = Logger.getLogger("org.bonitasoft.SdeAccess");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static SimpleDateFormat sdfHuman = new SimpleDateFormat("yyyy-MM-dd");

    public static class GcdmResult {

        public String status;
        public String errorstatus;
        public List<Map<String, Object>> listRecords = new ArrayList<Map<String, Object>>();
        public Map<String, List<Map<String, Object>>> listsSelect = new HashMap<String, List<Map<String, Object>>>();




    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* PADashboard */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class GasCompositionParameter {

        public String filterUWI = null;

        public boolean allowDirectConnection = false;

        public int maxRecord = 100;
        public String orderByField = "POINTNAME";
        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

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

            gasCompositionParameter.filterUWI = (String) jsonHash.get("filteruwi");
            return gasCompositionParameter;
        }

    };

    /** get the list of the PADashboard */
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

            // SANTOS : list to get all informations
            sqlRequest = "select POINTNAME as SupplyChainePoint,effectiveDate as EffectiveDateTime, * from composition";

            final List<Object> listRequestObject = new ArrayList<Object>();

            sqlRequest += " order by " + gasComposition.orderByField + " desc ";

            gcdmResult.listRecords = executeRequest(con, sqlRequest, listRequestObject, gasComposition.maxRecord, gasComposition.formatDateJson);

            // list Select
            final List<Map<String, Object>> listRecords = new ArrayList<Map<String, Object>>();
            final HashMap<String, Object> option = new HashMap<String, Object>();
            option.put("id", "1");
            option.put("name", "First value");
            listRecords.add(option);
            gcdmResult.listsSelect.put("LISTSUPPLYCHAIN", listRecords);

            gcdmResult.status = gcdmResult.listRecords.size() + " items found";



        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("Error during the sql request [" + sqlRequest + "] call " + e.toString() + " at " + sw.toString());
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during query the table by[" + sqlRequest + "] : " + e.toString() + "]";
            gcdmResult.listRecords = null;
        }
        if (con != null)
        {
            con = null; // finish to use the connection
        }
        return gcdmResult;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* private */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
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
