package com.santos.gcdmaccess;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.session.APISession;
import org.json.simple.JSONValue;

import com.santos.gcdmaccess.GcdmToolbox.GcdmResult.typeColumn;
import com.santos.toolbox.Toolbox;

public class GcdmReport {

    static Logger logger = Logger.getLogger("org.bonitasoft.GcdmReport");


    /* ******************************************************************************** */
    /*                                                                                  */
    /* GCDM Report */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static class ReportParameter {

        public String typeReport;
        public String supplyChain;
        public Date dateFrom;
        public Date dateTo;
        public boolean auditTrail;

        public boolean allowDirectConnection = false;

        public int maxRecord = 100;
        public String orderByField = "";
        public boolean formatDateJson = true;

        public boolean tableNameUpperCase = false;
        public boolean enquoteTableName = false;
        public boolean colNameUpperCase = false;

        public List<String> listToDelete = null;

        public static ReportParameter getFromJson(final String jsonSt)
        {
            final ReportParameter reportParameter = new ReportParameter();
            if (jsonSt == null) {
                return reportParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return reportParameter;
            }
            reportParameter.typeReport = (String) jsonHash.get("TYPEREPORT");
            reportParameter.supplyChain = (String) jsonHash.get("SUPPLYCHAINPOINT");

            reportParameter.dateFrom = Toolbox.getDate(jsonHash.get("FROMDATE"), null);
            reportParameter.dateTo = Toolbox.getDate(jsonHash.get("TODATE"), null);

            reportParameter.auditTrail = Toolbox.getBoolean(jsonHash.get("AUDITTRAIL"), Boolean.FALSE);
            return reportParameter;
        }

        @Override
        public String toString()
        {
            return "typeReport[" + typeReport + "] supplyChain[" + supplyChain + "] dateFrom[" + dateFrom + "]-[" + dateTo + "] audiTrail[" + auditTrail + "]";
        }
    };

    // FDR-
    public static GcdmToolbox.GcdmResult report(final ReportParameter reportParameter,
            final APISession apiSession)
    {
        final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();

        // calculate the list
        final Connection con = GcdmToolbox.getConnection(reportParameter.allowDirectConnection);
        if (con == null)
        {
            gcdmResult.status = "";
            gcdmResult.errorstatus = "Can't access the datasource [" + GcdmToolbox.DATASOURCE_NAME + "]";

            return gcdmResult;
        }
        String sqlRequest = "";
        try
        {
            GcdmBusinessAccess.completeListSupplyChain(con, gcdmResult);

            if ("gascompositiondefault".equals(reportParameter.typeReport))
            {
                gcdmResult.addHeaderColumns("EFFECTIVEDATETIME", "Effective Date/Time", typeColumn.date);
                gcdmResult.addHeaderColumns("SUPPLYCHAINEPOINT", "Supply Chain Point", typeColumn.text);
                gcdmResult.addHeaderColumns("SPECIFICGRAVITY", "Specific Gravity", typeColumn.text);
                gcdmResult.addHeaderColumns("HEATINGVALUE", "Heating Value", typeColumn.number);
                gcdmResult.addHeaderColumns("METHANE", "Methane C1", typeColumn.number);
                gcdmResult.addHeaderColumns("ETHANE", "Ethane C2", typeColumn.number);
                gcdmResult.addHeaderColumns("PROPANE", "Propane C3", typeColumn.number);
                gcdmResult.addHeaderColumns("IBUTANE", "i-Butane C4i", typeColumn.number);
                gcdmResult.addHeaderColumns("NBUTANE", "n-Butane C4n", typeColumn.number);
                gcdmResult.addHeaderColumns("BUTANE", "Butane C4", typeColumn.number);
                gcdmResult.addHeaderColumns("IPENTANE", "i-Pentane C5i", typeColumn.number);
                gcdmResult.addHeaderColumns("NEOPENTANE", "Neo-Pentane", typeColumn.number);
                gcdmResult.addHeaderColumns("NPENTANE", "n-Pentane C5n", typeColumn.number);
                gcdmResult.addHeaderColumns("PENTANE", "Pentane C5", typeColumn.number);
                gcdmResult.addHeaderColumns("HEPTANE", "Heptane C7", typeColumn.number);
                gcdmResult.addHeaderColumns("OCTANE", "Octane C6", typeColumn.number);
            }

            // FDR-77 The system must display historical, current and future data relating to the data set.
            // FDR-78 The initial display of data must be sorted by Supply Chain Point column (ascending ordered).
            // FDR-80 The system must provide a toggle option allowing users to include audit trail history information within the report.
            //           ==> ADD HISTORIC MARKER
            // FDR-82 The system must display the following additional information for audit trail rows:
            // FDR-83 The user must have the ability to filter the report by Supply Chain Point.
            // FDR-84 The user must have the ability to filter the report by specifying a date range,
            sqlRequest = "select DATECHANGED as EFFECTIVEDATETIME, * from R_ConversionFactors ";
            final List<Object> listRequestObject = new ArrayList<Object>();

            gcdmResult.listValues = GcdmToolbox.executeRequest(con, sqlRequest, listRequestObject, reportParameter.maxRecord,
                    reportParameter.formatDateJson);

            int count = 0;
            for (final Map<String, Object> line : gcdmResult.listValues) {
                count++;
                if (count % 3 == 0 && reportParameter.auditTrail) {
                    line.put("HISTORICMARKER", true);
                }
                line.put("HEATINGVALUE", String.valueOf(System.currentTimeMillis() / 1000 % (60 * 60 * 24)));
            }
            logger.info("Report[" + reportParameter.typeReport + "] Parameters[" + reportParameter.toString() + "] sqlRequest[" + sqlRequest
                    + "] listValue.size() [" + gcdmResult.listValues.size() + "]");

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

}
