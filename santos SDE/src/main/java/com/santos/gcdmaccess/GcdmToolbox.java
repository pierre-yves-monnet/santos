package com.santos.gcdmaccess;

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

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.identity.Role;
import org.bonitasoft.engine.identity.RoleNotFoundException;
import org.bonitasoft.engine.identity.UserMembership;
import org.bonitasoft.engine.identity.UserMembershipCriterion;
import org.bonitasoft.engine.profile.Profile;
import org.bonitasoft.engine.profile.ProfileCriterion;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;


public class GcdmToolbox {

    static Logger logger = Logger.getLogger("org.bonitasoft.SdeAccess");
    public static String DATASOURCE_NAME = "GCDM_DS";
    protected static SimpleDateFormat sdfHuman = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    //     October 13, 2014 11:13:00
    public static SimpleDateFormat sdfJavasscriptHour = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    public static SimpleDateFormat sdfEffectiveDate = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    public static SimpleDateFormat sdfHour = new SimpleDateFormat("HH:mm");

    public static class GcdmResult {

        public String status;
        public String errorstatus;
        public List<Map<String, Object>> listValues = new ArrayList<Map<String, Object>>();
        public Map<String, Object> values = new HashMap<String, Object>();
        public Map<String, List> historyValues = new HashMap<String, List>();

        public Map<String, List<Map<String, Object>>> listsSelect = new HashMap<String, List<Map<String, Object>>>();
        public List<Map<String, Object>> listHeader = new ArrayList<Map<String, Object>>();
        public List<Map<String, Object>> newGasCompositionFields = new ArrayList<Map<String, Object>>();
        public Map<String, Object> newGasCompositionValues = new HashMap<String, Object>();
        public List<BEvent> listEvents = new ArrayList<BEvent>();

        public boolean isEditProfile = false;

        /**
         * @param id
         * @param display
         * @param type "string" or "date" or "number"
         */
        public enum typeColumn {
            text, date, number
        };

        public void addHeaderColumns(final String id, final String display, final typeColumn type) {
            final HashMap<String, Object> oneheader = new HashMap<String, Object>();
            oneheader.put("id", id);
            oneheader.put("display", display);
            oneheader.put("type", type.toString());

            listHeader.add(oneheader);
        }

        public void addEditFields(final String id, final String display, final typeColumn typeofField,
                boolean mandatory,
                final boolean readonly, final Object minrange, final Object maxrange) {
            final HashMap<String, Object> oneField = new HashMap<String, Object>();
            oneField.put("id", id);
            oneField.put("display", display);
            oneField.put("typeoffield", typeofField.toString());
            if (readonly) {
                mandatory = false;
            }

            oneField.put("mandatory", mandatory);
            if (mandatory) {
                oneField.put("cssstyle", "background-color: #e15782;"); //Pink beige bedroom
                oneField.put("cssclass", "santosmandatory");
            }

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
            GcdmToolbox.logger.info("GcdmResult: message[" + status + "] erroMessage[" + errorstatus + "] events[" + listEvents.toString() + "]");
            final Map<String, Object> result = new HashMap<String, Object>();
            result.put("LISTVALUES", listValues);
            result.put("VALUES", values);
            result.put("HISTORYVALUES", historyValues);
            result.put("LISTHEADERS", listHeader);
            result.put("MESSAGE", status);
            result.put("ERRORMESSAGE", errorstatus);
            result.put("LISTSUPPLYCHAIN", listsSelect.get("LISTSUPPLYCHAIN"));
            result.put("NEWGASCOMPOSITIONFIELDS", newGasCompositionFields);
            result.put("NEWGASCOMPOSITIONVALUES", newGasCompositionValues);
            // logger.info("GcdmResult:GetHtml" + BEventFactory.getHtml(listEvents));
            result.put("LISTEVENTS", BEventFactory.getHtml(listEvents));
            result.put("ISEDITPROFILE", isEditProfile);
            return result;
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Profile */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static boolean isAdminProfile(final Long userId, final ProfileAPI profileAPI)
    {
        List<Profile> listProfile;
        listProfile = profileAPI.getProfilesForUser(userId, 0, 1000, ProfileCriterion.NAME_ASC);

        for (final Profile profile : listProfile)
        {
            if (profile.isDefault() && "Administrator".equals(profile.getName())) {
                return true;
            }
        }
        return false;

    }

    /**
     * @param userId
     * @param profileAPI
     * @param identityAPI
     * @return
     */
    public static boolean isEditGcdmProfile(final Long userId, final ProfileAPI profileAPI, final IdentityAPI identityAPI)
    {

        // search by the role
        Role roleGcdm = null;
        try {
            roleGcdm = identityAPI.getRoleByName("ROLEEDITGCDM");
            final List<UserMembership> listMemberShip = identityAPI.getUserMemberships(userId, 0, 2000, UserMembershipCriterion.ROLE_NAME_ASC);
            for (final UserMembership userMemberShip : listMemberShip)
            {
                if (userMemberShip.getRoleId() == roleGcdm.getId()) {
                    return true;
                }
            }
        } catch (final RoleNotFoundException e) {
        };

        // search by the ProfileId
        List<Profile> listProfile;
        listProfile = profileAPI.getProfilesForUser(userId, 0, 1000, ProfileCriterion.NAME_ASC);

        for (final Profile profile : listProfile)
        {
            if ("EDITGCDM".equals(profile.getName())) {
                return true;
            }
        }
        return false;

    }

    /**
     * get the connection
     *
     * @return
     */
    protected static Connection getConnection(final boolean allowDirectConnection)
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

    private static Connection getDirectConnection()
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

    public static List<Map<String, Object>> executeRequest(final Connection con, final String sqlRequest, final List<Object> listRequestObject,
            final int maxRecord,
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
            // logger.info("Read  [" + record + "]");
            listRecords.add(record);
        }
        pstmt.close();
        return listRecords;
    }

}
