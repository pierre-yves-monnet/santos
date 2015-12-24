package com.santos.gcdmaccess;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.session.APISession;

import com.santos.gcdmaccess.GcdmBusinessAccess.GasCompositionParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.NewGasCompositionParameter;
import com.santos.gcdmaccess.GcdmToolbox.GcdmResult;

public class GcdmAccess {

    static Logger logger = Logger.getLogger("org.bonitasoft.GcdmAccess");

    /* ******************************************************************************** */
    /*                                                                                  */
    /* GCDM GasComposition */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static GcdmToolbox.GcdmResult getListGasComposition(final GasCompositionParameter gasCompositionParameter,
            final APISession apiSession)
    {
        GcdmToolbox.GcdmResult gcdmResult = null;
        try
        {
        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();

            gcdmResult = gcdmBusinessAccess.getListGasComposition(gasCompositionParameter);

        final IdentityAPI identityAPI = TenantAPIAccessor.getIdentityAPI(apiSession);
        final ProfileAPI profileAPI = TenantAPIAccessor.getProfileAPI(apiSession);

        gcdmResult.isEditProfile = GcdmToolbox.isEditGcdmProfile(apiSession.getUserId(), profileAPI, identityAPI);
        } catch (final Exception e)
        {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            logger.severe("getListGasComposition: Error " + e.toString() + " at " + sw.toString());
            if (gcdmResult == null) {
                gcdmResult = new GcdmResult();
            }
            gcdmResult.status = "Fail";
            gcdmResult.errorstatus = "Error during call : " + e.toString() + "]";
            gcdmResult.listValues = null;
        }

        return gcdmResult;
    };

    public static GcdmToolbox.GcdmResult getGasComposition(final GasCompositionParameter gasCompositionParameter,
            final APISession apiSession)
    {
        GcdmToolbox.GcdmResult gcdmResult = null;

        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();

        gcdmResult = gcdmBusinessAccess.getGasComposition(gasCompositionParameter);
        return gcdmResult;
    }

    /**
     * delete from the list
     * FDR-53
     *
     * @param gasCompositionParameter
     * @param session
     * @param processAPI
     * @return
     */
    public static Map<String, Object> deleteListGasComposition(final GasCompositionParameter gasCompositionParameter, final APISession session,
            final ProcessAPI processAPI)
    {

        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();

        final GcdmToolbox.GcdmResult gcdmResult = gcdmBusinessAccess.deleteListGasComposition(gasCompositionParameter, processAPI);
        return gcdmResult.toMap();
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* GCDM NewGasComposition */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * search a new SearchListGasComposition
     *
     * @param newGasCompositionParameter
     * @param session
     * @param processAPI
     * @return
     */
    public static Map<String, Object> searchListGasComposition(final NewGasCompositionParameter newGasCompositionParameter, final APISession session,
            final ProcessAPI processAPI)
    {
        if (newGasCompositionParameter.errormessage != null)
        {
            final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
            gcdmResult.errorstatus = newGasCompositionParameter.errormessage;
            return gcdmResult.toMap();
        }
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();
        final GcdmToolbox.GcdmResult gcdmResult = gcdmBusinessAccess.searchListGasComposition(newGasCompositionParameter);
        return gcdmResult.toMap();
    }

    /**
     * add a new GasComposition
     *
     * @param newGasCompositionParameter
     * @param session
     * @param processAPI
     * @return
     */
    public static Map<String, Object> addNewGasComposition(final NewGasCompositionParameter newGasCompositionParameter, final APISession session,
            final ProcessAPI processAPI)
    {
        if (newGasCompositionParameter.errormessage != null)
        {
            final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
            gcdmResult.errorstatus = newGasCompositionParameter.errormessage;
            return gcdmResult.toMap();
        }
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();
        final GcdmToolbox.GcdmResult gcdmResult = gcdmBusinessAccess.addNewGasComposition(newGasCompositionParameter, processAPI);
        return gcdmResult.toMap();
    }



}
