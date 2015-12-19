package com.santos.gcdmaccess;

import java.util.Map;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.session.APISession;

import com.santos.gcdmaccess.GcdmBusinessAccess.GasCompositionParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.GcdmResult;
import com.santos.gcdmaccess.GcdmBusinessAccess.NewGasCompositionParameter;

public class GcdmAccess {

    /* ******************************************************************************** */
    /*                                                                                  */
    /* GCDM GasComposition */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static Map<String, Object> getListGasComposition(final GasCompositionParameter gasCompositionParameter, final APISession session,
            final ProcessAPI processAPI)
    {
        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();

        final GcdmResult gcdmResult = gcdmBusinessAccess.getListGasComposition(gasCompositionParameter);

        return gcdmResult.toMap();
    };

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

        final GcdmResult gcdmResult = gcdmBusinessAccess.deleteListGasComposition(gasCompositionParameter, processAPI);
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
            final GcdmResult gcdmResult = new GcdmResult();
            gcdmResult.errorstatus = newGasCompositionParameter.errormessage;
            return gcdmResult.toMap();
        }
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();
        final GcdmResult gcdmResult = gcdmBusinessAccess.searchListGasComposition(newGasCompositionParameter);
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
            final GcdmResult gcdmResult = new GcdmResult();
            gcdmResult.errorstatus = newGasCompositionParameter.errormessage;
            return gcdmResult.toMap();
        }
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();
        final GcdmResult gcdmResult = gcdmBusinessAccess.addNewGasComposition(newGasCompositionParameter, processAPI);
        return gcdmResult.toMap();
    }

}
