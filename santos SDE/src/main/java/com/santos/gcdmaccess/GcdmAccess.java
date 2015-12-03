package com.santos.gcdmaccess;

import java.util.HashMap;
import java.util.Map;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.session.APISession;

import com.santos.gcdmaccess.GcdmBusinessAccess.GasCompositionParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.GcdmResult;

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
        final Map<String, Object> result = new HashMap<String, Object>();

        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();

        final GcdmResult gcdmResult = gcdmBusinessAccess.getListGasComposition(gasCompositionParameter);

        result.put("LISTGASCOMPOSITION", gcdmResult.listRecords);
        result.put("MESSAGE", gcdmResult.status);
        result.put("ERRORMESSAGE", gcdmResult.errorstatus);
        result.put("LISTSUPPLYCHAIN", gcdmResult.listsSelect.get("LISTSUPPLYCHAIN"));
        return result;
    }
}
