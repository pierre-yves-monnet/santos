package com.santos.gcdmaccess;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.api.TenantAPIAccessor;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.log.event.BEventFactory;

import com.santos.gcdmaccess.GcdmBusinessAccess.CalculGravityParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.GasCompositionParameter;
import com.santos.gcdmaccess.GcdmBusinessAccess.NewGasCompositionParameter;
import com.santos.gcdmaccess.GcdmToolbox.GcdmResult;
import com.santos.toolbox.Toolbox;

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

    // FDR-35 get the Defaut Gas Composition
    public static GcdmToolbox.GcdmResult getDefaultGasComposition(final GasCompositionParameter gasCompositionParameter,
            final APISession apiSession)
    {
        GcdmToolbox.GcdmResult gcdmResult = null;

        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();

        gcdmResult = gcdmBusinessAccess.getDefaultGasComposition(gasCompositionParameter);
        return gcdmResult;
    }

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
    public static GcdmToolbox.GcdmResult deleteListGasComposition(final GasCompositionParameter gasCompositionParameter, final APISession apiSession)
    {

        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();

        final GcdmToolbox.GcdmResult gcdmResult = gcdmBusinessAccess.deleteListGasComposition(gasCompositionParameter, apiSession);
        return gcdmResult;
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
    public static Map<String, Object> searchListGasComposition(final NewGasCompositionParameter newGasCompositionParameter, final APISession session)
    {
        if (BEventFactory.isError(newGasCompositionParameter.listEvents))
        {
            final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
            gcdmResult.listEvents = newGasCompositionParameter.listEvents;
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
    public static Map<String, Object> addNewGasComposition(final NewGasCompositionParameter newGasCompositionParameter, final APISession apiSession)
    {
        if (BEventFactory.isError(newGasCompositionParameter.listEvents))
        {
            final GcdmToolbox.GcdmResult gcdmResult = new GcdmToolbox.GcdmResult();
            gcdmResult.listEvents = newGasCompositionParameter.listEvents;
            return gcdmResult.toMap();
        }
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();
        final GcdmToolbox.GcdmResult gcdmResult = gcdmBusinessAccess.addNewGasComposition(newGasCompositionParameter, apiSession);
        return gcdmResult.toMap();
    }

    public static class StepCalcul
    {

        public String details = "";
        public String formula = "";
        public BigDecimal value;

        public StepCalcul()
        {
            value = new BigDecimal(0.0);
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);
        };

        public StepCalcul(final String explfactor1, final BigDecimal f1)
        {
            details = explfactor1 + "(" + f1 + ")";
            formula = f1 + " ";
            value = f1;
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);
        }

        public static StepCalcul getInstance(final String explfactor1, final BigDecimal f1)
        {
            return new StepCalcul(explfactor1, f1);
        };

        public void parenthesis()
        {
            details = "(" + details + ")";
            formula = "(" + formula + ")";
        }

        public StepCalcul plus(final StepCalcul st)
        {
            details += " + " + st.details;
            formula += " + " + st.formula;

            value = value.add(st.value);
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);
            return this;
        }

        public StepCalcul minus(final StepCalcul st)
        {
            details += " - " + st.details;
            formula += " - " + st.formula;
            value = value.add(st.value.negate());
            return this;
        }

        public StepCalcul divide(final StepCalcul st)
        {
            details += " / " + st.details;
            formula += " / " + st.formula;
            value = value.divide(st.value);
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);
            return this;
        }

        public StepCalcul divide(final long divider)
        {
            details += "(" + details + ") / " + divider;
            formula = "(" + formula + ") / " + divider;
            value = value.divide(BigDecimal.valueOf(divider));
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);
            return this;
        }

        public StepCalcul multiply(final StepCalcul st)
        {
            details += " * " + st.details;
            formula += " * " + st.formula;
            value = value.multiply(st.value);
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);
            return this;
        }

    }

    public static class StepCalculFactory
    {

        public Map<String, BigDecimal> conversion;
        public CalculGravityParameter calculGravityParameter;

        public StepCalcul getValue(final String param)
        {
            return StepCalcul.getInstance(param, Toolbox.getBigDecimal(calculGravityParameter.getValue(param), BigDecimal.valueOf(0)));
        }

        public StepCalcul getFactor(final String param)
        {
            return StepCalcul.getInstance("Factor_" + param, conversion.get(param));
        }

    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* heating value */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static GcdmToolbox.GcdmResult calculateGravityAndHeating(final CalculGravityParameter calculGravityParameter,
            final APISession apiSession)
    {

        // calculate the list
        final GcdmBusinessAccess gcdmBusinessAccess = new GcdmBusinessAccess();
        final GcdmToolbox.GcdmResult gcdmResult = gcdmBusinessAccess.getConversionFactors(calculGravityParameter);
        if (gcdmResult.errorstatus != null)
        {
            return gcdmResult;
        }

        final Map<String, BigDecimal> conversionSGFactor = new HashMap<String, BigDecimal>();
        final Map<String, BigDecimal> conversionHVFactor = new HashMap<String, BigDecimal>();
        for (final Map<String, Object> conversion : gcdmResult.listValues)
        {
            BigDecimal value = Toolbox.getBigDecimal(conversion.get("SGVALUE"), BigDecimal.valueOf(0.0));
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);

            conversionSGFactor.put((String) conversion.get("PGMSCODE"), value);

            value = Toolbox.getBigDecimal(conversion.get("HVVALUE"), BigDecimal.valueOf(0.0));
            value = value.setScale(10, BigDecimal.ROUND_HALF_UP);

            conversionHVFactor.put((String) conversion.get("PGMSCODE"), value);
        }

        /*
         * =(Value_C1*SGConversionFactor_C1
         * +Value_C2*SGConversionFactor_C2
         * +Value_C3*SGConversionFactor_C3
         * +Value_C4I*SGConversionFactor_C4I
         * +Value_C4N*SGConversionFactor_C4N
         * +Value_C5I*SGConversionFactor_C5I
         * +Value_C5N*SGConversionFactor_C5N
         * +Value_CO2*SGConversionFactorCO2
         * +Value_N2*SGConversionFactor_N2)/100
         * + ( Value_H2S*SGConversionFactor_H2S
         * + (Value_C6P - (
         * Value_C8+Value_C9P+Value_BEN+Value_CYC
         * )/1000 * SGConversionFactor_C6P
         * )
         * )/10000
         * + Value_C9P*SGConversionFactor_C9P
         * + Value_NEO*SGConversionFactor_NEO
         * + Value_BEN*SGConversionFactor_BEN
         * + Value_CYC*SGConversionFactor_CYC
         * +(Value_C8-Value_C9P )*SGConversionFactor_C8
         * )/10000000
         */

        // calcul !
        for (int i = 0; i < 2; i++)
        {
            final StepCalculFactory SCF = new StepCalculFactory();
            SCF.conversion = i == 0 ? conversionSGFactor : conversionHVFactor;
            SCF.calculGravityParameter = calculGravityParameter;

            final StepCalcul specificGravity = new StepCalcul();
            final String[] listFirstFactor = new String[] { "C1", "C2", "C3", "C4I", "C4N", "C5I", "C5N", "CO2", "N2" };
            for (final String oneFactor : listFirstFactor) {
                specificGravity.plus(SCF.getValue(oneFactor).multiply(SCF.getFactor(oneFactor)));
            }
            specificGravity.parenthesis();
            specificGravity.divide(100);

            final StepCalcul tempH2S = SCF.getFactor("H2S");
            final StepCalcul tempC8 = SCF.getValue("C8").plus(SCF.getValue("C9P").plus(SCF.getValue("BEN").plus(SCF.getValue("CYC"))));
            tempC8.divide(1000L);
            tempC8.multiply(SCF.getFactor("C6P"));

            tempH2S.plus(tempC8);
            tempH2S.divide(10000);

            specificGravity.plus(tempH2S);

            specificGravity.plus(SCF.getValue("C9P").multiply(SCF.getFactor("C9P")));
            specificGravity.plus(SCF.getValue("NEO").multiply(SCF.getFactor("NEO")));
            specificGravity.plus(SCF.getValue("BEN").multiply(SCF.getFactor("BEN")));
            specificGravity.plus(SCF.getValue("CYC").multiply(SCF.getFactor("CYC")));
            final StepCalcul tempC8PEnd = SCF.getValue("C8").minus(SCF.getValue("C9P"));
            tempC8PEnd.parenthesis();
            tempC8PEnd.multiply(SCF.getFactor("C8"));

            specificGravity.plus(tempC8PEnd);

            // the next part
            if (i == 0)
            {
                gcdmResult.values.put("SPECIFICGRAVITY", specificGravity.value);
                gcdmResult.values.put("SPECIFICGRAVITYFORMULA", specificGravity.formula);
                gcdmResult.values.put("SPECIFICGRAVITYDETAILS", specificGravity.details);
            }
            else
            {
                gcdmResult.values.put("HEATINGVALUE", specificGravity.value);
                gcdmResult.values.put("HEATINGVALUEFORMULA", specificGravity.formula);
                gcdmResult.values.put("HEATINGVALUEDETAILS", specificGravity.details);

            }
        }
        return gcdmResult;
    }

}
