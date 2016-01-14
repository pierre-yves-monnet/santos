package com.santos.toolbox;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Toolbox {

    static Logger logger = Logger.getLogger(Toolbox.class.getName());
    
    public static String version = "SDE Java version 1.97";

    public static SimpleDateFormat sdfJavasscript = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");


    static public Integer getInteger(final Object parameter, final Integer defaultValue)
    {
        if (parameter == null) {
            return defaultValue;
        }
        try
        {
            return Integer.valueOf(parameter.toString());
        } catch (final Exception e)
        {
            logger.severe("Can't decode integer [" + parameter + "]");
            return defaultValue;
        }
    }

    static public Long getLong(final Object parameter, final Long defaultValue)
    {
        if (parameter == null) {
            return defaultValue;
        }
        try
        {
            return Long.valueOf(parameter.toString());
        } catch (final Exception e)
        {
            logger.severe("Can't decode integer [" + parameter + "]");
            return defaultValue;
        }
    }

    static public Double getDouble(final Object parameter, final Double defaultValue)
    {
        if (parameter == null) {
            return defaultValue;
        }
        try
        {
            return Double.valueOf(parameter.toString());
        } catch (final Exception e)
        {
            logger.severe("Can't decode integer [" + parameter + "]");
            return defaultValue;
        }
    }

    static public BigDecimal getBigDecimal(final Object parameter, final BigDecimal defaultValue)
    {
        if (parameter == null) {
            return defaultValue;
        }
        try
        {
            return BigDecimal.valueOf(getDouble(parameter.toString(), defaultValue.doubleValue()));
        } catch (final Exception e)
        {
            logger.severe("Can't decode integer [" + parameter + "]");
            return defaultValue;
        }
    }

    static public Boolean getBoolean(final Object parameter, final Boolean defaultValue)
    {
        if (parameter == null) {
            return defaultValue;
        }
        try
        {
            return Boolean.valueOf(parameter.toString());
        } catch (final Exception e)
        {
            logger.severe("Can't decode boolean [" + parameter + "]");
            return defaultValue;
        }
    }

    static public String getString(final Object parameter, final String defaultValue)
    {
        if (parameter == null) {
            return defaultValue;
        }
        try
        {
            return parameter.toString();
        } catch (final Exception e)
        {
            return defaultValue;
        }
    }

    static public Date getDate(final Object parameter, final Date defaultValue)
    {
        if (parameter instanceof Date) {
            return (Date) parameter;
        } else if (parameter instanceof Long) {
            return new Date((Long) parameter);
        } else if (parameter instanceof String)
        {
            try
            {

                return sdfJavasscript.parse( (String) parameter );
            }
            catch(final ParseException e)
            {

            }
        }
        return defaultValue;

    }

    static public List<Map<String, String>> getList(final Object parameter, final List<Map<String, String>> defaultValue)
    {
        if (parameter == null) {
            return defaultValue;
        }
        try
        {
            return (List<Map<String, String>>) parameter;
        } catch (final Exception e)
        {
            return defaultValue;
        }
    }
    
    // TODO :: Ideally this should be in a DASH wide util class.
    // Taken from Talend SDE project; routines.WellNameUtil
    public static String formatWellName(String name) {

        name = name.trim();//
        name = name.replaceAll(" +", " ");//
        if (!name.isEmpty() && !name.equals("")) {
            String[] nameArray = name.split(" ");
            if (nameArray.length > 1) {
                for (int i = 1; i < nameArray.length; i++) {

                    if (!(nameArray[i].charAt(0) < 48 || nameArray[i].charAt(0) > 57)) {
                        String newName;     	   		 
                        newName = nameArray[i].replaceAll("[^0-9]+", " ").trim();

                        if (!newName.equals("")) {
                            newName = newName.replaceAll(" +", " ");
                            String[] newNameArray = newName.split(" ");
                            if (newNameArray[0].length() == 1) {

                                name = name.replaceFirst(" " + nameArray[i], " " + nameArray[i].replaceFirst(newNameArray[0], "00" + newNameArray[0]));

                            } else if (newNameArray[0].length() == 2) {

                                name = name.replaceFirst(" " + nameArray[i], " " + nameArray[i].replaceFirst(newNameArray[0], "0" + newNameArray[0]));
                            }
                        }

                        break;

                    }

                }
            }           

        }

        return name;
    }
}
