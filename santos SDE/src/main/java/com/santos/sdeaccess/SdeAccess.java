package com.santos.sdeaccess;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.bpm.flownode.ActivityInstance;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.ProcessDeploymentInfo;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.profile.Profile;
import org.bonitasoft.engine.profile.ProfileCriterion;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.session.APISession;
import org.json.simple.JSONValue;

import com.bonitasoft.engine.bpm.process.impl.ProcessInstanceSearchDescriptor;
import com.santos.sdeaccess.SdeBusinessAccess.CreateWellParameter;
import com.santos.sdeaccess.SdeBusinessAccess.PADashboardParameter;
import com.santos.sdeaccess.SdeBusinessAccess.SdeNumberStatus;
import com.santos.sdeaccess.SdeBusinessAccess.SdeParameter;
import com.santos.sdeaccess.SdeBusinessAccess.SdeResult;
import com.santos.sdeaccess.SdeBusinessAccess.SystemSummaryParameter;
import com.santos.sdeaccess.SdeBusinessAccess.TableDashBoard;
import com.santos.sdeaccess.SdeBusinessAccess.WellListParameter;
import com.santos.toolbox.Toolbox;

public class SdeAccess {

    //    private static Logger logger = Logger.getLogger(SdeAccess.class.getName());
    private static Logger logger = Logger.getLogger("org.bonitasoft.SdeAccess");

    public static String version = "SDE Java version 0.0.4";

    static{
        logger.info(version);
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* getWellTrackerDashboardList */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    /**
     * get the list of all cases
     *
     * @param apiSession
     * @param processAPI
     * @param identityAPI
     * @return
     */
    public static class ListCasesParameter {

        /**
         * in order to protect the server, a maxResult is use.
         */
        public int maxResult = 1000;

        /**
         * if true, the source of information is the database, table DASHBOARD.
         * Il false, source of information is the list of cases
         */
        public boolean sourceIsDatabase = true;

        /**
         * if set, then the method "filterOnProcess" is call, in order to check if the case must be filtered, or not.
         * nota : valid only is sourceIsDatabase is true
         */
        public boolean filterOnProcesses = false;

        /**
         * please send back the list of all Business filter value
         * nota : valid only is sourceIsDatabase is true
         */
        public String[] listOfProcesses = null;

        /** user ask to apply a businessFilterValue */
        public boolean completeWithBusinessData = true;

        /**
         * in order to search if an existing case Id + TaskId exist for this user, give information to search the processDefiniion
         */
        public String processName = "SDEdemo";
        public String processVersion = null;
        public SdeParameter sdeParameter = new SdeParameter();

        public static ListCasesParameter getFromJson(final String jsonSt)
        {
            final ListCasesParameter listCasesParameter = new ListCasesParameter();
            if (jsonSt == null) {
                return listCasesParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return listCasesParameter;
            }

            listCasesParameter.maxResult = Toolbox.getInteger(jsonHash.get("maxresult"), 1000);
            listCasesParameter.filterOnProcesses = Toolbox.getBoolean(jsonHash.get("filterOnProcesses"), true);
            listCasesParameter.sourceIsDatabase = Toolbox.getBoolean(jsonHash.get("sourceIsDatabase"), true);
            listCasesParameter.sdeParameter.scheduledOnlineDateInFutur = Toolbox.getBoolean(jsonHash.get("scheduledOnlineDateInFutur"), true);
            listCasesParameter.sdeParameter.filterOnStatus2 = Toolbox.getBoolean(jsonHash.get("filteronstatus2"), true);
            return listCasesParameter;

        }

        @Override
        public String toString()
        {
            return "maxResult[" + maxResult + "] sourceIsDatabase[" + sourceIsDatabase + "] filterOnProcesses[" + filterOnProcesses + "] onlynewrecord["
                    + sdeParameter.scheduledOnlineDateInFutur + "]";
        }
    }

    /**
     * get the list of cases, based on the list of tasks accessible by the user.
     *
     * @param listCasesParameter
     * @param apiSession
     * @param processAPI
     * @param identityAPI
     * @return
     */
    public static Map<String, Object> getWellTrackerDashboardList(final ListCasesParameter listCasesParameter, final APISession apiSession,
            final ProcessAPI processAPI,
            final IdentityAPI identityAPI)
    {
        logger.info("getWellTrackerDashboardList list cases --- param:" + listCasesParameter.toString());
        final HashMap<String, Object> result = new HashMap<String, Object>();
        final SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyy");

        // Search the SDE Demo process

        final ProcessDefinition processDefinition = getProcessDefinition(listCasesParameter.processName, listCasesParameter.processVersion, processAPI);
        // to give access to the TASKID, then search all task available for the user. Then create a map of SDENUMBER/SDESTATUS -> TaskInstance
        final Map<String, HumanTaskInstance> mapSdeNumberToTask = getAllTasksForUser(processDefinition == null ? null : processDefinition.getId(),
                apiSession.getUserId(), processAPI);

        // -------------- search in the database now

        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();

        SearchOptionsBuilder searchOptionsBuilder = new SearchOptionsBuilder(0, listCasesParameter.maxResult);
        String traceinfo = "SdeAccess.getWellTrackerDashboardList:";

        final List<SdeNumberStatus> listSdeNumber = new ArrayList<SdeNumberStatus>();
        final HashMap<SdeNumberStatus, HashMap<String, Object>> mapCasesBySdeNumberStatus = new HashMap<SdeNumberStatus, HashMap<String, Object>>();
        final ArrayList<Map<String, Object>> listCases = new ArrayList<Map<String, Object>>();

        if (listCasesParameter.sourceIsDatabase)
        {
            final Calendar limitToInitiateSde = Calendar.getInstance();
            limitToInitiateSde.set(Calendar.MILLISECOND, 0);
            limitToInitiateSde.set(Calendar.SECOND, 0);
            limitToInitiateSde.set(Calendar.MINUTE, 0);
            limitToInitiateSde.set(Calendar.HOUR_OF_DAY, 0);
            // Get back 1 month and then begining : we are the 10 November, only November and October is OK)

            limitToInitiateSde.add(Calendar.MONTH, -1);
            limitToInitiateSde.set(Calendar.DAY_OF_MONTH, 1);

            final SdeParameter sdeParameter = new SdeParameter();
            sdeParameter.allDashboardRecords = true;
            sdeParameter.formatDateJson = listCasesParameter.sdeParameter.formatDateJson;
            sdeParameter.allowDirectConnection = listCasesParameter.sdeParameter.allowDirectConnection;
            sdeParameter.scheduledOnlineDateInFutur = listCasesParameter.sdeParameter.scheduledOnlineDateInFutur;
            sdeParameter.filterOnStatus2 = listCasesParameter.sdeParameter.filterOnStatus2;
            final SdeResult sdeResult = sdeBusinessAccess.getSynthesisListSdeInformation(listSdeNumber, sdeParameter);
            if (sdeResult.status != null) {
                traceinfo += "sdeBusinessAccess.Status:" + sdeResult.status + ";";
            }
            traceinfo += "ListSource[" + listSdeNumber.size() + "] NumberResult[" + sdeResult.listSdeInformation.size() + "]";
            for (final SdeNumberStatus keySdeNumberStatus : sdeResult.listSdeInformation.keySet())
            {
                final Map<String, Object> sdeInfo = sdeResult.listSdeInformation.get(keySdeNumberStatus);
                final Map<String, Object> caseMap = new HashMap<String, Object>();
                setResultFromDatabase(caseMap, sdeInfo);
                //  final Integer sdeStatus = Toolbox.getInteger(sdeInfo.get(TableDashBoard.SDE_STATUS), null);
                final String submited = (String) sdeInfo.get(TableDashBoard.SUBMITTED);
                final String initiated = (String) sdeInfo.get(TableDashBoard.INITIATED);

                // before : sdeStatus != null && (sdeStatus.intValue() == 0 || sdeStatus.intValue() == 9)) {
                if (!"Y".equals(initiated)) {
                    // #41 : we can initiate ONLY
                    // Initiate SDE Request is possible only if Schedule_online_Date ? SYSDATE < 2 months
                    // (in the future OK, in the past 2 month not. Example : we are the 10 November, only November and October is OK)
                    final Date scheduledOnlineDate = Toolbox.getDate(sdeInfo.get(TableDashBoard.SCHEDULED_ONLINE_DATE), null);

                    if (scheduledOnlineDate != null &&
                            (scheduledOnlineDate.after(limitToInitiateSde.getTime()) || scheduledOnlineDate.equals(limitToInitiateSde.getTime()))) {
                        caseMap.put("initiateSdeRequest", true);
                    } else {
                        caseMap.put("initiateSdeRequest", false);
                    }
                    // TODO
                    // overwrite logic of hiding initateButton
                    // this is not needed for WellTrackerDashboard
                    // rule #51 : if the status is RED, the initiateSdeRequest is not available
                    final String status = Toolbox.getString(sdeInfo.get(SdeBusinessAccess.TableDashBoard.BWD_STATUS), null);
                    if ("RED".equals(status)) {
                        caseMap.put("initiateSdeRequest", false);
                    }

                    logger.info("SdeNumber[" + sdeInfo.get(TableDashBoard.SDE_NUMBER) + "] Date ["
                            + (scheduledOnlineDate == null ? "null" : sdf.format(scheduledOnlineDate)) + "] Limit=[" + sdf.format(limitToInitiateSde.getTime())
                            + "] : access=" + caseMap.get("initiateSdeRequest") + "]");

                    caseMap.put("accesstask", false);
                    if (processDefinition != null)
                    {
                        caseMap.put("processname", processDefinition.getName());
                        caseMap.put("processversion", processDefinition.getVersion());
                        caseMap.put("processid", processDefinition.getId());
                    }
                } else {
                    caseMap.put("initiateSdeRequest", false);
                    caseMap.put("accesstask", false);
                    // then a task should exist for this SDE Request
                    final HumanTaskInstance humanTask = mapSdeNumberToTask.get(keySdeNumberStatus.getKey());
                    if (humanTask != null)
                    {
                        caseMap.put("initiateSdeRequest", false);

                        caseMap.put("accesstask", true);
                        caseMap.put("caseid", humanTask.getParentProcessInstanceId());
                        caseMap.put("taskid", humanTask.getId());
                        caseMap.put("taskName", humanTask.getName());
                        ProcessDefinition processDefinitionTask = processDefinition;
                        if (processDefinition != null && humanTask.getProcessDefinitionId() != processDefinition.getId())
                        {
                            try
                            {
                                processDefinitionTask = processAPI.getProcessDefinition(humanTask.getProcessDefinitionId());
                            } catch (final Exception e) {
                            };
                        }
                        if (processDefinitionTask != null)
                        {
                            caseMap.put("processname", processDefinitionTask.getName());
                            caseMap.put("processversion", processDefinitionTask.getVersion());
                            caseMap.put("processid", processDefinitionTask.getId());
                        }
                    }
                }
                // search if a caseId exist for this one

                caseMap.put("sdenumber", sdeInfo.get(TableDashBoard.SDE_NUMBER));
                caseMap.put("sdestatus", sdeInfo.get(TableDashBoard.SDE_STATUS));
                // caseMap.put("glng", "GLNG");
                // caseMap.put("caseid", processInstance.getId());
                listCases.add(caseMap);
            }

            // sort now
            Collections.sort(listCases, new Comparator<Map<String, Object>>()
            {

                @Override
                public int compare(final Map<String, Object> s1,
                        final Map<String, Object> s2)
                {
                    final Integer sdeNumberS1 = Toolbox.getInteger(s1.get("sdenumber"), 0);
                    final Integer sdeNumberS2 = Toolbox.getInteger(s2.get("sdenumber"), 0);
                    if (sdeNumberS1 != null) {
                        return sdeNumberS1.compareTo(sdeNumberS2);
                    }
                    return 0;
                }
            });

            // in order to give access to the PROCESS INSTANCE, search it
            // to give access to the CASEID, search all cases available based on the CASEID
            final Map<Integer, Map<String, Object>> acumulatorCase = new HashMap<Integer, Map<String, Object>>();
            for (int i = 0; i < listCases.size(); i++)
            {
                final Map<String, Object> caseMap = listCases.get(i);
                final Integer sdeNumber = Toolbox.getInteger(caseMap.get("sdenumber"), null);
                if (sdeNumber != null && caseMap.get("caseid") == null)
                {
                    // search it
                    acumulatorCase.put(sdeNumber, caseMap);
                }
                if (acumulatorCase.size() > 10)
                {
                    resolveCaseId(acumulatorCase, processAPI);
                    acumulatorCase.clear();
                }
            }
            resolveCaseId(acumulatorCase, processAPI);

        }
        else
        {
            // ------------------ source is process

            if (listCasesParameter.filterOnProcesses)
            {
                if (listCasesParameter.listOfProcesses == null || listCasesParameter.listOfProcesses.length == 0)
                {
                    logger.severe("Parameters ask for a Filter on process, but no process is given");
                    traceinfo += "No process to filter;";
                }
                else
                {
                    try
                    {
                        traceinfo += " FilterOnProcess:";
                        searchOptionsBuilder.leftParenthesis();
                        int countFilter = 0;

                        final SearchOptionsBuilder searchOptionsProcessBuilder = new SearchOptionsBuilder(0, 1000);
                        // searchOptionsProcessBuilder.filter(ProcessDeploymentInfoSearchDescriptor.NAME, processName);

                        final SearchResult<ProcessDeploymentInfo> searchResultProcess = processAPI.searchProcessDeploymentInfos(searchOptionsProcessBuilder
                                .done());
                        for (final ProcessDeploymentInfo processDeploymentInfo : searchResultProcess.getResult())
                        {
                            for (final String processName : listCasesParameter.listOfProcesses)
                            {
                                if (processDeploymentInfo.getName().equalsIgnoreCase(processName))
                                {
                                    traceinfo += processDeploymentInfo.getName() + "(" + processDeploymentInfo.getProcessId() + ") ,";
                                    if (countFilter > 0) {
                                        searchOptionsBuilder.or();
                                    }
                                    countFilter++;
                                    searchOptionsBuilder.filter(HumanTaskInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
                                }
                            }
                        }
                        searchOptionsBuilder.rightParenthesis();
                        logger.info("getListCasesForSdeDashboard.Filter on processes :" + traceinfo);
                        if (countFilter == 0)
                        {
                            logger.severe("No process found with filter [" + listCasesParameter.listOfProcesses);
                            searchOptionsBuilder = new SearchOptionsBuilder(0, listCasesParameter.maxResult);
                        }
                    } catch (final Exception e)
                    {
                        final StringWriter sw = new StringWriter();
                        e.printStackTrace(new PrintWriter(sw));
                        final String exceptionDetails = sw.toString();
                        logger.severe("During getListCasesForSdeDashboard " + e.toString() + " at " + exceptionDetails);
                    }
                }

            }

            final HashMap<Long, HashMap<String, Object>> mapCasesByProcessInstance = new HashMap<Long, HashMap<String, Object>>();
            final HashMap<Long, ProcessDefinition> mapProcessDefinition = new HashMap<Long, ProcessDefinition>();

            try
            {
                final SearchResult<ProcessInstance> searchResult = processAPI.searchProcessInstances(searchOptionsBuilder.done());
                traceinfo += "SearchProcessInstance:" + searchResult.getResult().size() + "] processInstanceFound;";

                // Get the complementaty information
                final SearchOptionsBuilder searchTaskOptionsBuilder = new SearchOptionsBuilder(0, listCasesParameter.maxResult);

                for (int i = 0; i < searchResult.getResult().size(); i++)
                {
                    final ProcessInstance processInstance = searchResult.getResult().get(i);

                    final HashMap<String, Object> caseMap = new HashMap<String, Object>();
                    caseMap.put("processinstanceid", processInstance.getId());
                    if (i > 0) {
                        searchTaskOptionsBuilder.or();
                    }

                    searchTaskOptionsBuilder.filter(ActivityInstanceSearchDescriptor.PROCESS_INSTANCE_ID, processInstance.getId());
                    try
                    {
                        final Long sdeNumber = Long.valueOf(processInstance.getStringIndex1());
                        final Long sdeStatus = Long.valueOf(processInstance.getStringIndex2() == null ? "1" : processInstance.getStringIndex2());
                        caseMap.put("sdenumber", sdeNumber);
                        caseMap.put("sdestatus", sdeStatus);
                        caseMap.put("glnv", "GLNV");
                        caseMap.put("caseid", processInstance.getId());
                        final long processDefinitionIdInstance = processInstance.getProcessDefinitionId();
                        ProcessDefinition processDefinitionInstance = mapProcessDefinition.get(processDefinition.getId());
                        if (processDefinitionInstance == null)
                        {
                            processDefinitionInstance = processAPI.getProcessDefinition(processDefinition.getId());
                            mapProcessDefinition.put(processDefinition.getId(), processDefinitionInstance);
                        }
                        if (processDefinitionInstance != null)
                        {
                            caseMap.put("processname", processDefinitionInstance.getName());
                            caseMap.put("processid", processDefinition.getId());
                            caseMap.put("processversion", processDefinitionInstance.getVersion());
                        }
                        // we accept this processinstance
                        listCases.add(caseMap);
                        mapCasesByProcessInstance.put(processInstance.getId(), caseMap);
                        mapCasesBySdeNumberStatus.put(SdeNumberStatus.getInstance(sdeNumber, sdeStatus), caseMap);
                        listSdeNumber.add(SdeNumberStatus.getInstance(sdeNumber, sdeStatus));
                    } catch (final Exception e)
                    {
                        // do nothing, it's acceptable
                        traceinfo += "No sdeNumber in processInstance[" + processInstance.getId() + "] sdeNumber(StringIndex1)["
                                + processInstance.getStringIndex1() + "] sdeStatus(StringIndex2)[" + processInstance.getStringIndex2() + "]";
                    }
                }

                if (listCases.size() > 0)
                {
                    traceinfo += "Complete now by task informations;";
                    // now, let's complete to search all activity for each cases
                    final SearchResult<HumanTaskInstance> searchHumanTaskResult = processAPI.searchHumanTaskInstances(searchTaskOptionsBuilder.done());
                    for (final HumanTaskInstance humanTaskInstance : searchHumanTaskResult.getResult())
                    {
                        final long processInstanceIdTaskInstance = humanTaskInstance.getParentProcessInstanceId();
                        final HashMap<String, Object> caseMap = mapCasesByProcessInstance.get(processInstanceIdTaskInstance);
                        if (caseMap == null)
                        {
                            // not normal !
                            logger.severe("getListCasesForSdeDashboard: processinstance[" + processInstanceIdTaskInstance
                                    + "] not found in list of processInstance");
                            continue;
                        }
                        // we consider we wait only one task
                        caseMap.put("task", humanTaskInstance.getDisplayName());
                    }
                }
            } catch (final SearchException e) {
                final StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                final String exceptionDetails = sw.toString();
                logger.severe("During getListCases " + e.toString() + " at " + exceptionDetails + "traceInfo=" + traceinfo);

                result.put("error", e.toString());
            } catch (final Exception ex)
            {
                final StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                final String exceptionDetails = sw.toString();
                logger.severe("During getListCases " + ex.toString() + " at " + exceptionDetails + "traceInfo=" + traceinfo);

            }

            // call to complete by the business information
            if (listCasesParameter.completeWithBusinessData && listSdeNumber.size() > 0)
            {
                traceinfo += "Complete with BusinessData for [" + listSdeNumber.size() + "] event;";

                final SdeResult sdeResult = sdeBusinessAccess.getSynthesisListSdeInformation(listSdeNumber, listCasesParameter.sdeParameter);
                if (sdeResult.status != null) {
                    traceinfo += "sdeBusinessAccess.Status:" + sdeResult.status + ";";
                }
                traceinfo += "ListSource[" + listSdeNumber.size() + "] NumberResult[" + sdeResult.listSdeInformation.size() + "]";
                for (final SdeNumberStatus keySdeNumberStatus : sdeResult.listSdeInformation.keySet())
                {
                    final Map<String, Object> sdeInfo = sdeResult.listSdeInformation.get(keySdeNumberStatus);
                    final HashMap<String, Object> caseMap = mapCasesBySdeNumberStatus.get(keySdeNumberStatus);
                    if (caseMap == null)
                    {
                        // not normal !
                        logger.severe("getListCasesForSdeDashboard: SdeNumber[" + keySdeNumberStatus + "] not found in list of cases");
                        continue;
                    }

                    setResultFromDatabase(caseMap, sdeInfo);

                }

            }
        } // end sourceIsProcess

        // last case : if there are a caseId, doe not show the initiateSdeRequest
        for (final Map<String, Object> caseMap : listCases)
        {
            if (caseMap.get("caseid") != null) {
                caseMap.put("initiateSdeRequest", false);
            }
        }

        result.put("listcases", listCases);

        logger.info("listCases= " + result + "] trace=" + traceinfo);

        return result;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* WellList */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * get the list of information
     *
     * @param listSdeNumber
     * @param sdeParameter
     * @return
     */
    public static Map<String, Object> getWellList(final WellListParameter wellListParameter)
    {
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();
        final HashMap<String, Object> result = new HashMap<String, Object>();

        final SdeResult sdeResult = sdeBusinessAccess.getWellList(wellListParameter);
        // assuming there are only one result
        result.put("STATUS", sdeResult.status);
        result.put("ERRORSTATUS", sdeResult.errorstatus);
        if (wellListParameter.resultAList)
        {
            result.put("RESULT", sdeResult.listRecords);
            result.put("DETAILS", "" + (sdeResult.listRecords == null ? "0 records founds" : sdeResult.listRecords.size() + " found"));

        }
        else
        {
            if (sdeResult.listRecords != null && sdeResult.listRecords.size() == 1) {
                result.putAll(sdeResult.listRecords.get(0));
            } else {
                logger.severe("getWellList: Only one Record is expected result=" + (sdeResult.listRecords == null ? null : sdeResult.listRecords.size()));
                if (sdeResult.listRecords == null)
                {
                    // don't touch the status here
                }
                else if (sdeResult.listRecords.size() == 0)
                {
                    result.put("STATUS", "NoValue");
                    result.put("DETAILS", "" + (sdeResult.listRecords == null ? "no result" : "No record by the filter"));
                }
                else
                {
                    result.put("STATUS", "TooMuchValue");
                    result.put("DETAILS", "Number of record =" + sdeResult.listRecords.size());
                }
            }
        }
        return result;
    }

    /**
     * create a new Dashboard/ Well info from the given information
     *
     * @param createWellParameter
     * @return
     */
    public static Map<String, Object> createWellList(final CreateWellParameter createWellParameter)
    {
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();
        final HashMap<String, Object> result = new HashMap<String, Object>();

        final SdeResult sdeResult = sdeBusinessAccess.createWellList(createWellParameter);

        result.put("STATUS", sdeResult.status);
        result.put("ERRORSTATUS", sdeResult.errorstatus);
        return result;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* getListSystemSummary */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    /**
     * get the list of information
     *
     * @param listSdeNumber
     * @param sdeParameter
     * @return
     */
    public static Map<String, Object> getListSystemSummary(final SystemSummaryParameter systemSummaryParameter, final APISession apiSession,
            final ProcessAPI processAPI)
    {
        final String[] listSystemSummarydateToString = new String[] { TableDashBoard.SCHEDULED_ONLINE_DATE, TableDashBoard.ACTUAL_ONLINE_DATE,
                "WELL_DATA_DATE", "COMP_DATA_DATE", "RESP_OFFICER_DATE", "EC_DATE", "ENABLE_DATE", "AMPLA_DATE", "GWS_DATE", "OFM_DATE", "OIL_ODR_DATE",
                "PODS_DATE", "SALAS_DATE", "WPM_GLNG_DATE", "SDE_PROCESS_DATE" };
        final String[] listSystemSummarystyle = new String[] { "EC_STATUS", "ENABLE_STATUS", "AMPLA_STATUS", "GWS_STATUS", "OFM_STATUS", "OIL_ODR_STATUS",
                "PODS_STATUS", "SALAS_STATUS", "WPM_GLNG_STATUS" };

        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();
        final HashMap<String, Object> result = new HashMap<String, Object>();
        final Calendar currentDate = Calendar.getInstance();
        currentDate.set(Calendar.MILLISECOND, 0);
        currentDate.set(Calendar.SECOND, 0);
        currentDate.set(Calendar.MINUTE, 0);
        currentDate.set(Calendar.HOUR, 0);

        final ProcessDefinition processDefinition = getProcessDefinition(systemSummaryParameter.processName, systemSummaryParameter.processVersion, processAPI);
        // to give access to the TASKID, then search all task available for the user. Then create a map of SDENUMBER/SDESTATUS -> TaskInstance
        final Map<String, HumanTaskInstance> mapSdeNumberToTask = getAllTasksForUser(processDefinition == null ? null : processDefinition.getId(),
                apiSession.getUserId(), processAPI);

        final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        final SdeResult sdeResult = sdeBusinessAccess.getListSummary(systemSummaryParameter);
        // now, prepare each line for the look and feel
        if (sdeResult.listRecords != null) {
            for (final Map<String, Object> oneLine : sdeResult.listRecords)
            {
                // record contains : {PERMIT_SURFACE=permitsource, JOINT_VENTURE_NAME=JOINT_VENTURE, SALAS_DATE=null, BWD_STATUS=GREEN,
                // OFM_DATE=null, EC_WELL_CODE=EC_WELL_CODE, ARTIFICIAL_LIFT_SYSTEM=LIFT_SYSTEM, STRING_SUFFIX=S, MODIFIED_DATE=1442991600000,
                // EC_FIELD=EC_FIELD, PODS_ICON_WELL_ATTRIBUTE=PODS_ICON, WELL_TEMPLATE=TEMPLATE, OIL_ODR_STATUS=null, MODIFIED_BY=Walter.Bates,
                // OPERATOR_AREA=OPERATOR_AREA, SUBMITTED=N, BOTTOM_HOLE_LONGITUDE=12, PODS_DATE_PARENT_ATTRIBUTE=1442991600000,
                // PERMIT_BOTTOM_HOLE=bottomhole, EC_WELL_TYPE=EC_WELL_TYPE, GENSET_MAKE_MODEL=GENSET_MAKE_MODEL, NUMBER_OF_WELLS_ON_PAD=553,
                // WELL_DATA_DATE=null, ENABLE_DATE=null, LTAP_WELL=L, PODS_PRIMARY_PARENT_ATTRIBUTE=PARENT_ATTRIBUTE, SURFACE_LATITUDE=23,
                // BOTTOM_HOLE_LATITUDE=55, DATA_CONTRIBUTOR=null, PODS_STATUS=null, FIELD_NAME=fieldname, AMPLA_STATUS=null, WPM_GLNG_STATUS=null,
                // PODS_DATE_WELL_ATTRIBUTE=PODS_DATE, PODS_PRIMARY_WELL_ATTRIBUTE=PODS_PRIMARY, PODS_ICON_PARENT_ATTRIBUTE=PARENT_ATTRIBUTE,
                // GWS_STATUS=null, SDE_NUMBER=51383, WPM_GLNG_DATE=null, SALAS_STATUS=null, PRODUCTION_CALCULATION_METHOD=PRODUCTION_METHOD,
                // EC_STATUS=null, RESP_OFFICER_STATUS=null, COUNTRY=COUNTRY, WELL_TYPE=WELL_TYPE, WELL_ALIAS=WellAlias, WATER_DISPOSAL_METHOD=WATER,
                // WELL_LIFECYCLE_STAGE=STAGE, WELL_GROUP=WELL_GROUP, ODR_TEMPLATE=ODR_TEMPLATE, WELL_CODE=WELLCODE, SURFACE_LONGITUDE=43,
                // OIL_ODR_DATE=null, WELL_BORE_INTERVAL=BORE_INTERVAL, HISTORIAN_DATE=null, OP_AREA=OP_AREA, WELL_FULL_NAME=FULL NAME,
                // STATUS_DBMAP=STATUS_DBMAP, MANDATORY=null, SALAS_PARENT=SALAS_PARENT, REINJECTION_WELL=REINJECTION_WELL, PODS_DATE=null,
                // AMPLA_DATE=null, DB_ID=51383, PADDED_NAME=PADDED_NAME, FACILITY_CLASS_1_NAME=FACILITY_CLASS_1_NAME, BWI_ID=51383,
                // WELL_OPERATIONAL_STATUS=STATUS, IS_OIL_PRODUCTION=IS_OIL_PRODUCTION, SDE_STATUS=1, GWS_DATE=null, DATE_WELL_IDENTIFIED=1445583600000,
                // LIFT_TYPE=LIFT_TYPE, PAD_NAME=PAD_NAME, REQUEST_STATUS=1, BWI_DB_ID=51383, WELL_BORE_CODE=BORE_CODE, WELL_DATA_STATUS=null,
                // BUSINESS_UNIT=EABU, OFM_STATUS=null, REQUEST_TYPE=New, ACTUAL_ONLINE_DATE=null, AREA_NUMBER=null, ENABLE_STATUS=null,
                // COMP_DATA_DATE=null, WELL_HOOK_UP_SIMILAR_TO=WELL_HOOK_UP, PODS_WELL_TEMPLATE=WELL_TEMPLATE, WELL_CATEGORY_PRIMARY=CATEGORY_PRIM,
                // CONTRIBUTOR=null, ASSIGNED_RO=Jan.Fisher, PODS_PARENT_TEMPLATE=PARENT_TEMPLATE, SCHEDULED_ONLINE_DATE=1442991600000, COMP_DATA_STATUS=null,
                // WELL_CATEGORY_SECONDARY_1=CATEGORY_SEC_1, COMPLETION_TYPE=COMPLETION_TYPE, WELL_CATEGORY_SECONDARY_2=CATEGORY_SEC_2, GAS_INLET=INLET,
                // HISTORIAN_STATUS=null, EC_DATE=null, WELL_HOOK_UP_DETAILS=WELL_HOOK_UP_DETAILS, WELL_CATEGORY_SECONDARY_3=CATEGORY_SEC_3, FUEL_GAS_CONSUMPTION_RATE=53,
                // WELL_CATEGORY_FINAL=CATEGORY_FINAL, RESP_OFFICER_DATE=null}]

                // calcul the number of date to get online
                final Date scheduledOnlineDate = Toolbox.getDate(oneLine.get(TableDashBoard.SCHEDULED_ONLINE_DATE), null);
                String dayOnLineStyle = "text-align:right";

                if (scheduledOnlineDate != null)
                {
                    final long nbDays = scheduledOnlineDate.getTime() / (1000 * 60 * 60 * 24) - currentDate.getTimeInMillis() / (1000 * 60 * 60 * 24);
                    if (nbDays > 0)
                    {
                        oneLine.put("DAYSTOONLINE", nbDays);
                        if (nbDays >= 15) {
                            dayOnLineStyle += ";background-color:#D7E4BC"; // GREEN 215 228 188
                        } else if (nbDays >= 8) {
                            dayOnLineStyle += ";background-color:#FCD5B4"; // ORANGE 252 213 180
                        } else {
                            dayOnLineStyle += ";background-color:#E6B9B8"; // RED 230 185 184
                        }
                    }
                    else {
                        dayOnLineStyle += ";background-color:#DBE5F1"; //  GREY 219 229 241
                    }

                }
                oneLine.put("DAYSTOONLINE_STYLE", dayOnLineStyle);

                // final Date ActualOnlineDate = Toolbox.getDate( oneLine.get(TableDashBoard.ACTUAL_ONLINE_DATE), null );
                final Object onHold = oneLine.get(TableDashBoard.ON_HOLD);
                if ("Y".equals(onHold)) {
                    oneLine.put(TableDashBoard.ACTUAL_ONLINE_DATE + "_STYLE", "text-align:right;background-color:#FFFF99"); // YELLOW 225 255 153
                } else {
                    oneLine.put(TableDashBoard.ACTUAL_ONLINE_DATE + "_STYLE", "text-align:right");
                }

                final Long sdeNumber = Toolbox.getLong(oneLine.get(TableDashBoard.SDE_NUMBER), -1L);
                final Long sdeStatus = Toolbox.getLong(oneLine.get(TableDashBoard.SDE_STATUS), -1L);
                final SdeNumberStatus sdeNumberStatus = SdeNumberStatus.getInstance(sdeNumber.longValue(), sdeStatus.longValue());

                final HumanTaskInstance humanTask = mapSdeNumberToTask.get(sdeNumberStatus.getKey());
                if (humanTask != null && humanTask.getName().equals(systemSummaryParameter.paTaskName))
                {
                    oneLine.put("PATASKID", humanTask.getId());
                }

                for (final String attr : listSystemSummarystyle)
                {
                    final Object attrValue = oneLine.get(attr);
                    String style = "text-align:center";
                    if (attrValue == null || attrValue.toString().trim().length() == 0)
                    {
                        style += ";background-color:#DBE5F1";
                    }
                    oneLine.put(attr + "_STYLE", style);

                }

                for (final String attr : listSystemSummarydateToString)
                {
                    final Date attrDate = Toolbox.getDate(oneLine.get(attr), null);
                    if (attrDate != null) {
                        oneLine.put(attr + "_ST", sdf.format(attrDate));
                    }

                }

                final String sdeStatus2 = (String) oneLine.get("sde_status2");
                final String sdeStatus8 = (String) oneLine.get("sde_status2");
                if ("Y".equals(sdeStatus2)) {
                    oneLine.put("SDEPROCESSSTATUS", "Complete");
                } else if ("Y".equals(sdeStatus8))
                {
                    oneLine.put("SDEPROCESSSTATUS_STYLE", "background-color:#E6B9B8;text-align:center");
                    oneLine.put("SDEPROCESSSTATUS", "Integration Error");
                }
                else
                {
                    oneLine.put("SDEPROCESSSTATUS_STYLE", "background-color:#FFFF99;text-align:center");
                    oneLine.put("SDEPROCESSSTATUS", "In progress");
                }

            }
        }
        result.put("STATUS", sdeResult.status);
        result.put("DETAILS", "Number of record =" + sdeResult.listRecords.size());
        result.put("RESULT", sdeResult.listRecords);

        return result;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* startProcess */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */
    public static class StartprocessParameter
    {

        /**
         * list of case variable to start the case
         */
        public Map<String, Serializable> processVariables = new HashMap<String, Serializable>();

        /**
         * Process name where the case must be created
         */
        public String processName;

        /**
         * version. If no version is given, a case is created at the last process version
         */
        public String processVersion;

        public boolean waitFirstTask = false;

        public boolean assignTaskToUser = true;
        /**
         * in the situation of the "wait the time", then this is the delay to wait the first task
         */
        public int timeToWaitInMs = 0;

        public static StartprocessParameter getFromJson(final String jsonSt)
        {
            final StartprocessParameter startprocessParameter = new StartprocessParameter();
            if (jsonSt == null) {
                return startprocessParameter;
            }
            final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
            if (jsonHash == null) {
                return startprocessParameter;
            }

            startprocessParameter.processName = (String) jsonHash.get("processname");
            startprocessParameter.processVersion = (String) jsonHash.get("processversion");
            final Map<String, Serializable> processVariables = (Map<String, Serializable>) jsonHash.get("processvariables");

            startprocessParameter.processVariables = new HashMap<String, Serializable>();
            // Parse the variable, and if the name end by _long", translate it to a LongValue
            for (String key : processVariables.keySet())
            {
                final Serializable value = processVariables.get(key);
                if (key.endsWith("_long"))
                {
                    key = key.substring(0, key.length() - "_long".length());
                    startprocessParameter.processVariables.put(key, Toolbox.getLong(value, null));
                } else if (key.endsWith("_integer"))
                {
                    key = key.substring(0, key.length() - "_integer".length());
                    startprocessParameter.processVariables.put(key, Toolbox.getInteger(value, null));
                }
                else {
                    startprocessParameter.processVariables.put(key, value);
                }

            }

            startprocessParameter.waitFirstTask = Toolbox.getBoolean(jsonHash.get("waitfirsttask"), false);
            startprocessParameter.waitFirstTask = Toolbox.getBoolean(jsonHash.get("assigntasktouser"), false);
            startprocessParameter.timeToWaitInMs = Toolbox.getInteger(jsonHash.get("timetowaitinms"), 2000);

            return startprocessParameter;

        }

        @Override
        public String toString()
        {
            String result = "processName[" + processName + "] processVersion[" + processVersion + "]";
            for (final String key : processVariables.keySet())
            {
                final Object value = processVariables.get(key);
                result += key + "(" + (value == null ? "-" : value.getClass().getName()) + "):" + value + ";";
            }
            return result;
        }
    }

    /**
     * @param startprocessParameter
     * @param apiSession
     * @param processAPI
     * @param identityAPI
     * @return
     */

    public static Map<String, Object> startProcessParameter(final StartprocessParameter startprocessParameter, final APISession apiSession,
            final ProcessAPI processAPI,
            final IdentityAPI identityAPI)
    {
        logger.info("StartprocessParameter - param:" + startprocessParameter.toString());
        final HashMap<String, Object> result = new HashMap<String, Object>();
        try {
            Long processDefinitionId = null;
            if (startprocessParameter.processVersion == null)
            {
                processDefinitionId = processAPI.getLatestProcessDefinitionId(startprocessParameter.processName);
            }
            else
            {
                processDefinitionId = processAPI.getProcessDefinitionId(startprocessParameter.processName, startprocessParameter.processVersion);
            }

            result.put("processdefinitionid", processDefinitionId);
            final ProcessDefinition processDefinition = processAPI.getProcessDefinition(processDefinitionId);
            result.put("processname", processDefinition.getName());
            result.put("processversion", processDefinition.getVersion());

            final ProcessInstance processInstance = processAPI.startProcessWithInputs(processDefinitionId, startprocessParameter.processVariables);
            result.put("caseid", processInstance.getId());
            logger.info("StartprocessParameter - case created " + processInstance.getId());
            String message = "Case " + processInstance.getId() + " created";
            if (startprocessParameter.waitFirstTask)
            {

           //     xxxxx



                // ok, let's wait for the first task for this user
                logger.info("StartprocessParameter - Wait first task ");
                final boolean stillWait = true;
                int numberOfLoop = 0;
                Long taskId = null;
                while (stillWait)
                {
                    final SearchOptionsBuilder searchOptionBuilder = new SearchOptionsBuilder(0, 1000);
                    searchOptionBuilder.filter(HumanTaskInstanceSearchDescriptor.PROCESS_INSTANCE_ID, processInstance.getId());
                    // searchOptionBuilder.filter(HumanTaskInstanceSearchDescriptor.USER_ID, apiSession.getUserId());

                    final SearchResult<HumanTaskInstance> searchResult = processAPI.searchAssignedAndPendingHumanTasksFor(processDefinitionId,
                            apiSession.getUserId(), searchOptionBuilder.done());
                    if (searchResult.getCount() > 0)
                    {
                        taskId = searchResult.getResult().get(0).getId();
                        final ActivityInstance activityInstance = processAPI.getActivityInstance(taskId);
                        if (startprocessParameter.assignTaskToUser) {
                            processAPI.assignUserTask(taskId, apiSession.getUserId());
                        }

                        result.put("taskName", activityInstance.getName());
                        result.put("taskDescription", activityInstance.getDescription());
                        result.put("taskid", taskId);
                        break;
                    }
                    else
                    {
                        if (numberOfLoop * 500 > startprocessParameter.timeToWaitInMs) {
                            break;
                        }
                        numberOfLoop++;
                        try {
                            Thread.sleep(500);
                        } catch (final InterruptedException e) {
                        }
                    }

                }
                logger.info("StartprocessParameter - Task found [" + taskId + "]");
                if (taskId != null) {
                    message += "; Task is ready";
                } else {
                    message += "; No task ready";
                }
            }
            result.put("message", message);

        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();
            logger.severe("During startProcess " + e.toString() + " at " + exceptionDetails);

            result.put("error", e.toString());

        }
        logger.info("StartprocessParameter: result=[" + result.toString() + "]");
        return result;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* PADashboard */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    public static Map<String, Object> getListPADAshboard(final PADashboardParameter paDashboardParameter, final APISession session, final ProcessAPI processAPI)
    {
        final Map<String, Object> result = new HashMap<String, Object>();

        // calculate the list
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();

        final SdeResult sdeResult = sdeBusinessAccess.getListPaDashboard(paDashboardParameter,  session,   processAPI);

        result.put("LISTPADASHBOARD", sdeResult.listRecords);
        result.put("MESSAGE", sdeResult.status);
        result.put("ERRORMESSAGE", sdeResult.errorstatus);

        return result;
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Properties access */
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
     * get properties
     *
     * @param propertieFile
     * @return
     */
    public static Map<String, String> getProperties(final String propertieFile)
    {
        final Map<String, String> result = new HashMap<String, String>();

        final Properties properties = new Properties();
        try
        {
            final FileInputStream is = new FileInputStream(propertieFile);
            properties.load(is);
            is.close();
            for (final Object key : properties.keySet())
            {
                result.put(key.toString(), properties.get(key).toString());
            }
        } catch (final Exception e)
        {
        }

        return result;
    }

    /** get the propertuies file */

    public static String getProperties(final String propertieFile, final String attribut)
    {
        return getProperties(propertieFile).get(attribut);
    }

    /**
     * set the properties file
     *
     * @param propertieFile
     * @param jsonSt
     */
    public static String setProperties(final String propertieFile, final String jsonSt)
    {
        final Map<String, Object> jsonHash = (Map<String, Object>) JSONValue.parse(jsonSt);
        logger.info("setProperties jsonst=" + jsonSt + " Hash=" + jsonHash + " file=" + propertieFile);

        if (jsonSt != null && jsonSt.length() > 0 && jsonHash == null) {
            return "Can't decode the Json";
        }

        final Properties properties = new Properties();
        try
        {
            final FileInputStream is = new FileInputStream(propertieFile);
            properties.load(is);
            is.close();
        } catch (final Exception e)
        {
        } // don't care
        try
        {
            if (jsonHash != null) {
                properties.putAll(jsonHash);
            }
            final FileOutputStream os = new FileOutputStream(propertieFile);
            properties.store(os, "");
            os.close();
            return "File [" + propertieFile + "] updated ";
        } catch (final Exception e)
        {

            final StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            final String exceptionDetails = sw.toString();

            logger.severe("setProperties : " + e.toString() + " at " + exceptionDetails);
            return "Error : " + e.toString();
        }
    }

    /* ******************************************************************************** */
    /*                                                                                  */
    /* Private */
    /*                                                                                  */
    /*                                                                                  */
    /* ******************************************************************************** */

    private static void resolveCaseId(final Map<Integer, Map<String, Object>> acumulatorCase, final ProcessAPI processAPI)
    {
        final Map<Long, ProcessDefinition> mapProcessDefinition = new HashMap<Long, ProcessDefinition>();
        final SearchOptionsBuilder searchOptionBuilder = new SearchOptionsBuilder(0, 1000);
        searchOptionBuilder.filter(ProcessInstanceSearchDescriptor.STRING_INDEX_1, "-1");
        for (final Integer sdeNumber : acumulatorCase.keySet())
        {
            searchOptionBuilder.or();
            searchOptionBuilder.filter(ProcessInstanceSearchDescriptor.STRING_INDEX_1, sdeNumber.toString());
        }
        SearchResult<ProcessInstance> searchResult;
        try {
            searchResult = processAPI.searchProcessInstances(searchOptionBuilder.done());
            for (final ProcessInstance processInstance : searchResult.getResult())
            {
                // StringIndex3 contains the Cancel value
                final String isCancel = processInstance.getStringIndex3();
                if ("Y".equals(isCancel)) {
                    continue;
                }
                final Map<String, Object> caseMap = acumulatorCase.get(Toolbox.getInteger(processInstance.getStringIndex1(), null));
                if (caseMap != null) {
                    caseMap.put("caseid", processInstance.getRootProcessInstanceId());

                    ProcessDefinition processDefinition = mapProcessDefinition.get(processInstance.getProcessDefinitionId());
                    if (processDefinition == null)
                    {
                        processDefinition = processAPI.getProcessDefinition(processInstance.getProcessDefinitionId());
                        mapProcessDefinition.put(processInstance.getProcessDefinitionId(), processDefinition);
                    }
                    if (processDefinition != null)
                    {
                        caseMap.put("processname", processDefinition.getName());
                        caseMap.put("processversion", processDefinition.getVersion());
                        caseMap.put("processid", processDefinition.getId());
                    }
                }
            }
        } catch (final SearchException | ProcessDefinitionNotFoundException e) {
            logger.severe("SearchResolveCaseId " + e.toString());
        }

    }

    /**
     * @param caseMap
     * @param sdeInfo
     */
    private static void setResultFromDatabase(final Map<String, Object> caseMap, final Map<String, Object> sdeInfo)
    {

        // we consider we wait only one task
        caseMap.put("ScheduledOnlineDate", sdeInfo.get(SdeBusinessAccess.TableDashBoard.SCHEDULED_ONLINE_DATE));
        caseMap.put("WellCode", sdeInfo.get(SdeBusinessAccess.TableDashBoard.WELL_CODE));
        caseMap.put("WellFullName", sdeInfo.get(SdeBusinessAccess.TableDashBoard.WELL_FULL_NAME));
        caseMap.put("DateWellIdentified", sdeInfo.get(SdeBusinessAccess.TableDashBoard.DATE_WELL_IDENTIFIED));

        caseMap.put("FieldName", sdeInfo.get(SdeBusinessAccess.TableWellInfo.FIELD_NAME));

        caseMap.put("WellCategoryPrimary", sdeInfo.get(SdeBusinessAccess.TableWellInfo.WELL_CATEGORY_PRIMARY));
        caseMap.put("WellCategorySecondary1", sdeInfo.get(SdeBusinessAccess.TableWellInfo.WELL_CATEGORY_SECONDARY_1));
        caseMap.put("WellCategorySecondary2", sdeInfo.get(SdeBusinessAccess.TableWellInfo.WELL_CATEGORY_SECONDARY_2));
        caseMap.put("WellCategorySecondary3", sdeInfo.get(SdeBusinessAccess.TableWellInfo.WELL_CATEGORY_SECONDARY_3));

        caseMap.put("PermitSurface", sdeInfo.get(SdeBusinessAccess.TableWellInfo.PERMIT_SURFACE));
        caseMap.put("PermitBottomHole", sdeInfo.get(SdeBusinessAccess.TableWellInfo.PERMIT_BOTTOM_HOLE));
        caseMap.put("PadName", sdeInfo.get(SdeBusinessAccess.TableWellInfo.PAD_NAME));
        caseMap.put("JointVentureName", sdeInfo.get(SdeBusinessAccess.TableWellInfo.JOINT_VENTURE_NAME));
        caseMap.put("SurfaceLatitude", sdeInfo.get(SdeBusinessAccess.TableWellInfo.SURFACE_LATITUDE));
        caseMap.put("SurfaceLongitude", sdeInfo.get(SdeBusinessAccess.TableWellInfo.SURFACE_LONGITUDE));
        caseMap.put("BottomHoleLatitude", sdeInfo.get(SdeBusinessAccess.TableWellInfo.BOTTOM_HOLE_LATITUDE));
        caseMap.put("BottomHoleLongitude", sdeInfo.get(SdeBusinessAccess.TableWellInfo.BOTTOM_HOLE_LONGITUDE));
        caseMap.put("CompletionType", sdeInfo.get(SdeBusinessAccess.TableWellInfo.COMPLETION_TYPE));

        caseMap.put("BusinessUnit", sdeInfo.get(SdeBusinessAccess.TableWellInfo.BUSINESS_UNIT));
        caseMap.put("RequestType", sdeInfo.get(SdeBusinessAccess.TableDashBoard.REQUEST_TYPE));
        caseMap.put("BasicWellData", sdeInfo.get(SdeBusinessAccess.TableDashBoard.BWD_STATUS));

        // special managmeent Task #43
        final String status = Toolbox.getString(sdeInfo.get(SdeBusinessAccess.TableDashBoard.BWD_STATUS), null);
        if ("GREEN".equals(status) || "ORANGE".equals(status) || "AMBER".equals(status)) {
            caseMap.put("Status", "Ready for action");
        }
        if ("RED".equals(status)) {
            caseMap.put("Status", "Awaiting Data");
        }
        logger.info("Special Management for status=[" + status + "] : caseMap=[" + caseMap.get("Status"));

        caseMap.put("WellDataStatus", sdeInfo.get(SdeBusinessAccess.TableDashBoard.WELL_DATA_STATUS));
        caseMap.put("AssignedRO", sdeInfo.get(SdeBusinessAccess.TableDashBoard.ASSIGN_RO));

    }

    /**
     * get the process Definition
     *
     * @param processName
     * @param processVersion
     * @param processAPI
     * @return
     */
    public static ProcessDefinition getProcessDefinition(final String processName, final String processVersion, final ProcessAPI processAPI)
    {
        // logger.info("Search for ProcessDefinition ["+processName+"] ProcessVersion["+processVersion+"]");
        ProcessDefinition processDefinition = null;
        Long processDefinitionId = null;
        try
        {
            if (processVersion == null)
            {
                processDefinitionId = processAPI.getLatestProcessDefinitionId(processName);
            }
            else
            {
                processDefinitionId = processAPI.getProcessDefinitionId(processName, processVersion);
            }
            processDefinition = processDefinitionId == null ? null : processAPI.getProcessDefinition(processDefinitionId);
            logger.info("Search for ProcessDefinition [" + processName + "] ProcessVersion[" + processVersion + "] : found pid[" + processDefinitionId + "]");
        } catch (final Exception e) {
            logger.severe("Can't find process with processName[" + processName + "] version[" + processVersion + "] : "
                    + e.toString());
        }
        return processDefinition;
    }

    /**
     * @return
     */
    public static Map<String, HumanTaskInstance> getAllTasksForUser(final Long processDefinitionId, final Long userId, final ProcessAPI processAPI)
    {

        final Map<String, HumanTaskInstance> mapSdeNumberToTask = new HashMap<String, HumanTaskInstance>();

        Long processInstanceId = null;
        try
        {

            final SearchOptionsBuilder searchOptionBuilder = new SearchOptionsBuilder(0, 10000);
            if (processDefinitionId != null) {
                searchOptionBuilder.filter(HumanTaskInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDefinitionId);
            }
            final SearchResult<HumanTaskInstance> searchResultPendingHumanTask = processAPI.searchPendingTasksForUser(userId, searchOptionBuilder.done());
            final SearchResult<HumanTaskInstance> searchResultAssignedHumanTask = processAPI.searchAssignedAndPendingHumanTasksFor(processDefinitionId, userId,
                    searchOptionBuilder.done());
            final List<HumanTaskInstance> allTasks = new ArrayList<HumanTaskInstance>();
            allTasks.addAll(searchResultPendingHumanTask.getResult());
            allTasks.addAll(searchResultAssignedHumanTask.getResult());

            for (final HumanTaskInstance humanTask : allTasks)
            {
                processInstanceId = humanTask.getParentProcessInstanceId();
                final ProcessInstance processInstance = processAPI.getProcessInstance(processInstanceId);
                final Long sdeNumber = Toolbox.getLong(processInstance.getStringIndex1(), null);
                final Long sdeStatus = Toolbox.getLong(processInstance.getStringIndex2(), null);
                if (sdeNumber != null && sdeStatus != null)
                {
                    final SdeNumberStatus sdeNumberStatus = SdeNumberStatus.getInstance(sdeNumber.longValue(), sdeStatus.longValue());
                    mapSdeNumberToTask.put(sdeNumberStatus.getKey(), humanTask);
                }
            }
        } catch (final Exception e)
        {
            logger.severe("Can't access human task and string index processDefinitionId[" + processDefinitionId + "] processInstanceId[" + processInstanceId
                    + "] : "
                    + e.toString());

        }
        return mapSdeNumberToTask;
    }

    public static Map<String, HumanTaskInstance> getAllTasksForAllUsers(final Long processDefinitionId, final Long userId, final ProcessAPI processAPI)
    {

        final Map<String, HumanTaskInstance> mapSdeNumberToTask = new HashMap<String, HumanTaskInstance>();

        Long processInstanceId = null;
        try
        {

            final SearchOptionsBuilder searchOptionBuilder = new SearchOptionsBuilder(0, 10000);
            if (processDefinitionId != null) {
                searchOptionBuilder.filter(HumanTaskInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDefinitionId);
}

            final SearchResult<HumanTaskInstance> searchResultPendingHumanTask = processAPI.searchHumanTaskInstances(searchOptionBuilder.done());

            final List<HumanTaskInstance> allTasks = new ArrayList<HumanTaskInstance>();
            allTasks.addAll(searchResultPendingHumanTask.getResult());

            for (final HumanTaskInstance humanTask : allTasks)
            {
                processInstanceId = humanTask.getParentProcessInstanceId();
                final ProcessInstance processInstance = processAPI.getProcessInstance(processInstanceId);
                final Long sdeNumber = Toolbox.getLong(processInstance.getStringIndex1(), null);
                final Long sdeStatus = Toolbox.getLong(processInstance.getStringIndex2(), null);
                if (sdeNumber != null && sdeStatus != null)
                {
                    final SdeNumberStatus sdeNumberStatus = SdeNumberStatus.getInstance(sdeNumber.longValue(), sdeStatus.longValue());
                    mapSdeNumberToTask.put(sdeNumberStatus.getKey(), humanTask);
                }
            }
        } catch (final Exception e)
        {
            logger.severe("Can't access human task and string index processDefinitionId[" + processDefinitionId + "] processInstanceId[" + processInstanceId
                    + "] : "
                    + e.toString());

        }
        return mapSdeNumberToTask;
    }
}

