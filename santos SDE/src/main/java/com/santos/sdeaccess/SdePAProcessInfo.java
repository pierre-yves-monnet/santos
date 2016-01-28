/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.santos.sdeaccess;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.ActivityInstanceSearchDescriptor;
import org.bonitasoft.engine.bpm.flownode.HumanTaskInstance;
import org.bonitasoft.engine.bpm.process.ProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessInstance;
import org.bonitasoft.engine.exception.SearchException;
import org.bonitasoft.engine.search.SearchOptionsBuilder;
import org.bonitasoft.engine.search.SearchResult;
import org.bonitasoft.engine.session.APISession;

/**
 * @author ibiha
 */
public class SdePAProcessInfo {

    private static Logger logger = Logger.getLogger("org.bonitasoft.SdePAProcessInfo");

    public static Map<String, Object> getHumanTasksForSDENumber(final HashMap<String, Object> record, final SdeAccess.ListCasesParameter listCasesParameter,
            final APISession apiSession,
            final ProcessAPI processAPI,
            final IdentityAPI identityAPI) {

        final Long SDE_NUMBER = Long.valueOf(record.get("SDE_NUMBER").toString());
        final Long SDE_STATUS = Long.valueOf("9");

        record.put("KEEP_RECORD", false);

        final SdeBusinessAccess.SdeNumberStatus KEY = SdeBusinessAccess.SdeNumberStatus.getInstance(SDE_NUMBER, SDE_STATUS);

        final HashMap<String, Object> result = new HashMap<String, Object>();
        final SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyy");

        // Search the SDE Demo process
        logger.info("SdePAProcessInfo.getHumanTasksForSDENumber : search [" + listCasesParameter.processName + "] version[" + listCasesParameter.processVersion
                + "]");
        final ProcessDefinition processDefinition = SdeAccess.getProcessDefinition(listCasesParameter.processName, listCasesParameter.processVersion,
                processAPI);

        // to give access to the TASKID, then search all task available for the user. Then create a map of SDENUMBER/SDESTATUS -> TaskInstance
        final Map<String, HumanTaskInstance> mapSdeNumberToTask = SdeAccess.getAllTasksForAllUsers(
                processDefinition == null ? null : processDefinition.getId(),
                apiSession.getUserId(), processAPI);

        // -------------- search in the database now
        final SdeBusinessAccess sdeBusinessAccess = new SdeBusinessAccess();

        final SearchOptionsBuilder searchOptionsBuilder = new SearchOptionsBuilder(0, listCasesParameter.maxResult);
        String traceinfo = "SdeAccess.getWellTrackerDashboardList:";

        final List<SdeBusinessAccess.SdeNumberStatus> listSdeNumber = new ArrayList<SdeBusinessAccess.SdeNumberStatus>();
        final HashMap<SdeBusinessAccess.SdeNumberStatus, HashMap<String, Object>> mapCasesBySdeNumberStatus = new HashMap<SdeBusinessAccess.SdeNumberStatus, HashMap<String, Object>>();
        final ArrayList<Map<String, Object>> listCases = new ArrayList<Map<String, Object>>();

        {
            // ------------------ source is process

            //            if (listCasesParameter.filterOnProcesses) {
            //                if (listCasesParameter.listOfProcesses == null || listCasesParameter.listOfProcesses.length == 0) {
            //                    logger.severe("Parameters ask for a Filter on process, but no process is given");
            //                    traceinfo += "No process to filter;";
            //                } else {
            //                    try {
            //                        traceinfo += " FilterOnProcess:";
            //                        searchOptionsBuilder.leftParenthesis();
            //                        int countFilter = 0;
            //
            //                        final SearchOptionsBuilder searchOptionsProcessBuilder = new SearchOptionsBuilder(0, 1000);
            //                        final SearchResult<ProcessDeploymentInfo> searchResultProcess = processAPI.searchProcessDeploymentInfos(searchOptionsProcessBuilder.done());
            //                        for (final ProcessDeploymentInfo processDeploymentInfo : searchResultProcess.getResult()) {
            //                            for (final String processName : listCasesParameter.listOfProcesses) {
            //                                if (processDeploymentInfo.getName().equalsIgnoreCase(processName)) {
            //                                    traceinfo += processDeploymentInfo.getName() + "(" + processDeploymentInfo.getProcessId() + ") ,";
            //                                    if (countFilter > 0) {
            //                                        searchOptionsBuilder.or();
            //                                    }
            //                                    countFilter++;
            //                                    searchOptionsBuilder.filter(HumanTaskInstanceSearchDescriptor.PROCESS_DEFINITION_ID, processDeploymentInfo.getProcessId());
            //                                }
            //                            }
            //                        }
            //                        searchOptionsBuilder.rightParenthesis();
            ////                        logger.info("getListCasesForSdeDashboard.Filter on processes :" + traceinfo);
            //                        if (countFilter == 0) {
            //                            logger.severe("No process found with filter [" + listCasesParameter.listOfProcesses);
            //                            searchOptionsBuilder = new SearchOptionsBuilder(0, listCasesParameter.maxResult);
            //                        }
            //                    } catch (final Exception e) {
            //                        final StringWriter sw = new StringWriter();
            //                        e.printStackTrace(new PrintWriter(sw));
            //                        final String exceptionDetails = sw.toString();
            //                        logger.severe("During getListCasesForSdeDashboard " + e.toString() + " at " + exceptionDetails);
            //                    }
            //                }
            //
            //            }
            final HashMap<Long, HashMap<String, Object>> mapCasesByProcessInstance = new HashMap<Long, HashMap<String, Object>>();
            final HashMap<Long, ProcessDefinition> mapProcessDefinition = new HashMap<Long, ProcessDefinition>();

            try {
                final SearchResult<ProcessInstance> searchResult = processAPI.searchProcessInstances(searchOptionsBuilder.done());
                traceinfo += "SearchProcessInstance:" + searchResult.getResult().size() + "] processInstanceFound;";

                // Get the complementaty information
                final SearchOptionsBuilder searchTaskOptionsBuilder = new SearchOptionsBuilder(0, listCasesParameter.maxResult);

                for (int i = 0; i < searchResult.getResult().size(); i++) {
                    final ProcessInstance processInstance = searchResult.getResult().get(i);

                    final HashMap<String, Object> caseMap = new HashMap<String, Object>();
                    caseMap.put("processinstanceid", processInstance.getId());
                    if (i > 0) {
                        searchTaskOptionsBuilder.or();
                    }

                    searchTaskOptionsBuilder.filter(ActivityInstanceSearchDescriptor.PROCESS_INSTANCE_ID, processInstance.getId());
                    try {
                        final Long sdeNumber = Long.valueOf(processInstance.getStringIndex1());
                        final Long sdeStatus = Long.valueOf(processInstance.getStringIndex2() == null ? "1" : processInstance.getStringIndex2());

                        if (SDE_NUMBER.equals(sdeNumber)) {
                            final HumanTaskInstance humanTask = mapSdeNumberToTask.get(KEY.toString());
                            if (humanTask != null) {
                                record.put("caseid", humanTask.getParentProcessInstanceId());
                                record.put("taskid", humanTask.getId());
                                record.put("taskName", humanTask.getName());
                                record.put("KEEP_RECORD", true);
                            }

                        }
                        //                        caseMap.put("sdenumber", sdeNumber);
                        //                        caseMap.put("sdestatus", sdeStatus);
                        //                        caseMap.put("glnv", "GLNV");

                        //caseMap.put("caseid", processInstance.getId());
                        ProcessDefinition processDefinitionInstance = mapProcessDefinition.get(processDefinition.getId());
                        if (processDefinitionInstance == null) {
                            processDefinitionInstance = processAPI.getProcessDefinition(processDefinition.getId());
                            mapProcessDefinition.put(processDefinition.getId(), processDefinitionInstance);
                        }
                        if (processDefinitionInstance != null) {
                            caseMap.put("processname", processDefinitionInstance.getName());
                            caseMap.put("processid", processDefinition.getId());
                            caseMap.put("processversion", processDefinitionInstance.getVersion());
                        }
                        // we accept this processinstance
                        listCases.add(caseMap);

                        //                        System.out.println(caseMap.get("processname"));
                        //                        System.out.println(caseMap.get("processid"));
                        //                        System.out.println(caseMap.get("processversion"));
                        record.put("processname", caseMap.get("processname"));
                        record.put("processversion", caseMap.get("processversion"));

                        //                        mapCasesByProcessInstance.put(processInstance.getId(), caseMap);
                        //                        mapCasesBySdeNumberStatus.put(SdeBusinessAccess.SdeNumberStatus.getInstance(sdeNumber, sdeStatus), caseMap);
                        //                        listSdeNumber.add(SdeBusinessAccess.SdeNumberStatus.getInstance(sdeNumber, sdeStatus));
                    } catch (final Exception e) {
                        // do nothing, it's acceptable
                        traceinfo += "No sdeNumber in processInstance[" + processInstance.getId() + "] sdeNumber(StringIndex1)["
                                + processInstance.getStringIndex1() + "] sdeStatus(StringIndex2)[" + processInstance.getStringIndex2() + "]";
                    }
                }

                //                if (listCases.size() > 0) {
                //
                //                    traceinfo += "Complete now by task informations;";
                //                    // now, let's complete to search all activity for each cases
                //                    final SearchResult<HumanTaskInstance> searchHumanTaskResult = processAPI.searchHumanTaskInstances(searchTaskOptionsBuilder.done());
                //                    for (final HumanTaskInstance humanTaskInstance : searchHumanTaskResult.getResult()) {
                //
                //                        System.out.println("");
                //                        System.out.println(humanTaskInstance.getParentProcessInstanceId());
                //                        System.out.println(humanTaskInstance.getId());
                //                        System.out.println(humanTaskInstance.getName());
                //
                //                        record.put("caseid", humanTaskInstance.getParentProcessInstanceId());
                //                        record.put("taskid", humanTaskInstance.getId());
                //                        record.put("taskName", humanTaskInstance.getName());
                //
                //                        final long processInstanceIdTaskInstance = humanTaskInstance.getParentProcessInstanceId();
                //                        final HashMap<String, Object> caseMap = mapCasesByProcessInstance.get(processInstanceIdTaskInstance);
                //                        if (caseMap == null) {
                //                            // not normal !
                //                            logger.severe("getListCasesForSdeDashboard: processinstance[" + processInstanceIdTaskInstance
                //                                    + "] not found in list of processInstance");
                //                            continue;
                //                        }
                //                        // we consider we wait only one task
                //                        caseMap.put("task", humanTaskInstance.getDisplayName());
                //                    }
                //                }
            } catch (final SearchException e) {
                final StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                final String exceptionDetails = sw.toString();
                logger.severe("During getListCases " + e.toString() + " at " + exceptionDetails + "traceInfo=" + traceinfo);

                result.put("error", e.toString());
            } catch (final Exception ex) {
                final StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                final String exceptionDetails = sw.toString();
                logger.severe("During getListCases " + ex.toString() + " at " + exceptionDetails + "traceInfo=" + traceinfo);
            }

            /*
             * var processName = oneTask.processname;
             * var processVersion = oneTask.processversion;
             * var taskName = oneTask.taskName;
             * var caseId = oneTask.caseid;
             * var taskId = oneTask.taskid;
             */
            // call to complete by the business information
            //            if (listCasesParameter.completeWithBusinessData && listSdeNumber.size() > 0) {
            //                traceinfo += "Complete with BusinessData for [" + listSdeNumber.size() + "] event;";
            ////xxxxx
            //
            //                SdeBusinessAccess.PADashboardParameter paDashboardParameter = new SdeBusinessAccess.PADashboardParameter();
            //                //final SdeBusinessAccess.SdeResult sdeResult = sdeBusinessAccess.getListPaDashboard(paDashboardParameter);
            ////                if (sdeResult.status != null) {
            ////                    traceinfo += "sdeBusinessAccess.Status:" + sdeResult.status + ";";
            ////                }
            ////                traceinfo += "ListSource[" + listSdeNumber.size() + "] NumberResult[" + sdeResult.listSdeInformation.size() + "]";
            ////                for (final SdeBusinessAccess.SdeNumberStatus keySdeNumberStatus : sdeResult.listSdeInformation.keySet()) {
            ////                    final Map<String, Object> sdeInfo = sdeResult.listSdeInformation.get(keySdeNumberStatus);
            ////
            ////
            ////                    System.out.println("mapCasesBySdeNumberStatus.keySet()  "+mapCasesBySdeNumberStatus.keySet());
            ////
            ////                    System.out.println("keySdeNumberStatus " + keySdeNumberStatus);
            ////
            ////
            ////                    final HashMap<String, Object> caseMap = mapCasesBySdeNumberStatus.get(keySdeNumberStatus);
            ////
            ////
            ////
            ////                    if (caseMap == null) {
            ////                        // not normal !
            ////
            ////                        System.out.println("getListCasesForSdeDashboard: SdeNumber[" + keySdeNumberStatus + "] not found in list of cases");
            ////                        logger.severe("getListCasesForSdeDashboard: SdeNumber[" + keySdeNumberStatus + "] not found in list of cases");
            ////                        continue;
            ////                    }
            ////
            //////                    setResultFromDatabase(caseMap, sdeInfo);
            ////                }
            //
            //            }
            final Boolean KEEP_RECORD = (Boolean) record.get("KEEP_RECORD");

            if (KEEP_RECORD) {

                final String line = "SDE_NUMBER = " + SDE_NUMBER + ""
                        + " [processname=" + record.get("processname") + "]"
                        + " [processversion=" + record.get("processversion") + "]"
                        + " [taskName" + record.get("taskName") + "]"
                        + " [caseid" + record.get("caseid") + "]"
                        + " [taskid" + record.get("taskid") + "]";
                logger.info("SdePAProcessInfo.getHumanTasksForSDENumber ::  Keeping Human Task Information for " + line);

            } else {
                logger.info("SdePAProcessInfo.getHumanTasksForSDENumber ::  Skipping Human Task Information for SDE_NUMBER = " + SDE_NUMBER);
            }

        } // end sourceIsProcess

        /**
         * *****************************
         * var processName var processVersion var taskName var caseId var taskId * *****************************
         */
        // last case : if there are a caseId, doe not show the initiateSdeRequest
        //        for (final Map<String, Object> caseMap : listCases) {
        //            if (caseMap.get("caseid") != null) {
        //                caseMap.put("initiateSdeRequest", false);
        //            }
        //        }
        //        result.put("listcases", listCases);
        //        logger.info("listCases= " + result + "] trace=" + traceinfo);
        return record;
    }
}
