'use strict';
/**
 *
 */

(function() {


var appCommand = angular.module('dashboardapp', ['googlechart', 'ui.bootstrap','ngModal']);





// --------------------------------------------------------------------------
//
// Controler DashBoard
//
// --------------------------------------------------------------------------

// DashBoard the server
appCommand.controller('DashboardControler',
	function ( $http, $scope, $filter ) {
	
	// this.listheader = [ {"title":"cardNo" },{ "title":"AccountNo"},{ "title":"CorporateAccount"},{ "title":"Customer Name"},{ "title":"Phone"} ];
	this.listheader = [ {"title":"Date Well Identified", "id":"DateWellIdentified", "size":"12", "ctrl":"date"},
						{"title":"UWI", "id":"WellCode" , "size":"20"}, 
						{"title":"Well Full Name", "id":"WellFullName", "size":"20" }, 
						{"title":"Business unit", "id":"BusinessUnit", "size":"12" }, 
						{"title":"Request Type", "id":"RequestType", "size":"12" }, 
						{"title":"Schd online Date", "id":"ScheduledOnlineDate","size":"20", "ctrl":"date" }, 
						{"title":"Basic Well Data", "id":"BasicWellData", "size":"10", "ctrl":"semaphore" },
						{"title":"Status", "id":"Status", "size":"20" }, 
						{"title":"Assigned RO", "id":"AssignedRO", "size":"20" },
						{"title":"", "id":"buttons", "size":"0","ctrl":"button" },
						];

	this.scheduledOnlineDateInFutur=true;
	this.casemaxresult = 100;
	this.listcases=[ ];
	this.filtercase={};
	this.sourceformurl="";
	this.detailHistoryDataContributorEABU = { "sdenumber": "EABU",
												"ScheduledOnlineDate": "",
												"WellCode" : "Operations Geology Technical Assistant" ,
												"WellFullName" : "Operations Geology Technical Assistant",
												"FieldName" : "Operations Geology Technical Assistant",
												"PermitSurface" : "GIS Officer",
												"PermitBottomHole" : "GIS Officer",
												"PadName" : "Operations Geology Technical Assistant",
												"JointVentureName" : "Development Responsible Owner",
												"SurfaceLatitude" : "Operations Geology Technical Assistant",
												"SurfaceLongitude" : "Operations Geology Technical Assistant",
												"BottomHoleLatitude" : "Operations Geology Technical Assistant",
												"BottomHoleLongitude" : "Operations Geology Technical Assistant",
												"CompletionType" : "Operations Geology Technical Assistant"
												};
	this.detailHistoryDataContributorGLNG = { 
		"sdenumber": "GLNG",
		"ScheduledOnlineDate": "",
		"WellCode" : "TA subsurface gp - potentionally - tanya - Marivic will email",
			"WellFullName" : "TA subsurface gp - potentionally - tanya - Marivic will email - see dbMap",
			"WellAlias": "Field Development Planning Team Leader (for the relevant field)",
			"FieldName" : "Production Data team",
			"AreaNumber" : "Production Data team",
			"WellCategoryPrimary": "",
			"WellCategorySecondary1": "",
			"WellCategorySecondary2": "",
			"WellCategorySecondary3": "",
			"PermitSurface" : "Field Development Planning Team Leader (for the relevant field)",
			"PermitBottomHole" : "Field Development Planning Team Leader (for the relevant field)",
			"PadName" : "Field Development Planning Team Leader (for the relevant field)",
			"SurfaceLatitude" : "Field Development Planning Team Leader (for the relevant field)",
			"SurfaceLongitude" : "Field Development Planning Team Leader (for the relevant field)",
			"BottomHoleLatitude" : "Field Development Planning Team Leader (for the relevant field)",
			"BottomHoleLongitude" : "Field Development Planning Team Leader (for the relevant field)",

								};
	this.detailHistoryDataContributor={};
	this.isshowdetailcontributor=false;
	
	this.init = function() {
		var self=this;
	
		$http.get( '../API/system/session/1').success( function (data ) {
			self.userid = data.user_id; 
			});
		$http.get( '?page=custompage_SDEdashboard&action=init')
			.then( function ( jsonResult ) {	
						self.isAllowPropertiesView = jsonResult.data.isAllowPropertiesView;
						self.properties.updatesdeurl= jsonResult.data.updatesdeurl;
					},
					function ( jsonResult ) {
							});
		
	 };
	this.init();
	
	this.refreshcases = function(  )
	{
	
		var self=this;
		this.filtercase={};
		this.detailHistory={};
		
		this.orderByField='';
		var post = { "maxresult" : this.casemaxresult, "scheduledOnlineDateInFutur":this.scheduledOnlineDateInFutur };
		var json= angular.toJson(post, false);
		console.log("Call Server to get list of cases");
		$http.get( '?page=custompage_SDEdashboard&action=getWellTrackerDashboardList&json='+json )
				.success( function ( jsonResult ) {

					self.listcases 				= jsonResult.listcases;
					console.log('get all task listBusinessFilter '+angular.toJson(self.listcases ));
				})
				.error( function(e) {
					alert('an error occure during retrieve all cases');
					console.log('error receive' , e);
					});

	};
	this.refreshcases();


	// --------------------------------------------------------------------------
	//
	//   Callback
	//
	// --------------------------------------------------------------------------
	
	this.link  =function( headerLabel, oneItem) 
	{
		
		if (headerLabel.link == "accessBusinessUnit")
			alert("call Businessunint "+oneItem.BusinessUnit);
		// Url is 
		// http://localhost:8080/bonita/portal/resource/processInstance/SDEdemo/1.0/content/?id=11003&locale=en

		if (headerLabel.link == "accessDateWell") {
			this.isshowform=true;
			this.formtitle="Case "+oneItem.caseid;
			this.sourceformurl= '/bonita/portal/resource/processInstance/'+oneItem.processname+'/'+oneItem.processversion+'/content/?id='+oneItem.caseid+'&typeOfDisplay=basicwellInfo';
		}
		if (headerLabel.link == "accessDateWellGLNV") {
			this.isshowform=true;
			this.formtitle="Case "+oneItem.caseid;
			this.sourceformurl= '/bonita/portal/resource/processInstance/'+oneItem.processname+'/'+oneItem.processversion+'/content/?id='+oneItem.caseid+'&typeOfDisplay=basicwellGLNG';
		}
	}
	
	this.showDetails = function( oneTask ) {
		this.showModal(); 
		this.detailHistory = oneTask;
		if (oneTask.BusinessUnit == "EABU")
			this.detailHistoryDataContributor= this.detailHistoryDataContributorEABU;
		else
			this.detailHistoryDataContributor= this.detailHistoryDataContributorGLNG;
	}
	
	this.errormessage="";
	this.message ="";
	this.initiateSdeRequestbutton = function( headerLabel, oneTask ) {
	
			var post = { "processvariables" : {"ctrSdeNumber_integer" : oneTask.sdenumber, "ctrSdeStatus_integer":oneTask.sdestatus},
							"processname":"SDEdemo", 
							"waitfirsttask":true, 
							"timetowaitinms":"2000" };
			var json= angular.toJson(post, false);
			this.errormessage="";
			this.message="Creation in progress";
			

			var self=this;
			$http.get( '?page=custompage_SDEdashboard&action=startSdeRequest&json='+json )
					.success( function ( jsonResult ) {
						self.errormessage	= jsonResult.error;
						
						var processName			= jsonResult.processname;
						var processVersion		= jsonResult.processversion;
						var taskName			= jsonResult.taskName;
						var caseId				= jsonResult.caseid;
						var taskId				= jsonResult.taskid;
						self.message			= jsonResult.message;
						if (caseId!=null) {
							self.message= self.message+"; Access the task...";
							// assign the taskId
							var assigndata = {"assigned_id" : self.userid  };
							$http.put( '../API/bpm/humanTask/'+taskId )
								.success( function ( jsonResult ) {		
									self.message= "Display the task...";
																		// alert("Case "+caseId+" is started - taskId="+taskId)
									self.showtaskform( processName, processVersion, taskName, caseId, taskId);
								})
								.error( function ( data ) {
									self.errormessage	= "The task can't be assign to you:" +angular.toJson(data);
									self.message= "";
									alert("the task can't be assign to you "+angular.toJson(data));
								});
						}
						// self.isshowcases=false;
					})
					.error( function(e) {
						self.errormessage	= "An error occured during create SDE Request";
						self.message="";
						alert('an error occure during create SDE Request');
						console.log('error receive' , e);
						});
		}
	this.accessTaskButton = function(headerLabel, oneTask ) {
	
		var processName			= oneTask.processname;
		var processVersion		= oneTask.processversion;
		var taskName			= oneTask.taskName;
		var caseId				= oneTask.caseid;
		var taskId				= oneTask.taskid;
				
		this.showtaskform( processName, processVersion, taskName, caseId, taskId);
	}
	
	this.accessCaseOverviewButton = function(headerLabel, oneTask ) {
	
		var processName			= oneTask.processname;
		var processVersion		= oneTask.processversion;
		var taskName			= oneTask.taskName;
		var caseId				= oneTask.caseid;
		var taskId				= oneTask.taskid;
		
		this.formtitle = "Case "+caseId;
		this.sourceformurl= '/bonita/portal/resource/processInstance/'+processName+'/'+processVersion+'/content/?id='+caseId;
		console.log("URL: ",this.sourceformurl);
		this.isshowcases=false;
		this.isshowform=true;
	}
	// --------------------------------------------------------------------------
	//
	//   manage task force
	//
	// --------------------------------------------------------------------------
	
	this.showtaskform = function( processName, processVersion, taskName, caseId, taskId) {
		this.formtitle = "Tasks "+caseId;
		// this.sourceformurl= '/bonita/portal/homepage?ui=form&locale=en&tenant=1#form=' + processName + '--' + processVersion + '--' + taskName +'$entry&task=' + taskId+'&mode=form&assignTask=true'
		// http://localhost:8080/bonita/portal/homepage#?id=20044&_p=performTask&_pf=1
		this.sourceformurl= '/bonita/portal/resource/taskInstance/'+processName+'/'+processVersion+'/'+taskName+'/content/?id='+taskId;
		console.log("URL: ",this.sourceformurl);
		this.isshowcases=false;
		this.isshowform=true;
	};

	this.showcases = function () {
		this.isshowcases=true;
		this.isshowform=false;
	}
	
	// --------------------------------------------------------------------------
	//
	//   Modal page management
	//
	// --------------------------------------------------------------------------
	this.dialogShown=false;
	
	this.showModal = function () {
		this.dialogShown = !this.dialogShown;

	}
	
	// --------------------------------------------------------------------------
	//
	//   Case page management
	//
	// --------------------------------------------------------------------------
	this.casepagenumber=1;
	this.filtercase = { };
	this.caseitemsperpage=10;
	// this function is call at each display : on a filter, or on a order for example
	this.getCasesPage = function()
	{
		// console.log("getCasesPage : start "+this.listcases);
		var begin = ((this.casepagenumber - 1) * this.caseitemsperpage);
		var end = begin + this.caseitemsperpage;
		this.listcases = $filter('orderBy')(this.listcases, this.orderByField, this.reverseSort);

		var listcasesfiltered = $filter('filter') (this.listcases, this.filtercase );
		// console.log('Filter filter='+ angular.toJson(this.filtercase,true ) +' Order='+  this.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(listcasesfiltered));
		return listcasesfiltered.slice(begin, end);
	}



	this.orderByField='';
	this.reverseSort=true;
	this.setorder= function( paramorderfield, paramreversesort)
	{
		this.orderByField = paramorderfield;
		this.reverseSort = paramreversesort;
		console.log("Order : ["+this.orderByField+"] order="+this.reverseSort);
	}

	// --------------------------------------------------------------------------
	//
	//   WellList page management
	//
	// --------------------------------------------------------------------------
	this.updatesdedetails = {};
	this.updatesdedetails.search={};
	this.searchWellList = function() {
		var self = this;
		this.welllistmessage="Processing...";
		this.welllisterrormessage="";

		this.updatesdedetails.search.resultalist = true;
		var json= angular.toJson(this.updatesdedetails.search, false);
	
		$http.get( '?page=custompage_SDEdashboard&action=getWellList&json='+json )
			.then( function ( jsonResult ) {		
						self.welllistmessage= jsonResult.data.DETAILS;
						self.listwelllist = jsonResult.data.RESULT;
						
					},
					function ( jsonResult ) {
								self.welllisterrormessage	= "Error during get"+jsonResult.status;
								self.message= "";
								alert("Error during the request "+angular.toJson(jsonResult));
							});
	}
	
	
	this.createWellList = function( onewell ) {
		var self = this;
		this.welllistmessage="Processing...";
		this.welllisterrormessage="";
		
		var json= angular.toJson(onewell, false);
		var url = this.properties.updatesdeurl;
		var url = url.replace("{{UWI}}", onewell.UWI);
		var url = url.replace("{{WELL_FULL_NAME}}", onewell.WELL_FULL_NAME);
		var url = url.replace("{{BUSINESS_UNIT}}", onewell.BUSINESS_UNIT);
		var url = url.replace("{{FIELD_NAME}}", onewell.FIELD_NAME);
		console.log("Call URL UpdateSdeDetail : "+url);
		$http.get( url )
			.then( function ( jsonResult ) {		
						self.welllistmessage= "Success";
						// self.welllisterrormessage= jsonResult.data.ERRORSTATUS;
					},
					function ( jsonResult ) {
								self.welllisterrormessage	= "Error during get"+jsonResult.status;
								self.message= "";
								alert("Error during the request "+angular.toJson(jsonResult));
							});
	}
	
	this.listwelllist=[];
	this.welllistpagenumber=1;
	this.filterwelllist = {};
	this.welllistitemsperpage=10;
	this.welllistorderByField="uwi";
	this.welllistreverseSort=true;
	// this function is call at each display : on a filter, or on a order for example
	this.getWellListPage = function()
	{
		console.log("getWellList : start "+this.listwelllist);
		if (this.listwelllist==null)
		{
			this.listwelllist=[];
			return;
		}
		var begin = ((this.welllistpagenumber - 1) * this.welllistitemsperpage);
		var end = begin + this.welllistitemsperpage;
		this.listwelllist = $filter('orderBy')(this.listwelllist, this.welllistorderByField, this.welllistorderByField);

		var listwelllistfiltered = $filter('filter') (this.listwelllist, this.filterwelllist );
		console.log('Filter filter='+ angular.toJson( listwelllistfiltered,true ) +' Order='+  this.welllistorderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(this.filterwelllist));
		
		return listwelllistfiltered.slice(begin, end);
	}

	// --------------------------------------------------------------------------
	//
	//   SystemSummary
	//
	// --------------------------------------------------------------------------

	this.systemSummary={};
	
	this.systemSummary.search = {};
	this.systemSummary.filter = {};
	this.systemSummary.listsde=[];
	this.systemSummary.pagenumber=1;
	this.systemSummary.sdeperpage=50;
	this.systemSummary.orderByField="SDE_NUMBER";
	this.systemSummary.reversesort=true;
	this.searchSystemSummary = function(  )
	{
		var self=this;
		var json= angular.toJson(this.systemSummary.search, false);
		console.log("Call Server to get list of cases");
		$http.get( '?page=custompage_SDEdashboard&action=getListSystemSummary&json='+json )
				.then( function ( jsonResult ) {
					// console.log('searchSystemSummary: Sucess is '+angular.toJson(jsonResult.data ));
					self.systemSummary.listsde 				= jsonResult.data.RESULT;
					// console.log('searchSystemSummary: result is '+angular.toJson(self.systemSummary.listsde ));

				}, function( jsonResult) {
					alert('an error occure during retrieve all cases');
					console.log('error receive' , e);
				});

	};
	
	// this function is call at each display : on a filter, or on a order for example
	this.getSystemSummaryPage = function()
	{
		// console.log("---- getSystemSummaryPage : start list["+angular.toJson(this.systemSummary.listsde )+"]");
		if (this.systemSummary.listsde==null)
		{
			this.systemSummary.listsde=[];
			return this.systemSummary.listsde;
		}
		if (this.systemSummary.listsde.length ==0)
		{
			return this.systemSummary.listsde;
		}		
		var begin = ((this.systemSummary.pagenumber - 1) * this.systemSummary.sdeperpage);
		var end = begin + this.systemSummary.sdeperpage;
		this.systemSummary.listsde = $filter('orderBy')(this.systemSummary.listsde, this.systemSummary.orderByField, this.systemSummary.reversesort);
		// console.log('-- getSystemSummaryPage orderBy='+ angular.toJson( this.systemSummary.listsde,true ));
		
		var listfiltered = $filter('filter') (this.systemSummary.listsde, this.systemSummary.filter );
		console.log('-- getSystemSummaryPage filter='+angular.toJson(this.systemSummary.filter)+' listfiltered='+ angular.toJson( listfiltered,true ) +' Order='+  this.systemSummary.listsde.listsde + ' reservesort='+this.systemSummary.reversesort);
		
		return listfiltered.slice(begin, end);
	}

	// --------------------------------------------------------------------------
	//
	//   PA Dashboard
	//
	// --------------------------------------------------------------------------
	this.padashboard = {};
	this.padashboard.search ={};
	this.padashboard.sdeperpage = 50;
	this.padashboard.pagenumber = 1;
	this.padashboard.orderByField="";
	this.padashboard.reversesort= false;
	this.padashboard.listdata = [];
	
	this.searchPADashboard = function() {
		var self=this;
		var json= angular.toJson(this.padashboard.search, false);
		console.log("Call URL PADashboard : "+url);
		$http.get( '?page=custompage_SDEdashboard&action=getPADashboard&json='+json )
			.then( function ( jsonResult ) {
						self.padashboard.listdata = jsonResult.data.LISTPADASHBOARD	
						self.padashboard.message= jsonResult.data.MESSAGE;
						self.padashboard.errormessage= jsonResult.data.ERRORMESSAGE;
					},
					function ( jsonResult ) {
						self.padashboard.errormessage	= "Error during get"+jsonResult.status;
						self.padashboard.message= "";
						alert("Error during the request "+angular.toJson(jsonResult));
					});
	};
	
	
	this.getPADashboardPage = function() {
	// console.log("---- getSystemSummaryPage : start list["+angular.toJson(this.systemSummary.listsde )+"]");
		if (this.padashboard.listdata==null)
		{
			this.padashboard.listdata=[];
			return this.padashboard.listdata;
		}
		if (this.padashboard.listdata.length ==0)
		{
			return this.padashboard.listdata;
		}		
		var begin = ((this.padashboard.pagenumber - 1) * this.padashboard.sdeperpage);
		var end = begin + this.padashboard.sdeperpage;
		this.padashboard.listdata = $filter('orderBy')(this.padashboard.listdata, this.padashboard.orderByField, this.padashboard.reversesort);
		// console.log('-- getSystemSummaryPage orderBy='+ angular.toJson( this.systemSummary.listsde,true ));
		
		var listfiltered = $filter('filter') (this.padashboard.listdata, this.padashboard.filter );
		console.log('-- getPage filter='+angular.toJson(this.padashboard.filter)+' listfiltered='+ angular.toJson( listfiltered,true ) +' Order='+  this.padashboard.listdata + ' reservesort='+this.padashboard.reversesort);
		
		return listfiltered.slice(begin, end);
		
	};
	this.checkPALine = function( oneSynthesis, checkBoxName) {
		if (oneSynthesis[ checkBoxName ]==true) {
			// uncheck the another checkbox
			oneSynthesis["HOLD_CLARIFICATION"] = false;
			oneSynthesis["DO_NOT_LOAD"] = false;
			oneSynthesis["UPDATE_EC"] = false;
			oneSynthesis[ checkBoxName ] = true;
			
		}
		
	}
	this.submitPADashboard = function() {
	};
	
	// --------------------------------------------------------------------------
	//
	//   properties management
	//
	// --------------------------------------------------------------------------
	this.properties ={};
	this.properties.updatesdeurl="";
	this.setproperties = function() 
	{
		this.propertiesmessage="Processing...";
		var json= angular.toJson(this.properties, false);

		var self=this;
		$http.get( '?page=custompage_SDEdashboard&action=setproperties&json='+json )
				.then( function ( jsonResult ) {
					// console.log('searchSystemSummary: Sucess is '+angular.toJson(jsonResult.data ));
					self.propertiesmessage			= jsonResult.data.status;

				}, function( jsonResult) {
					alert('an error occure during set properties');
					console.log('error receive' , jsonResult.status);
					self.propertiesmessage="Error "+ jsonResult.status;
				});
	};
	
	// --------------------------------------------------------------------------
	//
	//   show pages
	//
	// --------------------------------------------------------------------------

	this.isshowcases=true;
	this.isshowform=false;
	this.isshowSystemSummary=false;
	this.isshowUpdateSdeDetails=false;
	this.isshowProperties=false;
	this.isAllowPropertiesView=false;
	this.isshowPADashboard = false;
	
	this.resetView = function()
	{
		this.isshowcases=false;
		this.isshowform=false;
		this.isshowSystemSummary=false;
		this.isshowUpdateSdeDetails=false;
		this.isshowdetailcontributor=false;
		this.isshowProperties=false;
		this.isshowPADashboard=false;
	}
	this.showViewForm = function() {
		this.resetView();
		this.isshowcases= true;
		this.refreshcases();
	};
	this.showViewUpdateSde = function() {
		this.resetView();
		this.isshowUpdateSdeDetails= true;
		
	};
	this.showViewPADashboard  = function() {
		this.resetView();
		this.isshowPADashboard= true;
		
	};
	this.showViewSystemSummary = function() {
		this.resetView();
		this.isshowSystemSummary= true;
	};
	this.showProperties = function() {
		this.resetView();
		this.isshowProperties= true;
	};
	this.allowPropertiesView = function() {
		return this.isAllowPropertiesView;
	}

	this.getButtonClass = function (isCurrent ) {
		if (isCurrent) 
			return "btn btn-primary";
		return "btn btn-success";
	}
});



})();