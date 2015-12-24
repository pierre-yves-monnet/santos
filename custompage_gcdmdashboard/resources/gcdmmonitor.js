'use strict';
/**
 *
 */

(function() {


var appCommand = angular.module('gcdmmonitor', ['googlechart', 'ui.bootstrap','ngModal', 'ngSanitize']);






// --------------------------------------------------------------------------
//
// Controler GCDM
//
// --------------------------------------------------------------------------

// Ping the server
appCommand.controller('GcdmControler',
	function ( $http, $scope, $filter,$sce ) {
	
	
	this.ismanager=false;
	
	this.casemaxresult = 100;
	
	this.sourceformurl="";
	self.message="";
	self.errormessage="";
	
	this.isinprogress = false;
	this.isshowGasCompositionDefault=true;
	this.isshowPressure=false;
	
	this.isshowform = false;
	
	this.isEditprofile = false;
	
	this.resetview = function() {
		this.isshowGasCompositionDefault=false;
		this.isshowPressure =false;
	};


	// --------------------------------------------------------------------------
	//
	// Generic function
	//
	// --------------------------------------------------------------------------


	this.setOrder= function( dataoperation, paramorderfield, paramreversesort)
	{
		dataoperation.orderByField = paramorderfield;
		dataoperation.reverseSort = paramreversesort;
		console.log("Order : ["+dataoperation.orderByField+"] order="+dataoperation.reverseSort);
	}
		
			
		
		
	this.getListValue = function( dataoperation )
	{
		var begin = (( dataoperation.casepagenumber - 1) * dataoperation.caseitemsperpage);
		var end = begin + dataoperation.caseitemsperpage;
		
		dataoperation.listValues  = $filter('orderBy')(dataoperation.listValues , dataoperation.orderByField, dataoperation.reverseSort);
		// console.log('Filter filter='+ angular.toJson(this.gasComposition.filtercase,true ) +' Order='+  this.gasComposition.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(this.gasComposition.listValues ));
		var listfiltered = $filter('filter') (dataoperation.listValues , dataoperation.filtercase );
		// console.log('Filter filter='+ angular.toJson(this.gasComposition.filtercase,true ) +' Order='+  this.gasComposition.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(listcasesfiltered));
		if (listfiltered==null)
			return null;
		return listfiltered.slice(begin, end);
	}
	
	
	this.getNewFields = function( dataoperation) {
		return dataoperation.listFields;
	};
	this.getHistoryFields = function (dataoperation, id) {
		return dataoperation.historyvalues[ id ];
	}
	
	this.closeDialog = function ( dataoperation ) {
		if (dataoperation.isChange==true) {
			 var r = confirm("You made some change without save them. Do you want to exit ? ");
			 if (r == true) {
				dataoperation.showModal = false;
			}
		}
		else {
			dataoperation.showModal = false;
		}
	}
		
	this.checkRange = function ( header,dataoperation ) {
		dataoperation.isChange=true;
		var message="";
		if (header.minrange != null && dataoperation.values[ header.id ] < header.minrange) {
			message="Out of range : must be more than "+header.minrange+";";
		}
		if (header.maxrange != null && dataoperation.values[ header.id ] > header.maxrange) {
			message="Out of range : must be less than "+header.maxrange+";";
		}
		console.log("CheckRange : "+message+" range"+header.minrange+" < "+header.maxrange+" : "+dataoperation.values[ header.id ] );

		dataoperation.values[ header.id +"_ERROR" ] = message;

	}
	
	this.getListEvents = function ( listevents ) {
		return $sce.trustAsHtml(  listevents);
	}
	
	this.isEditProfile = function () {
		return this.isEditprofile; 
	}
	// --------------------------------------------------------------------------
	//
	// Gaz composition
	//
	// --------------------------------------------------------------------------

	
	this.gasComposition ={};
	
	this.gasComposition.AddMessageDate="";
	this.gasComposition.showModal=false;
	this.gasComposition.listValues =[];
	this.gasComposition.listHeaders=[];
	this.gasComposition.ListSuplychain=[];
	this.gasComposition.orderByField='';
	this.gasComposition.reverseSort=true;
	this.gasComposition.values={};
	this.gasComposition.search = {};
	this.gasComposition.listFields=[];
	
	this.showGasComposition = function( typeOfGasComposition )
	{
		this.gasCompositionLegend = "Gas Composition "+typeOfGasComposition;
		this.gasComposition.type = typeOfGasComposition;
		this.resetview();
		this.isshowGasCompositionDefault=true;
		var self=this;
		var viewFutur = this.gasComposition.viewFuturDatedDefaults;
		if (viewFutur==null)
			viewFutur=false;
		var param = { "viewFuturDatedDefaults": viewFutur, "type" : typeOfGasComposition };
		var json = angular.toJson(param, false);
		$http.get( '?page=custompage_gcdmdashboard&action=showgascomposition&json='+json )
				.then( function ( jsonResult ) {

					self.gasComposition.listValues  		= jsonResult.data.LISTVALUES;
					self.gasComposition.message				= jsonResult.data.MESSAGE;
					self.gasComposition.errorMessage		= jsonResult.data.ERRORMESSAGE;
					self.gasComposition.ListSuplychain   	= jsonResult.data.LISTSUPPLYCHAIN;
					self.gasComposition.listHeaders			= jsonResult.data.LISTHEADERS;
					self.gasComposition.listFields			= jsonResult.data.NEWGASCOMPOSITIONFIELDS;
					self.isEditprofile						= jsonResult.data.ISEDITPROFILE;
					self.setOrder( self.gasComposition, 'SUPPLYCHAINEPOINT', false);
					console.log('get all default '+angular.toJson(self.listshowdefault ));
				},
				function(jsonResult) {
					alert("Can't connect the server "+jsonResult.status);
				});
	}
	
	this.showGasComposition( "Defaults" );
	
	
	
	
	this.gasCompositionCloseDialog = function () {
		if (this.gasComposition.isChange==true) {
			alert("You made some change without save them");
		}
		else {
			this.gasComposition.showModal = false;
		}
	}
	
	this.gasCompositionAdd = function () {
		this.gasComposition.showModal = true;
		this.gasComposition.values ={};
		this.gasComposition.isChange=false;
		}
	
	
	this.getGasCompositionListSupplychain = function() {
		return this.gasComposition.ListSuplychain;
	}

	// get the list of Columns header
	this.gasCompositionHeader=[];
	this.getGasCompositionColsHeader = function () {
		return this.gasComposition.listHeaders;
	}
	
	this.gasComposition.casepagenumber=1;
	this.gasComposition.caseitemsperpage = 1000;
	this.gasComposition.filtercase = { };
	
	
	this.gasCompositionEdit = function ( ) {
		var listToEdit=[];
		for (var i=0;i<this.gasComposition.listValues .length;i++) {
			console.log("Checked "+this.gasComposition.listValues [ i ].linechecked+" uid="+this.gasComposition.listValues [ i ].UID);
			if (this.gasComposition.listValues [ i ].linechecked)
				listToEdit.push( this.gasComposition.listValues [ i ].UID );
		}
		if (listToEdit.length==0) {
			alert("No line to edit; check one line");
			return;
		}
		if (listToEdit.length>1) {
			alert("Too much lines to edit; check only one line");
			return;
		}
		
		this.gasComposition.showModal = true;
		this.gasComposition.isChange=false;
		var param = { "UID" : listToEdit[ 0 ],"type" : this.gasComposition.type};
		var json = angular.toJson(param, false);
		var self=this;
		console.log("gasCompositionEdit: "+json)
		$http.get( '?page=custompage_gcdmdashboard&action=editgascomposition&json='+json )
		.then( function ( jsonResult ) {

			self.gasComposition.values  			= jsonResult.data.VALUES;
			self.gasComposition.historyvalues		= jsonResult.data.HISTORYVALUES;
			self.gasComposition.modalMessage		= jsonResult.data.MESSAGE;
			self.gasComposition.modalErrorMessage	= jsonResult.data.ERRORMESSAGE;
			self.gasComposition.modalListEvents		= jsonResult.data.LISTEVENTS;
		},
		function(jsonResult) {
			alert("Can't connect the server "+jsonResult.status);
		});
	}
	
	
	// click on the delete button
	this.gasCompositionDelete = function() {
		console.log("gasCompositionDelete");
		var listToDelete=[];
		for (var i=0;i<this.gasComposition.listValues .length;i++) {
			console.log("Checked "+this.gasComposition.listValues [ i ].linechecked+" uid="+this.gasComposition.listValues [ i ].UID);
			if (this.gasComposition.listValues [ i ].linechecked)
				listToDelete.push( this.gasComposition.listValues [ i ].UID );
		}
		if (listToDelete.length==0) {
			alert("No line to delete; check some lines");
			return;
		}
		
		console.log("listtodelete="+listToDelete);
		if (confirm("Are you sure to delete ? ")) {
	        // todo code for deletion
	    
		var post = {};
		post.listtodelete = listToDelete;
		var json = angular.toJson(post, false);
		var self=this;
		$http.get( '?page=custompage_gcdmdashboard&action=deletegascomposition&json='+json )
				.then( function ( jsonResult ) {

					self.gasComposition.listValues  	= jsonResult.data.LISTVALUES;
					self.gasComposition.message			= jsonResult.data.MESSAGE;
					self.gasComposition.errorMessage	= jsonResult.data.ERRORMESSAGE;
					self.gasComposition.listeventst		= jsonResult.data.LISTEVENTS;
					
				},
				function(jsonResult) {
					alert('an error occure during retrieve default '+jsonResult.status);
				});
		}
	};
	
	// --------------------------------------------------------------------------
	// Dialog modal GCDM
	this.getNewGasCompositionFields = function() {
		
		return this.gasComposition.listFields;
	};
	
	this.checkRangeGasComposition = function ( header ) {
		this.gasComposition.isChange=true;
		var message="";
		if (header.minrange != null && this.gasComposition.values[ header.id ] < header.minrange) {
			message="Out of range : must be more than "+header.minrange+";";
		}
		if (header.maxrange != null && this.gasComposition.values[ header.id ] > header.maxrange) {
			message="Out of range : must be less than "+header.maxrange+";";
		}
		console.log("CheckRange : "+message+" range"+header.minrange+" < "+header.maxrange+" : "+this.gasComposition.values[ header.id ] );

		this.gasComposition.values[ header.id +"_ERROR" ] = message;

	}
	
	this.searchNewGasComposition = function()	{
		this.gasComposition.isChange=false;
		var dateSt=this.formatDate( this.gasComposition.search.EFFECTIVEDATE, this.gasComposition.search.EFFECTIVETIME);
		this.gasComposition.search.EFFECTIVEDATE_ST = dateSt;
		var json = angular.toJson(this.gasComposition.search, false);
		var self=this;
		$http.get( '?page=custompage_gcdmdashboard&action=searchnewgascomposition&json='+json )
				.then( function ( jsonResult ) {

					self.gasComposition.values = jsonResult.data.NEWGASCOMPOSITIONVALUES;
					self.gasComposition.modalMessage = jsonResult.data.MESSAGE;
					self.gasComposition.modalErrorMessage = jsonResult.data.ERRORMESSAGE;
					self.gasComposition.modalListEvents	= jsonResult.data.LISTEVENTS;
				},
				function(jsonResult) {
					alert('an error occure during retrieve default '+jsonResult.status);
				});
		
	}
	
	this.saveNewGasComposition = function()	{
		this.gasComposition.listeventst	= "";
		
		var dateSt=this.formatDate( this.gasComposition.search.EFFECTIVEDATE, this.gasComposition.search.EFFECTIVETIME);
		// filter is part of the data to save !
		this.gasComposition.values.EFFECTIVEDATE_ST = dateSt;
		this.gasComposition.values.SUPPLYCHAINPOINT = this.gasComposition.search.SUPPLYCHAINPOINT;
		var json = angular.toJson(this.gasComposition.values, false);
		var self=this;
		$http.get( '?page=custompage_gcdmdashboard&action=savenewgascomposition&json='+json )
				.then( function ( jsonResult ) {

					self.gasComposition.values 		= jsonResult.data.NEWGASCOMPOSITIONVALUES;
					self.gasComposition.modalMessage 		= jsonResult.data.MESSAGE;
					self.gasComposition.modalErrorMessage = jsonResult.data.ERRORMESSAGE;
					self.gasComposition.modalListEvents	= jsonResult.data.LISTEVENTS;
					if (self.gasComposition.modalErrorMessage == null)
						this.gasComposition.isChange=false;
				},
				function(jsonResult) {
					alert('an error occure during retrieve default '+jsonResult.status);
				});
		
	}
	// --------------------------------------------------------------------------
	// GasCompositionSearch GCDM
		
	
		
	this.newGasCompositionSave = function()	{
		this.gasComposition.isChange=false;
	}

	
	this.newGasCompositionDelete = function()	{
		// calculate the list of element to delete
		
		 if (confirm("Are you sure to delete ? ")) {
		        // todo code for deletion
		    }

	}
	
	

	this.showprocessform = function( processName, processVersion, processId) {
		this.formtitle = "Create Case "+processName;
		this.sourceformurl= '/bonita/portal/homepage?ui=form&locale=en&tenant=1#form=' + processName + '--' + processVersion + '$entry&process=' + processId + '&autoInstantiate=false&mode=form';
		console.log("URL: ",this.sourceformurl);
		this.isshowcases=false;
		this.isshowform=true;
	};


	this.showcustompage = function( customName, customId ) {
		this.formtitle = "Action ";
		this.sourceformurl= '/bonita/portal/custompage?page=' + customId + '&locale=en&profile=4';
		// this.sourceformurl= '/bonita/portal/custompage?page=custompage_accountsearch&locale=en&profile=4';

		console.log("URL: ",this.sourceformurl);
		this.isshowcases=false;
		this.isshowform=true;
	};

	// --------------------------------------------------------------------------
	//
	// Presure form
	//
	// --------------------------------------------------------------------------
	this.pressure={};
	this.pressure.param={};
	this.pressure.listValues=[];
	this.pressure.shownModal = false;
	this.pressure.values=[];
	this.showPressure = function()
	{
		var self=this;
		var json = angular.toJson(this.pressure.param, false);
		
		$http.get( '?page=custompage_gcdmdashboard&action=showpressure&json='+json )
				.then( function ( jsonResult ) {

					self.pressure.listValues  		= jsonResult.data.LISTVALUES;
					self.pressure.message			= jsonResult.data.MESSAGE;
					self.pressure.errorMessage		= jsonResult.data.ERRORMESSAGE;
					self.pressure.listHeaders		= jsonResult.data.LISTHEADERS;
					self.pressure.listFields		= jsonResult.data.NEWGASCOMPOSITIONFIELDS;
					self.setOrder( self.pressure, 'SUPPLYCHAINEPOINT', false);
					
				},
				function(jsonResult) {
					alert("Can't connect the server "+jsonResult.status);
				});
	}
	this.pressure.casepagenumber=1;
	this.pressure.caseitemsperpage=1000;

	this.pressure.filtercase = { };

	this.showReportPressure = function () {
		this.resetview();
		this.isshowPressure = true;
		this.showPressure();
	}
	
	
	this.getPressureColsHeader = function () {
		return this.pressure.listHeaders;
	}
	this.setPressureOrder= function( paramorderfield, paramreversesort)
	{
		this.pressure.orderByField = paramorderfield;
		this.pressure.reverseSort = paramreversesort;
		console.log("Order : ["+this.pressure.orderByField+"] order="+this.pressure.reverseSort);
	}
	
	this.getPressureValue = function()
	{
		var begin = ((this.pressure.casepagenumber - 1) * this.pressure.caseitemsperpage);
		var end = begin + this.pressure.caseitemsperpage;
		
		this.pressure.listValues  = $filter('orderBy')(this.pressure.listValues , this.pressure.orderByField, this.pressure.reverseSort);
		// console.log('Filter filter='+ angular.toJson(this.pressure.filtercase,true ) +' Order='+  this.gasComposition.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(this.gasComposition.listValues ));
		var listpressurefiltered = $filter('filter') (this.pressure.listValues , this.pressure.filtercase );
		// console.log('Filter filter='+ angular.toJson(this.pressure.filtercase,true ) +' Order='+  this.gasComposition.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(listcasesfiltered));
		if (listpressurefiltered==null)
			return null;
		return listpressurefiltered.slice(begin, end);
	}
	
	// --------------------------------------------------------------------------
	// Modal Pressure
	
	
	this.pressureAdd = function () {
		console.log("pressure.showModal");
		this.pressure.showModal = true;
		this.pressure.isChange=false;
	
		}
	
	
	this.saveNewPressure = function(  )	{
		this.pressure.listeventst	= "";
		
		// var dateSt=this.formatDate( this.newGasComposition.search.EFFECTIVEDATE, this.newGasComposition.search.EFFECTIVETIME);
		// filter is part of the data to save !
		// this.newGasComposition.values.EFFECTIVEDATE_ST = dateSt;
		// this.newGasComposition.values.SUPPLYCHAINPOINT = this.newGasComposition.search.SUPPLYCHAINPOINT;
		var json = angular.toJson(this.pressure.values, false);
		var self=this;
		$http.get( '?page=custompage_gcdmdashboard&action=savenewpressure&json='+json )
				.then( function ( jsonResult ) {

					self.pressure.values 		= jsonResult.data.NEWGASCOMPOSITIONVALUES;
					self.pressure.newMessage 		= jsonResult.data.MESSAGE;
					self.pressure.newErrorMessage = jsonResult.data.ERRORMESSAGE;
					self.pressure.newListeventst	= jsonResult.data.LISTEVENTS;
					if (self.pressure.newErrorMessage == null)
						this.pressure.isChange=false;
				},
				function(jsonResult) {
					alert('an error occure during retrieve default '+jsonResult.status);
				});
		
	}
// --------------------------------------------------------------------------
//
// Task form
//
// --------------------------------------------------------------------------

	this.showtaskform = function( processName, processVersion, taskName, caseId, taskId) {
		this.formtitle = "Tasks "+caseId;
		this.sourceformurl= '/bonita/portal/homepage?ui=form&locale=en&tenant=1#form=' + processName + '--' + processVersion + '--' + taskName +'$entry&task=' + taskId + '&mode=form&assignTask=true'
		console.log("URL: ",this.sourceformurl);
		this.isshowcases=false;
		this.isshowform=true;
	};


// --------------------------------------------------------------------------
//
// Controler GCDM
//
// --------------------------------------------------------------------------

	this.assignToUser = function()
	{
		this.assignstatuserror="";
		this.assignstatus="AA";
		if (this.assigneeuser =="")
		{
			alert("Please select a user");
			this.assignstatuserror="Please select a user";
			return;
		}


		var listcasetoassign=[];
		for (var i=0; i<this.listcases.length; i++)
		{
			var onecase = this.listcases[ i ];
			if (onecase.casechecked)
			{
				var jsoncase = { "taskId" : onecase.taskId,
				                 "caseId" : onecase.caseId };
				listcasetoassign.push( jsoncase );
			}
		};
		if (listcasetoassign.length==0)
		{
			this.assignstatuserror="Check one case minimum";
			return;
		}
		var postMsg = {
					"assigneeuser": this.assigneeuser,
					"listcasetoassign": listcasetoassign,
					"displayalltasks": this.displayalltasks,
			};
		var json= angular.toJson(postMsg, true);

		var self=this;
		$http.get( '?page=custompage_gcdmdashboard&action=assignuser&paramjson='+json )
				.success( function ( jsonResult ) {
						console.log("assignuser",jsonResult);
						self.assignstatus		= jsonResult.status;
						self.assignstatuserror 	= jsonResult.error;
						self.listcases 			= jsonResult.listcases;
						self.listheader			= jsonResult.listheader;

				})
				.error( function() {
					alert('an error occure');
					});
	}
	

	this.checkDateByTheDay = function ( dateUser ) {
		// console.log("checkDateByTheDay "+dateUser);
		this.errorMessage="";
		var now = new Date();
		if (dateUser !== null && dateUser < now) {
			console.log("checkDateByTheDay ERROR "+dateUser);	
			this.errorMessage="Date must be in the future";
			
		}
		
	}
	this.formatDate = function(dateValue, timeValue){
          // var dateOut = new Date(date);
          var dateOut =  $filter('date')(new Date(dateValue),'dd-MM-yyyy');
          if (timeValue!=null) {
        	  var timeOut =  $filter('date')(new Date(timeValue),'HH:mm');
              dateOut += " "+timeOut;
        	  }
          return dateOut;
    };
	
	

});



})();