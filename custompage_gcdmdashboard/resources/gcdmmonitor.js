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
	this.isshowform = false;
	
	this.resetview = function() {
		this.isshowGasCompositionDefault=false;
	};
	
	this.gasComposition ={};
	
	this.gasComposition.AddMessageDate="";
	this.gasComposition.DialogShown=false;
	this.gasComposition.listValues =[];
	this.gasComposition.listHeaders=[];
	this.gasComposition.ListSuplychain=[];
	
	this.getListEvents = function () {
		return $sce.trustAsHtml( this.gasComposition.listeventst);
	}
	this.getListNewEvents = function () {
		return $sce.trustAsHtml( this.newGasComposition.listeventst);
	}
	
	this.newGasComposition ={};
	this.newGasComposition.values={};
	this.newGasComposition.search = {};
	this.newGasComposition.listFields=[];
	
	this.showGasComposition = function( typeOfGasComposition )
	{
		this.gasCompositionLegend = "Gas Composition "+typeOfGasComposition;
		this.resetview();
		this.isshowGasCompositionDefault=true;
		var self=this;
		var param = { "type" : typeOfGasComposition};
		var json = angular.toJson(param, false);
		$http.get( '?page=custompage_gcdmdashboard&action=showgascomposition&json='+json )
				.then( function ( jsonResult ) {

					self.gasComposition.listValues  		= jsonResult.data.LISTVALUES;
					self.gasComposition.message				= jsonResult.data.MESSAGE;
					self.gasComposition.errorMessage		= jsonResult.data.ERRORMESSAGE;
					self.gasComposition.ListSuplychain   	= jsonResult.data.LISTSUPPLYCHAIN;
					self.gasComposition.listHeaders			= jsonResult.data.LISTHEADERS;
					self.newGasComposition.listFields		= jsonResult.data.NEWGASCOMPOSITIONFIELDS;
					self.setorder( 'SUPPLYCHAINEPOINT', false);
					console.log('get all default '+angular.toJson(self.listshowdefault ));
				},
				function(jsonResult) {
					alert("Can't connect the server "+jsonResult.status);
				});
	}
	
	this.showGasComposition( "Defaults" );
	
	
	
	
	this.gasCompositionCloseDialog = function () {
		if (this.newGasComposition.isChange==true) {
			alert("You made some change without save them");
		}
		else {
			this.gasCompositionDialogShown = false;
		}
	}
	this.gasCompositionAdd = function () {
		this.gasCompositionDialogShown = true;
		this.newGasComposition.isChange=false;
		}
	
	
	this.getGasCompositionListSupplychain = function() {
		return this.gasComposition.ListSuplychain;
		}

	// get the list of Columns header
	this.gasCompositionHeader=[];
	this.getGasCompositionColsHeader = function () {
		return this.gasComposition.listHeaders;
	}
	
	this.casepagenumber=1;
	this.filtercase = { };
	this.caseitemsperpage=10;
	// this function is call at each display : on a filter, or on a order for example
	this.getGasCompositionValue = function()
	{
		var begin = ((this.casepagenumber - 1) * this.caseitemsperpage);
		var end = begin + this.caseitemsperpage;
		
		this.gasComposition.listValues  = $filter('orderBy')(this.gasComposition.listValues , this.orderByField, this.reverseSort);
		// console.log('Filter filter='+ angular.toJson(this.filtercase,true ) +' Order='+  this.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(this.gasComposition.listValues ));
		var listgascompositionfiltered = $filter('filter') (this.gasComposition.listValues , this.filtercase );
		// console.log('Filter filter='+ angular.toJson(this.filtercase,true ) +' Order='+  this.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(listcasesfiltered));
		if (listgascompositionfiltered==null)
			return null;
		return listgascompositionfiltered.slice(begin, end);
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
	    }
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
	};
	
	// --------------------------------------------------------------------------
	//
	// Dialog modal GCDM
	//
	// --------------------------------------------------------------------------
	this.getNewGasCompositionFields = function() {
		
		return this.newGasComposition.listFields;
	};
	
	this.checkRangeGasComposition = function ( header ) {
		this.newGasComposition.isChange=true;
		var message="";
		if (header.minrange != null && this.newGasComposition.values[ header.id ] < header.minrange) {
			message="Out of range : must be more than "+header.minrange+";";
		}
		if (header.maxrange != null && this.newGasComposition.values[ header.id ] > header.maxrange) {
			message="Out of range : must be less than "+header.maxrange+";";
		}
		console.log("CheckRange : "+message+" range"+header.minrange+" < "+header.maxrange+" : "+this.newGasComposition.values[ header.id ] );

		this.newGasComposition.values[ header.id +"_ERROR" ] = message;

	}
	
	this.searchNewGasComposition = function()	{
		this.newGasComposition.isChange=false;
		var dateSt=this.formatDate( this.newGasComposition.search.EFFECTIVEDATE, this.newGasComposition.search.EFFECTIVETIME);
		this.newGasComposition.search.EFFECTIVEDATE_ST = dateSt;
		var json = angular.toJson(this.newGasComposition.search, false);
		var self=this;
		$http.get( '?page=custompage_gcdmdashboard&action=searchnewgascomposition&json='+json )
				.then( function ( jsonResult ) {

					self.newGasComposition.values = jsonResult.data.NEWGASCOMPOSITIONVALUES;
					self.newGasComposition.message = jsonResult.data.MESSAGE;
					self.newGasComposition.errorMessage = jsonResult.data.ERRORMESSAGE;
					self.newGasComposition.listeventst	= jsonResult.data.LISTEVENTS;
				},
				function(jsonResult) {
					alert('an error occure during retrieve default '+jsonResult.status);
				});
		
	}
	
	this.saveNewGasComposition = function()	{
		this.newGasComposition.listeventst	= "";
		
		var dateSt=this.formatDate( this.newGasComposition.search.EFFECTIVEDATE, this.newGasComposition.search.EFFECTIVETIME);
		// filter is part of the data to save !
		this.newGasComposition.values.EFFECTIVEDATE_ST = dateSt;
		this.newGasComposition.values.SUPPLYCHAINPOINT = this.newGasComposition.search.SUPPLYCHAINPOINT;
		var json = angular.toJson(this.newGasComposition.values, false);
		var self=this;
		$http.get( '?page=custompage_gcdmdashboard&action=savenewgascomposition&json='+json )
				.then( function ( jsonResult ) {

					self.newGasComposition.values 		= jsonResult.data.NEWGASCOMPOSITIONVALUES;
					self.newGasComposition.message 		= jsonResult.data.MESSAGE;
					self.newGasComposition.errorMessage = jsonResult.data.ERRORMESSAGE;
					self.newGasComposition.listeventst	= jsonResult.data.LISTEVENTS;
					if (self.newGasComposition.errorMessage == null)
						this.newGasComposition.isChange=false;
				},
				function(jsonResult) {
					alert('an error occure during retrieve default '+jsonResult.status);
				});
		
	}
	// --------------------------------------------------------------------------
	//
	// GasCompositionSearch GCDM
	//
	// --------------------------------------------------------------------------
		
	
		
	this.newGasCompositionSave = function()	{
		this.newGasComposition.isChange=false;
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
// Order by
//
// --------------------------------------------------------------------------

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