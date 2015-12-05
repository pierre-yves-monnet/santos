'use strict';
/**
 *
 */

(function() {


var appCommand = angular.module('gcdmmonitor', ['googlechart', 'ui.bootstrap','ngModal']);






// --------------------------------------------------------------------------
//
// Controler GCDM
//
// --------------------------------------------------------------------------

// Ping the server
appCommand.controller('GcdmControler',
	function ( $http, $scope, $filter ) {
	
	
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
	this.gasComposition.ListValues=[];
	this.gasComposition.listHeaders=[];
	this.gasComposition.ListSuplychain=[];
	this.gasComposition.NewValueGasComposition={};
	this.gasComposition.listAddFields=[];
	
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

					self.gasComposition.ListValues 			= jsonResult.data.LISTVALUES;
					self.gasComposition.Message				= jsonResult.data.MESSAGE;
					self.gasComposition.ErrorMessage		= jsonResult.data.ERRORMESSAGE;
					self.gasComposition.ListSuplychain   	= jsonResult.data.LISTSUPPLYCHAIN;
					self.gasComposition.listHeaders			= jsonResult.data.LISTHEADERS;
					self.gasComposition.listAddFields		= jsonResult.data.LISTADDFIELDS;
					self.setorder( 'SUPPLYCHAINEPOINT', false);
					console.log('get all default '+angular.toJson(self.listshowdefault ));
				},
				function(jsonResult) {
					alert('an error occure during retrieve default '+jsonResult.status);
				});
	}
	
	this.showGasComposition( "Defaults" );
	
	this.gasCompositionAdd = function () {
		this.gasCompositionDialogShown = true;
		}
	
	this.getGasCompositionListSuplychain = function() {
		return this.gasComposition.ListSuplychain;
		}

	this.gasCompositionHeader=[];
	this.getGasCompositionHeader = function () {
		return this.gasComposition.listHeaders;
	}
	this.getGasCompositionAddFields = function() {
		return this.gasComposition.listAddFields;
	};
	
	this.checkRangeGasComposition = function ( header ) {
		var message="";
		if (header.minrange != null && this.gasComposition.NewValueGasComposition[ header.id ] < header.minrange) {
			message="Out of range : must be more than "+header.minrange+";";
		}
		if (header.maxrange != null && this.gasComposition.NewValueGasComposition[ header.id ] > header.maxrange) {
			message="Out of range : must be less than "+header.maxrange+";";
		}
		console.log("CheckRange : "+message+" range"+header.minrange+" < "+header.maxrange+" : "+this.gasComposition.NewValueGasComposition[ header.id ] );

		this.gasComposition.NewValueGasComposition[ header.id +"_ERROR" ] = message;

	}
		
	this.casepagenumber=1;
	this.filtercase = { };
	this.caseitemsperpage=10;
	// this function is call at each display : on a filter, or on a order for example
	this.getGasCompositionValue = function()
	{
		var begin = ((this.casepagenumber - 1) * this.caseitemsperpage);
		var end = begin + this.caseitemsperpage;
		
		this.gasComposition.ListValues = $filter('orderBy')(this.gasComposition.ListValues, this.orderByField, this.reverseSort);
		console.log('Filter filter='+ angular.toJson(this.filtercase,true ) +' Order='+  this.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(this.gasComposition.ListValues));
		var listgascompositionfiltered = $filter('filter') (this.gasComposition.ListValues, this.filtercase );
		// console.log('Filter filter='+ angular.toJson(this.filtercase,true ) +' Order='+  this.orderByField + ' reservesort='+this.reverseSort+' listcasesfiltered='+angular.toJson(listcasesfiltered));
		return listgascompositionfiltered.slice(begin, end);
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

	
	

});



})();