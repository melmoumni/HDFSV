//Getting window's height and width
var w = window,
d = document,
e = d.documentElement,
g = d.getElementsByTagName('body')[0],
x = w.innerWidth || e.clientWidth || g.clientWidth,
y = w.innerHeight|| e.clientHeight|| g.clientHeight;



$("#waitChartDatabases")
.css("position", "absolute")
.css("left", (x/4 - 64 + 10) + "px")
.css("top", (y/4 - 64 + 10) + "px");

$("#waitChartTables")
.css("position", "absolute")
.css("left", (3*x/4 - 64 + 10) + "px")
.css("top", (y/4 - 64 + 10) + "px");

$("#databases")
.css("position", "absolute")
.css("left", (10) + "px")
.css("top", (y/2 + 10) + "px")
.css("width", (x/2 - 50)+ "px")
.css("height", (y/2 - 10) + "px");

$("#externals")
.css("position", "absolute")
.css("left", (x/2 + 10) + "px")
.css("top", (y/2 + 10) + "px")
.css("width", (x/2 - 50)+ "px")
.css("height", (y/4 - 10) + "px");


$("#internals")
.css("position", "absolute")
.css("left", (x/2 + 10) + "px")
.css("top", (y/2 + y/4 + 10) + "px")
.css("width", (x/2 - 50)+ "px")
.css("height", (y/4 - 10) + "px");


var color = d3.scale.category10();

/**
 * Error handling for servers different than Wildfly

function errorManager(status) {
	switch(status){
	case 1000:
		error("Status " + status + ". Can't get Hive data, possible solution : <br>- Set the HIVE_CONF environment variable to the absolute path of your hadoop hive-site.xml in your ~/.bashrc and then source ~/.bashrc <br>- Check that the HIVE_CONF is set to a correct path and that the file is properly formated.", "error");
		break;
	case 1001:
		error("Status " + status + ". Can't get Hadoop data, possible solution : <br>- Set the HADOOP_CONF environment variable to the absolute path of your hadoop hive-site.xml in your ~/.bashrc and then source ~/.bashrc <br>- Check that the HADOOP_CONF is set to a correct path and that the file is properly formated.", "error");
		break;
	case 1002:
		error("Status " + status + ". This should not be occuring. Maybe somthing wrong with a table, can't be accessd or smthg ? I don't really know", "warning");
		break;
	case 1003:
		error("Status " + status + ". Can't get hive or hadoop configuration file, check your HADOOP_CONF and HIVE_CONF environment variables to to see if the path their is correct", "error");
		break;
	default:
		error("An unknown error occured <br>error status " + status, "error");
		break;
	}
}

function error(message, level) {
	$("#error").append("<div class='message "+ level +"'>"+ level.toUpperCase() +": " + message +"</div>")
			   .show();

	$(".message:last").click(function() {$(this).fadeOut('fast'); });
	if(level === "error") {
		$("#waitChartDatabases").hide();
		$("#waitChartTables").hide();
		$("table").hide(); 
	}
}
**/

function errorManagerJSON(array_item) {
	var dset = [];
	for(var i = 0, len = array_item.length; i < len; i++) {
		if(! parseInt(array_item[i].isOk)) {
			error("Can not find " + array_item[i].label, "warning");
		} else 
			dset.push(array_item[i]);
	}
	return dset;
}

/**
 * Error handlig for Wildfly server :
 */
function error(message, level) {
	$("#error").append("<div class='message "+ level +"'>"+ level.toUpperCase() +": " + message +"</div>")
	.show();

	$(".message:last").click(function() {$(this).fadeOut('fast'); });
	if(level === "error") {
		$("#waitChartDatabases").hide();
		$("#waitChartTables").hide();
		$("table").hide(); 
	}
}

function errorManager(status, c_error) {
	if(status == 404)
		error(c_error.responseText.slice(45,-14), "error");
	else
		error("An unkown error code has been used " + status + "<br>" + c_error.responseText.slice(45,-14), "error");
}

(function(d3) {
	'use strict';

	//Function for asynchronous call
	function httpGetAsync(theUrl, callback1, callback2) {
		$("#chartTables").empty();
		$("#waitChartTables").show();
		var xmlHttp = new XMLHttpRequest();
		xmlHttp.onreadystatechange = function() { 
			if (xmlHttp.readyState == 4 && xmlHttp.status == 200){
				if(typeof callback2 != 'undefined') {
					json_g = JSON.parse(xmlHttp.responseText);
					callback1(xmlHttp.responseText, {"x": x/2-20, "y": y/2-20});
					callback2(xmlHttp.responseText);
				} else
					callback1(xmlHttp.responseText, {"x": x/2-20, "y": y/2-20});
			} else if(xmlHttp.readyState == 4 && xmlHttp.status != 200){
				errorManager(xmlHttp.status);
			}
		}
		xmlHttp.open("GET", theUrl, true); // true for asynchronous which we want
		xmlHttp.send(null);
	}

	//global variable containing the tables json for each database (so we don't have to send a request twice for the same database)
	var dbInfo = {};
	var hasCreatedDB = false;
	var firstLoad = true;
	var json_g;
	
	function formatBytes(bytes,decimals) {
		if(bytes == 0) return '0 o';
		var k = 1000;
		var dm = decimals + 1 || 3;
		var sizes = ['o', 'Ko', 'Mo', 'Go', 'To', 'Po', 'Eo', 'Zo', 'Yo'];
		var i = Math.floor(Math.log(bytes) / Math.log(k));
		return (bytes / Math.pow(k, i)).toPrecision(dm) + ' ' + sizes[i];
	}

	//Function to draw databases pie (top left)
	function drawDatabasesPie(json, size){
		json = JSON.parse(json);
		var data;
		var targetWaiter = "#waitChartDatabases";
		var target = "#chartDatabases";
		if(hasCreatedDB) {
			targetWaiter = "#waitChartTables";
			target = "#chartTables";
			data = json.tbls.sort(function(a,b){
				return parseInt(b.count) - parseInt(a.count);
			});
			$(target).empty();
		} else {
			hasCreatedDB = true;
			data = json.dbs.sort(function(a,b){
				return parseInt(b.count) - parseInt(a.count);
			});
		}

		data = errorManagerJSON(data);
		$(targetWaiter).hide();
		$(target)
		.css("width", width)
		.css("display", "inline-block");
		var width = size.x - 20;
		var height = size.y - 20;
		var radius = Math.min(width, height) / 2;

		var svg = d3.select(target)
		.append('svg')
		.attr('width', width + 10)
		.attr('height', height + 10)
		.append('g')
		.attr('transform', 'translate(' + (width / 2 + 10) + 
				',' + (height / 2 + 10) + ')');

		var arc = d3.svg.arc()
		.outerRadius(radius);

		var pie = d3.layout.pie()
		.value(function(d) { return d.count; })
		.sort(null);

		var tip = d3.tip()
		.attr('class', 'd3-tip')
		.offset([y/6, 0])
		.html(function(d) {
			return d.data.label + "<br>" + "<span style='color:orangered'>" + formatBytes(d.data.count, 2) + "</span>";
		});

		svg.call(tip);

		var path = svg.selectAll('path')
		.data(pie(data))
		.enter()
		.append('path')
		.attr('d', arc)
		.attr('fill', function(d, i) { 
			return color(d.data.label);
		})
		.on("mouseover", tip.show)
		.on("mouseout", tip.hide);
		
		if(firstLoad) {
			path.on("click", (function(e){
				var name = e.data.label;
				//No need to send a request
				if(typeof dbInfo[name] != 'undefined') {
					getDBInfoCallBack(dbInfo[name], {"x": x/2-20, "y": y/2-20});
				} // we need to send a request
				else {
					httpGetAsync("/HDFSV/HiveTables?database="+name, getDBInfoCallBack);				
				}
				$(".highlighted").removeClass("highlighted");
				$("#"+name).addClass("highlighted");
			}));
			
			json_g.dbs.sort(function(a, b) {
			    return parseInt(b.count) - parseInt(a.count);
			});
			httpGetAsync("/HDFSV/HiveTables?database="+json_g.dbs[0].label, getDBInfoCallBack);
			firstLoad = false;
		}
	}

	function getDBInfoCallBack(json, size) {
		var tmp = JSON.parse(json);
		dbInfo[tmp.database] = json;
		drawDatabasesPie(dbInfo[tmp.database], size);
		appendTablesInfo(tmp);
	}
	
	function appendTablesInfo(json) {
		$("#externals_tbody").empty();
		$("#internals_tbody").empty();
		$("#externals_thead").empty();
		$("#internals_thead").empty();
		$("#externals_thead").append("<tr><th></th><th>Externals for <span style='color:"+color(json.database)+"'>"+json.database+"</span></th><th></th></tr><tr><th><span>Name</span></th><th><span>Location /</span></th><th><span>Size</span></th></tr>");
		$("#internals_thead").append("<tr><th></th><th>Internals for <span style='color:"+color(json.database)+"'>"+json.database+"</span></th><th></th></tr><tr><th><span>Name</span></th><th><span>Location /</span></th><th><span>Size</span></th></tr>");
		var externals = [];
		var internals = [];
		for(var i = 0; i<json.tbls.length; i++){
			if(json.tbls[i].type == "EXTERNAL_TABLE"){
				externals.push(json.tbls[i]);
			}
			else if(json.tbls[i].type == "MANAGED_TABLE"){
				internals.push(json.tbls[i]);
			}
			
		}
		externals.sort(function(a, b) {
		    return parseInt(b.count) - parseInt(a.count);
		});
		internals.sort(function(a, b) {
		    return parseInt(b.count) - parseInt(a.count);
		});
		for(var i = 0; i<externals.length; i++){
			if(externals[i].type == "EXTERNAL_TABLE"){
				var col = color(externals[i].label);
				$("#externals_tbody").append("<tr class='db-info' id='"+externals[i].label+"'><td class='lalign' style='color: " + col + ";'>" + externals[i].label + "</td><td style='color: " + col + ";'>" + externals[i].location + "</td> <td style='color: " + col + ";'> " + formatBytes(externals[i].count,2) + "</td>");
			}
		}
		for(var i = 0; i<internals.length; i++){
			if(internals[i].type == "MANAGED_TABLE"){
				var col = color(internals[i].label);
				$("#internals_tbody").append("<tr class='db-info' id='"+internals[i].label+"'><td class='lalign' style='color: " + col + ";'>" + internals[i].label + "</td><td style='color: " + col + ";'>" + internals[i].location + "</td> <td style='color: " + col + ";'> " + formatBytes(internals[i].count,2) + "</td>");
			}
		}
	}
	
	function f2(json){
		json = JSON.parse(json);
		json.dbs.sort(function(a, b) {
		    return parseInt(b.count) - parseInt(a.count);
		});
		for(var i = 0; i<json.dbs.length; i++){
			var col = color(json.dbs[i].label);
			$("#databases_tbody").append("<tr class='db-info db-onclick' id='"+json.dbs[i].label+"'><td class='lalign' style='color: " + col + ";'>" + json.dbs[i].label + "</td><td style='color: " + col + ";'>" + json.dbs[i].location + "</td> <td style='color: " + col + ";'> " + formatBytes(json.dbs[i].count,2) + "</td>");
		}
		$("#"+json.dbs[0].label).addClass("highlighted");
		$(".db-onclick").click(function(e) {
			var name = e.currentTarget.id;
			//No need to send a request
			if(typeof dbInfo[name] != 'undefined') {
				getDBInfoCallBack(dbInfo[name], {"x": x/2-20, "y": y/2-20});
			} // we need to send a request
			else {
				httpGetAsync("/HDFSV/HiveTables?database="+name, getDBInfoCallBack);				
			}
			$(".highlighted").removeClass("highlighted");
			$("#"+name).addClass("highlighted");
		})
	}
	httpGetAsync("/HDFSV/HiveDatabases", drawDatabasesPie, f2);
})(window.d3);