//Getting window's height and width
var w = window,
d = document,
e = d.documentElement,
g = d.getElementsByTagName('body')[0],
x = w.innerWidth || e.clientWidth || g.clientWidth,
y = w.innerHeight|| e.clientHeight|| g.clientHeight;
var color = d3.scale.category10(),
	margin = 20,
	topButtonsMargin = $("#button").height(),
	gifSize = 100,
	width = 2*x/3-2*margin,
	height = y-(topButtonsMargin  + 2*margin),
	radius = Math.min(width, height) / 2 - 10;
	
$("#waitChartTables")
.css("position", "absolute")
.css("left", ((2*x/3-gifSize)/2) + "px")
.css("top", (y-gifSize - topButtonsMargin)/2 + "px");
$("#tables")
.css("position", "absolute")
.css("left", (2*x/3 + margin) + "px")
.css("top", topButtonsMargin + "px")
.css("width", (x/3 - 2*margin)+ "px");
$("#chartTables")
.css("position", "absolute")
.css("left", (2*x/3 - 2*radius)/2 + "px")
.css("top", ((y - topButtonsMargin - 2*radius)/2 + topButtonsMargin) + "px")
.css("width", 2*radius+ "px")
.css("height", 2*radius + "px");

/**
 * Error handling for server different than Wildfly
 * @param status
 
function errorManager(status) {
	switch(status){
	case 1001:
		error("Status " + status + ". Can't get Hadoop data, possible issues : <br>- The HADOOP_CONF environment to core-site.xml is not correct or not set <br>- The file core-site.xml is not properly formated <br>- Hadoop is not running", "error");
		break;
	case 1002:
		error("Status " + status + ". There is an issue with Hbase : <br>- The HBASE_CONF environment to hbase-site.xml is not correct or not set <br>- The file hbase-site.xml is not properly formated <br>- Hbase is not running", "error");
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
			error("Can not find " + array_item[i].name + " at " + array_item[i].location, "warning");
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
		error(c_error.responseText.slice(45,-14).replace(/&amp;lt;/g, '<').replace(/&amp;gt;/g, '>'), "error");
	else
		error("An unkown error code has been used " + status + "<br>" + c_error.responseText.slice(45,-14), "error");
}


(function(d3) {
	'use strict';

	function formatBytes(bytes,decimals) {
		if(bytes == 0) return '0 o';
		var k = 1000;
		var dm = decimals + 1 || 3;
		var sizes = ['o', 'Ko', 'Mo', 'Go', 'To', 'Po', 'Eo', 'Zo', 'Yo'];
		var i = Math.floor(Math.log(bytes) / Math.log(k));
		return (bytes / Math.pow(k, i)).toPrecision(dm) + ' ' + sizes[i];
	}

	function httpGetAsync(theUrl, callback1, callback2) {
		$("#chartTables").empty();
		$("#waitChartTables").show();
		var xmlHttp = new XMLHttpRequest();
		xmlHttp.onreadystatechange = function() { 
			if (xmlHttp.readyState == 4 && xmlHttp.status == 200){
				$("#waitChartTables").hide();
				callback1(xmlHttp.responseText);
				callback2(xmlHttp.responseText);
			} else if(xmlHttp.readyState == 4 && xmlHttp.status != 200) {
				errorManager(xmlHttp.status);
			}
		}
		xmlHttp.open("GET", theUrl, true); // true for asynchronous which we want
		xmlHttp.send(null);
	}

	function drawPie(data){
		var json = JSON.parse(data);
		var dataset = json.tbls.sort(function(a,b){
			return parseInt(b.size) - parseInt(a.size);
		});
		dataset = errorManagerJSON(dataset);

		var svg = d3.select('#chartTables')
		.append('svg')
		.attr('width', 2*radius)
		.attr('height', 2*radius)
		.append('g')
		.attr('transform', 'translate(' + radius + 
				',' + radius + ')');

		var arc = d3.svg.arc()
		.outerRadius(radius);

		var pie = d3.layout.pie()
		.value(function(d) { return d.size; })
		.sort(null);

		var tip = d3.tip()
		.attr('class', 'd3-tip')
		.offset([y/6, 0])
		.html(function(d) {
			return d.data.name + "<br>" + "<span style='color:orangered'>" + formatBytes(d.data.size, 2) + "</span>";
		});

		svg.call(tip);

		var path = svg.selectAll('path')
		.data(pie(dataset))
		.enter()
		.append('path')
		.attr('d', arc)
		.attr('fill', function(d, i) { 
			return color(d.data.name);
		})
		.on("mouseover", tip.show)
		.on("mouseout", tip.hide);;
	}

	function fillTable(data){
		var json = JSON.parse(data);
		json.tbls.sort(function(a, b) {
		    return parseInt(b.size) - parseInt(a.size);
		});
		for(var i = 0; i<json.tbls.length; i++){
			if(!parseInt(json.tbls[i].isOk)) continue;
			var col = color(json.tbls[i].name);
			$("#databases_tbody").append("<tr class='db-info db-onclick' id='"+json.tbls[i].name+"'><td class='lalign' style='color: " + col + ";'>" + json.tbls[i].name + "</td><td style='color: " + col + ";'>" + json.tbls[i].location + "</td> <td style='color: " + col + ";'> " + formatBytes(json.tbls[i].size,2) + "</td>");
		}
	}

	httpGetAsync("/HDFSV/HBaseTables", drawPie, fillTable);

  })(window.d3);