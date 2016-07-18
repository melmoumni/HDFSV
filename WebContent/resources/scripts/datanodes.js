/** Error handling for server different than wildfly:
 * 
 * function errorManager(status) {
	switch(status){
	case 1001:
		error("Status " + status + ". Can't get Hadoop data, possible solution : <br>- Set the HADOOP_CONF environment variable to the absolute path of your hadoop hive-site.xml in your ~/.bashrc and then source ~/.bashrc <br>- Check that the HADOOP_CONF is set to a correct path and that the file is properly formated.", "error");
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
}

**/

function errorManagerJSON(obj) {
	if(parseInt(obj.summary[0].used) == -1)
		error("While accessing global used space. Returned " + obj.summary[0].used, "warning");
	if(parseInt(obj.summary[0].unused) == -1)
		error("While accessing global unused space. Returned " + obj.summary[0].used, "warning");
	if(parseInt(obj.replication) == -1)
		error("While accessing global replication factor. Returned " + obj.replication, "warning");
	if(!parseInt(obj.isOk))
		error("Can not access global datanodes informations.", "error");
		
	for(var i = 1, len = obj.summary.length; i < len; i++) {
		if(!parseInt(obj.summary[i].isOk))
			error("Can not access node  " + obj.summary[i].name, "warning");
		if(parseInt(obj.summary[i].used) == -1)
			error("Can not used space in node " + obj.summary[i].name, "warning");
		if(parseInt(obj.summary[i].unused) == -1)
			error("Can not unused space in node " + obj.summary[i].name, "warning");
	}
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

function httpGetAsync(theUrl, callback) {
	var xmlHttp = new XMLHttpRequest();
	xmlHttp.onreadystatechange = function() { 
		if (xmlHttp.readyState == 4 && xmlHttp.status == 200)
			callback(xmlHttp.responseText);
		else if (xmlHttp.readyState == 4 && xmlHttp.status != 200)
			errorManager(xmlHttp.status, xmlHttp);
	}
	xmlHttp.open("GET", theUrl, true); // true for asynchronous which we want
	xmlHttp.send(null);
} 

function formatBytes(bytes,decimals) {
	if(bytes == 0) return '0 o';
	var k = 1000;
	var dm = decimals + 1 || 3;
	var sizes = ['o', 'Ko', 'Mo', 'Go', 'To', 'Po', 'Eo', 'Zo', 'Yo'];
	var i = Math.floor(Math.log(bytes) / Math.log(k));
	return (bytes / Math.pow(k, i)).toPrecision(dm) + ' ' + sizes[i];
}

var w = window,
	d = document,
	e = d.documentElement,
	g = d.getElementsByTagName('body')[0],
	x = w.innerWidth || e.clientWidth || g.clientWidth,
	y = w.innerHeight|| e.clientHeight|| g.clientHeight;

var margin = {top: y/10, right: x/10, bottom: y/10, left: x/10 }; 
var hue = d3.scale.category10();

var length = 0;
var radius = 0;
var wrapper_width = 0;

function displayDatanode(data, i) {
	var $wrapper = $('#datanodes').children().eq(i-1);
	$wrapper.children().eq(0).text(data.name);
	var dataset = [{ label: 'Used space', count: data.used, percentage: data.percentage}, 
	               { label: 'Free space', count: data.unused, percentage: (100-data.percentage)}
	];
	
	var vis = d3.select($wrapper[0])
	.append("svg:svg") //create the SVG element inside the <body>
	.data([dataset]) //associate our data with the document
	.attr("width", wrapper_width + "px") //set the width of the canvas
	.attr("height", wrapper_width + "px") //set the height of the canvas
	.append("svg:g") //make a group to hold our pie chart
	.attr('transform', 'translate(' + (wrapper_width/2)+ ',' + (wrapper_width/2) + ')');

	var arc = d3.svg.arc()
	.outerRadius(radius)
	.innerRadius(radius/2);

	var pie = d3.layout.pie() //this will create arc data for us given a list of values
	.value(function(d) { return d.count; }); // Binding each value to the pie

	var arcs = vis.selectAll("g.slice")
	.data(pie)
	.enter()
	.append("svg:g")
	.attr("class", "slice");    //allow us to style things in the slices (like text)

	arcs.append("svg:path")
	.attr("fill", function(d, i) { return hue(i); } )
	.attr("d", arc);
}

function createWrapper() {
	$datanodes = $("#datanodes");
	$datanodes.append("<div>");

	$last = $("#datanodes div:last-child").last();

	$last
	.css("width", wrapper_width)
	.css("height", wrapper_width + 15)
	.css("margin", margin.right/5)
	.css("display", "inline-block");

	$last.append("<div>");
	$last.children().eq(0).css("width", wrapper_width).css("height", 15).addClass("center");
}

httpGetAsync("/HDFSV/NodesData", function(json) {
	var obj = JSON.parse(json);
	errorManagerJSON(obj);
	$datanodes = $("#datanodes");
	$datanodes.css({"margin-top": margin.top,
					"margin-bottom": 0,
					"margin-left": margin.left,
					"margin-right": margin.right
				});
	
	var dataset = [{ label: 'Used space', count: obj.summary[0].used}, 
	               { label: 'Free space', count: obj.summary[0].unused}];
	
	for(var i = 0; i < dataset.length; i++) {
		$("#infoSize").append("<div><figure class='circle' style='background: " + hue(i) + "'></figure><span class='info' style='color: "+ hue(i) +"'>" + dataset[i].label + " &nbsp&nbsp</span><span class='right' style='color: white'> " + formatBytes(dataset[i].count,2) + "</span></div><div style='clear:both;'></div>");
	}
	
	$("#infoSize").append("</br><div style='color:white;'>Replication factor : "+obj.replication+"</br>"+"Number of datanodes : "+(obj.summary.length-1)+"</div>")
	$("#infoSize")
	.css("position", "absolute")
	.css("left", "75%")
	.css("top", "0px")
	.css("z-index", 10);
	
	
	length = obj.summary.length - 1;
	var size_min = 100,
		size_max = 500;
	var $width = $("#datanodes").width() - 2*margin.right;
	
	if($width/length > size_min && $width/length < size_max)
		wrapper_width = $width/length;
	else if($width/length < size_min)
		wrapper_width = size_min;
	else if($width/length > size_max)
		wrapper_width = size_max;
	
	radius = wrapper_width/2;
	
	for(var i = 0; i < length; i++) {
		createWrapper();
	}
	for(var i = 0; i < length; i++) {
		displayDatanode(obj.summary[i+1], i+1);
	}
});