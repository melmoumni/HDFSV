/**
 *  Create the HDFS view at /HDFSV/Start
 *  Overall view that should be created by this script: 
 *  ________________________________________
 * | <nav buttons>		|					|
 * |					|					|
 * |					|	   overall		|
 * |					|	 disk usage		|
 * |					|					|
 * |					|					|
 * |   the whole tree	|					|
 * |	  cluser		|___________________|
 * |	visualized		|					|
 * |  using sunburst	|					|
 * |					|		path		|
 * |					|	corresponding	|
 * |					|	to the child	|
 * |					|	 of current		|
 * |  <minSize range>	|	 directory		|
 * |____________________|___________________|
 * 
 */

(function() {
	"use strict";

	var w = window,
	d = document,
	e = d.documentElement,
	g = d.getElementsByTagName('body')[0],
	x = w.innerWidth || e.clientWidth || g.clientWidth,
	y = w.innerHeight|| e.clientHeight|| g.clientHeight;

	x=x/2;							//put svg in the left side of the screen

	var numberOfLayers = 7;			//number of layers including the center

	var margin = {top: y/30, right: x/30, bottom: y/30, left: x/30 }, //border margin of the window and radius of the suburst layers
	radius = Math.min(y - 2*margin.top, x - 2*margin.right, y - 2*margin.bottom, x - 2*margin.left)/(2*numberOfLayers);

	var min_degree_arc_filter = 2;	//filter svg arc element smaller than 2degrees
	var hue = d3.scale.category10();//color set

	function formatBytes(bytes,decimals) { //used to format the size of files/folders to become human readable
		if(bytes == 0) return '0 o';
		var k = 1000;
		var dm = decimals + 1 || 3;
		var sizes = ['o', 'Ko', 'Mo', 'Go', 'To', 'Po', 'Eo', 'Zo', 'Yo'];
		var i = Math.floor(Math.log(bytes) / Math.log(k));
		return (bytes / Math.pow(k, i)).toPrecision(dm)+ ' ' + sizes[i];
	}

	function getSearchParameters() {	  	//get the parameters passed through url
		var prmstr = window.location.search.substr(1);
		return prmstr != null && prmstr != "" ? transformToAssocArray(prmstr)
				: {};
	}

	function transformToAssocArray(prmstr) { //map the url parameters
		var params = {};
		var prmarr = prmstr.split("&");
		for (var i = 0; i < prmarr.length; i++) {
			var tmparr = prmarr[i].split("=");
			params[tmparr[0]] = tmparr[1];
		}
		return params;
	}

	var params = getSearchParameters();

	var rvalue = 6;			//default search value used to compute the threshold of the size of files displayed
	if (params.minSize)		//is url parameter minSize is set then update the threshold value				
		rvalue = params.minSize;

	//the following lines are just UI update as a function of the rvalue parameter
	$("#refresh").append(" (> " + f(rvalue) + ")");
	$("#refresh").attr("href", "/HDFSV/HadoopView?minSize=" + rvalue);
	
	$("#range")
	.val(rvalue)
	.on("change", function(){
		$("#refresh")
		.html("Refresh view (> " + f($(this).val()) + ")")
		.attr("href", "/HDFSV/HadoopView?minSize=" + $("#range").val())
		.attr("title", "Only files and directory with a size greater than " + f($(this).val()) + " are going to be included. Those smaller will be stored under the 'other' label"); 
	});
	$("#rangevalue").html(f(rvalue));
	//end of ui update

	var luminance = d3.scale.sqrt() 
	.domain([0, 1e6])
	.clamp(true)
	.range([90, 20]);

	// generate a tooltip when mouseover the arc
	var tip = d3.tip()
	.attr('class', 'd3-tip')
	.direction("e")
	.offset(function() {
		return [0, 5]
	})
	.html(function(d) {
		return d.name + "<br>" + "<span style='color:orangered'>" + formatBytes(d.value, 2) + "</span>";
	});

	/**
	 * Error handler using the code status returned by the servlet
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
		var regex = "'<body>(.*?)</body></html>'si";
		var matches = c_error.responseText.match(/<body>(.*?)<\/body>/);
		if(matches) {
			error(matches[1], "error");
		} else {
			error("An unknown error occured <br>error status " + status, "error");
		}
	}

	// creating the svg element that wrapp the sunburst, and placing it at the correct place 
	
	/**
	 * Here we are creating the left part of the screen: 
	 *  _____________________ 
	 * |<nav buttons>		| ...
	 * |					| ...
	 * |					| ...
	 * |					| ...
	 * |					| ...
	 * |					| ...
	 * |   the whole tree	| ...
	 * |	  cluster		| ...
	 * |	visualized		| ...
	 * |  using sunburst	| ...
	 * |					| ...
	 * |					| ...
	 * |					| ...
	 * |					| ...
	 * |  <minSize range>	| ...
	 * |____________________| ...
	 */
	var svg = d3.select("body").append("svg")
	.attr("width", (numberOfLayers*2*radius) + "px")
	.attr("height", (numberOfLayers*2*radius) + "px")
	.style("position", "absolute")
	.style("top",margin.top + "px")
	.style("left",margin.left + "px")
	.append("g")
	.attr("transform", "translate(" + (numberOfLayers*radius) + "," + (numberOfLayers*radius) + ")");

	svg.call(tip);

	var partition = d3.layout.partition()
	.sort(function(a, b) { 
		if(a.sum > b.sum)
			return -1;
		return 1;
	})
	.size([2 * Math.PI, radius]);

	var arc = d3.svg.arc()
	.startAngle(function(d) { return d.x; })
	.endAngle(function(d) { return d.x + d.dx ; })
	.padAngle(0.05) 
	.padRadius(radius/3)
	.innerRadius(function(d) { return radius * d.depth; })
	.outerRadius(function(d) { return radius * (d.depth + 1) - 1; });

	var explore = $('#infos').css("height");

	// get time to know how long the servlet takes time to send the response
	var start = new Date().getTime();
	// send a request to the HDFSContent servlet
	// if the HDFSContent has some issue, you may fallback to using the less optimized FileContent servlet
	d3.json("/HDFSV/HadoopData?minSize=" + $("#range").val(), function(error, root) {
		$("#wait").hide();
		if(error) {
			errorManager(error.status, error);
		} else {
			// compute and display the time it took for the servlet to answer the request
			var end = new Date().getTime();
			$("#time").text("hdfs fetched in " + (end-start)/1000 + "s");		
		}
		$("#path").html('<span class="path_element" style="background-color: #cccccc">' +root.name+'</span>');

		$("#infos")
		.css("position", "absolute")
		.css("left", (x + margin.left) + "px")
		.css("top", (parseInt($("#path").css("top"), 10) + parseInt($("#path").height(),10) + 15 ) + "px")
		.css("z-index", 10)
		.css("width", (x - 2*margin.left) + "px")
		.css("height", (numberOfLayers*radius - 2*margin.top) + "px");
		// Compute the initial layout on the entire tree to sum sizes.
		// Also compute the full name and fill color for each node,
		// and stash the children so they can be restored as we descend.
		partition
		.value(function(d) { return d.size; })
		.nodes(root)
		.forEach(function(d) {
			d._children = d.children;
			d.sum = d.value;
			d.key = key(d);
			d.fill = fill(d);
		});
		// Now redefine the value function to use the previously-computed sum.
		partition
		.children(function(d, depth) { return depth < numberOfLayers-1 ? d._children : null; })
		.value(function(d) { return d.sum; });

		var center = svg.append("circle")
		.attr("r", radius)
		.on("click", zoomOut);

		center.append("title").text("zoom out");

		$("#center").text(formatBytes(root.value, 2));

		var children_sorted_ = root.children.sort(function(a,b){
			if(a.value > b.value)
				return -1;
			else if (a.value < b.value)
				return 1;
			else 
				return 0;
		});

		var children_sorted = [];
		var nb_undefined = 0;
		for(var i = 0, len = children_sorted_.length; i < len; i++){
			var child = children_sorted_[i];
			if(typeof child.name === "undefined")
				nb_undefined++;
			var col = fill(child).toString();
			var folder_icon = '';
			if(typeof child.children != "undefined")
				folder_icon = '<i class="fa fa-folder"></i>';
			$("#infos").append("<div class='node'>"+folder_icon+"<figure class='circle' style='background: " + col + "'></figure><span class='info' style='color: " + col + "'> " + child.name + "</span><span class='right' style='color: white'> " + formatBytes(child.value,2) + "</span></div><div style='clear:both;'></div>");
			children_sorted[i] = child;
		}

		for(var i = 0, len = children_sorted.length; i < len; i++){
			var $thisDiv = $("#infos").children().eq(i*2);
			$thisDiv.click(function(event){
				var idx = $(event.currentTarget).index();
				//because there is hidden div that break the float: right
				idx = idx/2;
				for(var j = 0; j < root.children.length; j++) {
					if(root.children[j].name === children_sorted[idx].name) {
						idx = j;
						break;
					}
				}
				zoomIn(root.children[idx]);
			})
			.on("mouseout",function(event){
				var child = getPathTargetByEvent(event, root);
				var tar = document.getElementById(key(child));
				$(tar).css("opacity", 0.9).css("fill", fill(child));
			})
			.on("mouseover",function(event){
				var child = getPathTargetByEvent(event, root);
				var tar = document.getElementById(key(child));
				var col = d3.rgb($(tar).css("fill")).brighter();
				$(tar).css("opacity", 0.9).css("fill", col.toString());
			});
		}

		var path = svg.selectAll("path")
		.data(partition.nodes(root).slice(1))
		.enter().append("path")
		.filter(function(d) { return (d.dx > min_degree_arc_filter *(Math.PI)/180); })
		.attr("d", arc)
		.attr("id", function(d) {return key(d); })
		.style("fill", function(d) { return fill(d); })
		.on('mouseover', tip.show)
		.on('mouseout', tip.hide)
		.each(function(d) { this._current = updateArc(d); })
		.on("click", zoomIn);

		function zoomIn(p) {
			if (p.depth > 1) p = p.parent;
			if (!p.children) return;
			zoom(p, p);
		}

		function zoomOut(p) {
			if(typeof p === "undefined") return;
			if (!p.parent) return;
			zoom(p.parent, p);
		}

		var end2 = new Date().getTime();
		$("#time").append(" | visualization in " + ((end2-end)/1000) + "s");
		// Zoom to the specified new root.
		function zoom(root, p) {
			if (document.documentElement.__transition__) return;
			// Rescale outside angles to match the new layout.
			var enterArc,
			exitArc,
			outsideAngle = d3.scale.linear().domain([0, 2 * Math.PI]);

			function insideArc(d) {
				return p.key > d.key
				? {depth: d.depth - 1, x: 0, dx: 0} : p.key < d.key
						? {depth: d.depth - 1, x: 2 * Math.PI, dx: 0}
				: {depth: 0, x: 0, dx: 2 * Math.PI};
			}


			function outsideArc(d) {
				return {depth: d.depth + 1, x: outsideAngle(d.x), dx: outsideAngle(d.x + d.dx) - outsideAngle(d.x)};
			}

			center.datum(root);

			var children_sorted = root.children.sort(function(a,b){
				if(a.value > b.value)
					return -1;
				else if (a.value < b.value)
					return 1;
				else 
					return 0;
			}).slice();

			$("#infos").empty();
			if(typeof root.parent != "undefined") {
				$("#infos").append("<div class='node' id='prevFolder'><span class='info' style='color: #fff'>..</span></div>");
				$("#prevFolder").click(function(event){
					zoomIn(root.parent);
				});
			}

			// When zooming in, arcs enter from the outside and exit to the inside.
			// Entering outside arcs start from the old layout.
			if (root === p) enterArc = outsideArc, exitArc = insideArc, outsideAngle.range([p.x, p.x + p.dx]);

			path = path.data(partition.nodes(root).slice(1), function(d) { return d.key; });

			// When zooming out, arcs enter from the inside and exit to the outside.
			// Exiting outside arcs transition to the new layout.
			if (root !== p) enterArc = insideArc, exitArc = outsideArc, outsideAngle.range([p.x, p.x + p.dx]);

			d3.transition().duration(750).each(function() {
				path.exit().transition()
				.style("fill-opacity", function(d) { return d.depth === 1 + (root === p) ? 1 : 0; })
				.attr("id", function(d) {return key(d); })
				.attrTween("d", function(d) { return arcTween.call(this, exitArc(d)); })
				.remove();

				path.enter()
				.append("path")
				.style("fill-opacity", function(d) { return d.depth === 2 - (root === p) ? 1 : 0; })
				.style("fill", function(d) { return d.fill; })
				.attr("id", function(d) {return key(d); })
				.on("click", zoomIn)
				.on('mouseover', tip.show)
				.on('mouseout', tip.hide)
				.each(function(d) { this._current = enterArc(d); });

				path.transition()
				.style("fill-opacity", 1)
				.style("fill", function(d) { return fill(d); })
				.style("display", function(d) { if(Math.abs(d.x - (d.x + d.dx)) > min_degree_arc_filter *(Math.PI)/180) return "inherit"; return "none"; })
				.attrTween("d", function(d) { return arcTween.call(this, updateArc(d)); });
			});

			var val = 0;
			for(var i = 0, len=children_sorted.length; i < len; i++){
				val += root.children[i].sum;
				var child = children_sorted[i];
				var col = fill(child).toString();
				var folder_icon = '';
				if(typeof child.children != "undefined")
					folder_icon = '<i class="fa fa-folder"></i>';
				$("#infos").append("<div class='node'>"+ folder_icon +" <figure class='circle' style='background: " + col + "'></figure><span class='info' style='color: "+ col +"'>" + child.name + "</span><span class='right' style='color: white'> " + formatBytes(child.value,2) + "</span></div><div style='clear:both;'></div>");
			}
			$("#center").text(formatBytes(val,2));

			$("#path").empty();
			var current_dir = root;
			var path_dir = '';

			while(current_dir.parent != null){
				var col = d3.select(current_dir)[0][0].fill.toString();
				path_dir += '<span class="path_element" style="background-color: ' + col + '">' + current_dir.name+'</span>/$#';
				current_dir = current_dir.parent;
			}
			path_dir += '<span class="path_element" style="background-color: #cccccc">' + current_dir.name+'</span>/$#';
			$("#path").html(path_dir.split("/$#").reverse().join(""));
			$("#infos").css("top", parseInt($("#path").css("top"), 10) + parseInt($("#path").height(),10) + 5);

			$(".path_element").click(function(){
				var current_elem = root;
				for(var i = 0, len=$(this).nextAll().length; i < len; i++)
					current_elem = current_elem.parent;
				zoomIn(current_elem);	
			});

			for(var i = 0, len=children_sorted.length; i < len; i++){
				var iterator = 2*i;
				if(typeof root.parent != "undefined")
					iterator = 2*i + 1;
				var $thisDiv = $("#infos").children().eq(iterator);
				$thisDiv.click(function(event){
					var child = getPathTargetByEvent(event, root);
					zoomIn(child);
				});
				$thisDiv.on("mouseover",function(event){
					var child = getPathTargetByEvent(event, root);
					var tar = document.getElementById(key(child));
					var col = d3.rgb($(tar).css("fill")).brighter();
					$(tar).css("opacity", 1).css("fill", col.toString());
				});
				$thisDiv.on("mouseout",function(event){
					var child = getPathTargetByEvent(event, root);
					var tar = document.getElementById(key(child));
					$(tar).css("opacity", 0.9).css("fill", fill(child));
				});
			}
		}
	});

	function key(d) {
		var k = [], p = d;
		while (typeof p.parent != 'undefined') k.push(p.name), p = p.parent;
		return k.reverse().join(".");
	}

	var colors = [];

	function makeColorGradient(frequency1, frequency2, frequency3, phase1, phase2, phase3, center, width, len) {
		if (center == undefined)   center = 128;
		if (width == undefined)    width = 127;
		if (len == undefined)      len = 50;

		for (var i = 0; i < len; ++i) {
			var red = Math.sin(frequency1*i + phase1) * width + center;
			var grn = Math.sin(frequency2*i + phase2) * width + center;
			var blu = Math.sin(frequency3*i + phase3) * width + center;
			colors[i] = {"r":red, "g": grn, "b": blu};
		}
	}

	makeColorGradient(.015,.015,.015,0,2,4, 170, 85, 360);

	function fill(d) {
		var angle_degree = Math.floor(d.x * (360/(2*Math.PI))) % 360;
		var index = angle_degree; //Math.floor(angle_degree * (colors.length - 1)/ 360);
		var c = d3.lab("rgb("+colors[index].r+"," +  colors[index].g + "," + colors[index].b + ")");
		c.l = luminance(300000 / d.depth);
		return c;
	}

	function getPathTargetByEvent(event, root) {
		var tokens = event.currentTarget.textContent.split(" ");
		var thatname = '';
		//stop at tokens.length - 2 because we concatenate the whole name except the size at the end of the line
		for(var k = 0; k < tokens.length - 2; k++)  {
			thatname += tokens[k];
			if(k!=tokens.length - 3 && k != 0)
				thatname += " ";
		}
		var idx = 0;
		for(var j = 0; j < root.children.length; j++) {
			if(root.children[j].name === thatname) {
				idx = j;
				break;
			}
		}
		return root.children[idx];
	}

	function arcTween(b) {
		var i = d3.interpolate(this._current, b);
		this._current = i(0);
		return function(t) {
			return arc(i(t));
		};
	}

	function updateArc(d) {
		return {depth: d.depth, x: d.x, dx: d.dx};
	}

	d3.select(self.frameElement).style("height", margin.top + margin.bottom + "px");
	
	/**
	 * Here we are creating the rights part of the screen: 
	 *     _________________
	 * ...|					|
	 * ...|					|
	 * ...|	   overall		|
	 * ...|	 disk usage		|
	 * ...|					|
	 * ...|					|
	 * ...|_________________|
	 * ...|					|
	 * ...|		path		|
	 * ...|	corresponding	|
	 * ...|	to the child	|
	 * ...|	 of current		|
	 * ...|	 directory		|
	 * ...|_________________|
	 * 
	 */


	$("#wait")
	.css("position", "absolute")
	.css("left", (numberOfLayers*radius+margin.left - 64) + "px")
	.css("top", (numberOfLayers*radius+margin.top - 64) + "px");

	$("#center")
	.css("position", "absolute")
	.css("left", (numberOfLayers*radius+margin.left - 28) + "px")
	.css("top", (numberOfLayers*radius+margin.top - 9) + "px")
	.css("z-index", 10);

	$("#error")
	.css("position", "absolute")
	.css("left", (numberOfLayers*radius+margin.left - 28) + "px")
	.css("top", (numberOfLayers*radius+margin.top - 9) + "px")
	.css("z-index", 10);

	$("#border")
	.css("left", x);

	var this_radius = Math.min((x/2-2*margin.left),(y/2-2*margin.top))/2;

	$("#datanodes")
	.css("position", "absolute")
	.css("top",  (margin.top/2) + "px")
	.css("left", (3*x/2 - margin.right) + "px")
	.css("width", (numberOfLayers*radius + 36) + "px")
	.css("height", (numberOfLayers*radius) + "px");

	$("#replication")
	.css("position", "absolute")
	.css("top",  (margin.top/2 + (numberOfLayers*radius -10)) + "px")
	.css("left", (3*x/2 - margin.right) + "px");

	$("#path")
	.css("left", (x + margin.left) + "px") 
	.css("top", (y/2 + margin.top/2) + "px")
	.css("height", "auto !important")
	.css("z-index", 10)
	.css("max-width", (x - 2*margin.left) + "px");

	$("#switch")
	.css("top",0)
	.css("left",0);

	$("#time")
	.css("top",y-20)
	.css("left",0);

	/**
	 * Now we are building the right part of the screen: 
	 */
	
	y = y/2; //now we divide the height by 2 and only work in the bottom or top part
	var this_radius = 0,
		marg = 0;
		
	if(x/2 - 2*margin.left > y-2*margin.top){
		this_radius = y - 2*margin.top;
		marg = margin.top;
	}
	else{
		this_radius = x/2 - 2*margin.left;
		marg = margin.left;
	}
	
	function httpGetAsync(theUrl, callback) {
		var xmlHttp = new XMLHttpRequest();
		xmlHttp.onreadystatechange = function() { 
			if (xmlHttp.readyState == 4 && xmlHttp.status == 200)
				callback(xmlHttp.responseText);
		}
		xmlHttp.open("GET", theUrl, true); // true for asynchronous which we want
		xmlHttp.send(null);
	}


	function drawDiskChart(json) {
		var obj = JSON.parse(json);
		var dataset = [{ label: 'Used space', count: obj.summary[0].used}, 
		               { label: 'Free space', count: obj.summary[0].unused}];
		//4 times smaller than sunburst (diameter is half of the one of the sunburst)

		var vis = d3.select("#chart")
		.append("svg:svg") //create the SVG element inside the <body>
		.data([dataset]) //associate our data with the document
		.attr("width", this_radius + "px") //set the width of the canvas
		.attr("height", this_radius + "px")//set the height of the canvas
		.style("position", "absolute")
		.style("top",  (margin.top) + "px")
		.style("left", (x + marg) + "px")
		.append("svg:g") //make a group to hold our pie chart
		.attr('transform', 'translate(' + (this_radius/2)+ ',' + (this_radius/2) + ')');

		var arc = d3.svg.arc()
		.outerRadius(this_radius/2);

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

		for(var i = 0; i < dataset.length; i++) {
			$("#infoSize").append("<div><figure class='circle' style='background: " + hue(i) + "'></figure><span class='info' style='color: "+ hue(i) +"'>" + dataset[i].label + " &nbsp&nbsp</span><span class='right' style='color: white'> " + formatBytes(dataset[i].count,2) + "</span></div><div style='clear:both;'></div>");
		}

		$("#infoSize").append("</br><div style='color:white;'>Replication factor : "+obj.replication+"</br>"+"Number of datanodes : "+(obj.summary.length-1)+"</div>")
		$("#infoSize")
		.css("position", "absolute")
		.css("left", (3*x/2 + margin.left) + "px")
		.css("top", ((y - $("#infoSize").height())/2) + "px")
		.css("z-index", 10);

		$("#details")
		.css("position", "absolute")
		.css("left", (3*x/2 + margin.left) + "px")
		.css("top", ((y - $("#infoSize").height())) + "px");
	}

	httpGetAsync("/HDFSV/NodesData", drawDiskChart)
})();