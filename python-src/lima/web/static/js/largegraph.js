/*
This file contains the scripts to control the large graph on the dashboard
*/

/*
This section gets all the data from all of the routers and creates the graph to display it
*/
$.getJSON("/get?id=allLargeData", function(routerData){
  var graph = new Rickshaw.Graph( {
    element: document.querySelector("#chart"),
    width: $('#largegraph').width(),
    height: 250,
    renderer: 'area',
    stroke: true,
    series: [ {
      data: routerData[1],
      color: '#FFCC00',
      name: 'Total TCP Flows'
    }, {
      data: routerData[2],
      color: '#FF9900',
      name: 'Total UDP Flows'
    } ]
  } );
  graph.renderer.unstack = true;

  //axes
  var xAxis = new Rickshaw.Graph.Axis.Time({
    graph: graph,
    timeFormat: function(x) {
      return x/1000;
    }
  });

  xAxis.render();

  //axes
  var y_axis = new Rickshaw.Graph.Axis.Y( {
    graph: graph,
    orientation: 'left',
    tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
    element: document.getElementById('y_axis'),
  } );
  graph.render();

  //re-render graph with window resize
  $(window).resize(function() {
    graph.configure({ width: $('#largegraph').width(), height: 250 }); 
    graph.render();
  });

  //hover details
  var hoverDetail = new Rickshaw.Graph.HoverDetail( {
    graph: graph,
    xFormatter: function(x) { var date = new Date(x); return date.toLocaleString();},
    yFormatter: function(y) { return y }
  } );

//time slider
  var slider = new Rickshaw.Graph.RangeSlider({
    graph: graph,
    element: document.querySelector('#slider')
  });
} );