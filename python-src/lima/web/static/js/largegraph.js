$.getJSON("/get?id=allLargeData", function(routerData){
    console.log("LARGE DATA",routerData)
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

    console.log("hello2");

   var x_axis = new Rickshaw.Graph.Axis.Time( { graph: graph } );

    var y_axis = new Rickshaw.Graph.Axis.Y( {
        graph: graph,
        orientation: 'left',
        tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
        element: document.getElementById('y_axis'),
    } );
    console.log("hello3");

    graph.render();

    console.log("hello1");



$(window).resize(function() {
    graph.configure({ width: $('#largegraph').width(), height: 250 }); 
    graph.render();
});



var hoverDetail = new Rickshaw.Graph.HoverDetail( {
    graph: graph,
    xFormatter: function(x) { var date = new Date(x); return date.toLocaleString();},
    yFormatter: function(y) { return y }
} );

/*
var legend = new Rickshaw.Graph.Legend({
    graph: graph,
    element: document.querySelector('#legend'),
    height: '10px'
});


var shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
    graph: graph,
    legend: legend
});

*/
console.log("HERE1");

//leave last
    var slider = new Rickshaw.Graph.RangeSlider({
    graph: graph,
    element: document.querySelector('#slider')
    });

  } );