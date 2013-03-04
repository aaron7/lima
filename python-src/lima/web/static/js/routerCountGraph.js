/*
This file contains the scripts to control the objects on the dashboard page
*/

/*
This section creates the HTML string to show router information
*/
function fnFormatDetails(routerIP, numEvents)
{

  sOut = 
  '<div class="innerDetails"> \
  <b>Number of open events:</b> '+numEvents+' \
  <br /> \
  <div id="routerCountGraph"> \
  <div id="chart-'+routerIP.replace(/\./g,"-")+'"></div> \
  <div id="slider-'+routerIP.replace(/\./g,"-")+'"></div> \
  </div> \
  </div>';

  return sOut;
}

/*
This section creates the graph with the data from the server
*/

function fnMakeGraph( routerIP ) {
  $.getJSON("/get?id=largeData&router="+routerIP, function(routerData){
    var graph = new Rickshaw.Graph( {
      element: document.querySelector("#chart-"+routerIP.replace(/\./g,"-")),
      width: $('#routers').width(),
      height: 150,
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
    var x_axis = new Rickshaw.Graph.Axis.Time( { graph: graph } );
    //axes
    var y_axis = new Rickshaw.Graph.Axis.Y( {
      graph: graph,
      orientation: 'left',
      tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
      element: document.getElementById('y_axis'),
    } );
    graph.render();

    //re-render graph when window resize
    $(window).resize(function() {
      graph.configure({ width: ($('#routers').width()-50), height: 150 }); 
      graph.render();
        //redraw the time slider
        var slider = new Rickshaw.Graph.RangeSlider({
          graph: graph,
          element: document.querySelector('#slider-'+routerIP.replace(/\./g,"-"))
        });
    });

  //hover detail for graph
  var hoverDetail = new Rickshaw.Graph.HoverDetail( {
    graph: graph,
    xFormatter: function(x) { var date = new Date(x); return date.toLocaleString();},
    yFormatter: function(y) { return y }
  } );

  //time slider
  var slider = new Rickshaw.Graph.RangeSlider({
    graph: graph,
    element: document.querySelector('#slider-'+routerIP.replace(/\./g,"-"))
  });
} );
}

/*
This section handles the control of the extra router information
*/
$(document).ready(function() {
  var anOpen = [];
  //show router information on click
  $(document).on("click", '#routers td.control', function () {
   var nTr = this.parentNode;
   var i = $.inArray( nTr, anOpen );

   if ( i === -1 ) {
    //closed, so open and get info
    var oData = $('#routers').dataTable().fnGetData( nTr );
    $.getJSON("/get?id=eventData&router="+oData[0], function(eventData){
            var nDetailsRow = $('#routers').dataTable().fnOpen( nTr, fnFormatDetails(oData[0],eventData.length), 'details' );
            $('div.innerDetails', nDetailsRow).slideDown();
            anOpen.push( nTr );
            fnMakeGraph(oData[0]);
          });
  } else {
    //open, so close
          $('div.innerDetails', $(nTr).next()[0]).slideUp( function () {
            $('#routers').dataTable().fnClose( nTr );
            anOpen.splice( i, 1 );
          } );
        }
      } );
});