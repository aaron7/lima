/*
This file contains the scripts to control extra event information and graphs on the Events page
*/

/*
This section creates the HTML for the extra event details - it parses its arguments into the HTML string
*/  
function fnFormatDetails( eventID, destIP, flowCount, flowDataAvg, flowDataTotal, srcIP, packetCount )
{

  sOut = 
  '<div class="innerDetails"> \
  <b>Destination IP:</b> '+destIP+' \
  <b>Source IP:</b> '+srcIP+' \
  <b>Flow Count:</b> '+flowCount+' \
  <b>Average Flow Data:</b> '+flowDataAvg+' \
  <b>Total Flow Data:</b> '+flowDataTotal+' \
  <b>Total Packet Count:</b> '+packetCount+' \
  <br /> \
  <div id="routerCountGraph"> \
  <div id="chart-'+eventID+'"></div> \
  <div id="slider-'+eventID+'"></div> \
  </div> \
  </div>';

  return sOut;
}

/*
This section creates the graph using Rickshaw and the data from its arguments
*/  
function fnMakeGraph( eventID, largeData ) {
  var graph = new Rickshaw.Graph( {
    element: document.querySelector("#chart-"+eventID),
    width: $('#events').width(),
    height: 150,
    /* interpolation: 'basis', */
    renderer: 'area',
    stroke: true,
    series: [ {
      data: largeData[1],
      color: '#FFCC00',
      name: 'Total TCP Flows'
    }, {
      data: largeData[2],
      color: '#FF9900',
      name: 'Total UDP Flows'
    } ]
  } );
  graph.renderer.unstack = true;

  //set up axes
  var x_axis = new Rickshaw.Graph.Axis.Time( { graph: graph } );

  var y_axis = new Rickshaw.Graph.Axis.Y( {
    graph: graph,
    orientation: 'left',
    tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
    element: document.getElementById('y_axis'),
  } );
  graph.render();

  //redraw graph when window resizes
  $(window).resize(function() {
    graph.configure({ width: ($('#events').width() - 50), height: 150 }); 
    graph.render();

      //redraw the time slider
      var slider = new Rickshaw.Graph.RangeSlider({
        graph: graph,
        element: document.querySelector('#slider-'+eventID)
      });

  });

  //hover detail for graph
  var hoverDetail = new Rickshaw.Graph.HoverDetail( {
    graph: graph,
    xFormatter: function(x) { var date = new Date(x); return date.toLocaleString();},
    yFormatter: function(y) { return y }
  } );

  //time slider for graph
  var slider = new Rickshaw.Graph.RangeSlider({
    graph: graph,
    element: document.querySelector('#slider-'+eventID)
  });

}

/*
This section handles the interactivity of the data table to display the extra infroamtion
*/  
$(document).ready(function() {
  var anOpen = [];

  //clicking on a row in the table shows the extra information
  $(document).on("click", '#events td.control', function () {
   var nTr = this.parentNode;
   var i = $.inArray( nTr, anOpen );
   if ( i === -1 ) {
          //closed, so get the data and open
          var oData = $('#events').dataTable().fnGetData( nTr );
          $.getJSON("/get?id=threatData&timestamp="+oData[7]+"&routerIP=IP("+oData[1]+")&type="+oData[2]+"&startTime="+oData[5]+"&endTime="+oData[6], function(threatData){
            var nDetailsRow = $('#events').dataTable().fnOpen( nTr, fnFormatDetails(oData[0],threatData["f1:destIP"],threatData["f1:flowCount"],threatData["f1:flowDataAvg"],threatData["f1:flowDataTotal"],threatData["f1:srcIP"],threatData["f1:packetCount"]), 'details' );
            $('div.innerDetails', nDetailsRow).slideDown();
            anOpen.push( nTr );
            fnMakeGraph(oData[0],threatData["largeData"]);
        });
        }
        else {
          //open, so close the extra details
          $('div.innerDetails', $(nTr).next()[0]).slideUp( function () {
           $('#events').dataTable().fnClose( nTr );
           anOpen.splice( i, 1 );
         } );
        }
      });
});