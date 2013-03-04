  
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
  </div> \
  </div>';

/*
jQuery.ajax({
  url: '/get?id=threatData&key="+createTS+"+IP("+routerIP+")+"+createTS+"+"+createTS',
  type: 'GET',
  dataType: 'json',
  complete: function(xhr, textStatus) {
    //called when complete
  },
  success: function(data, textStatus, xhr) {
    evenData = data;
  },
  error: function(xhr, textStatus, errorThrown) {
    //called when there is an error
  }
});*/






  return sOut;
}

/*
function fnGetEventData( routerIP ) {
  $.getJSON("/get?id=eventData&router="+routerIP, function(data){
    console.log("EVENT DATA",data);
  }
}*/


function fnMakeGraph( eventID, largeData ) {
    console.log("#chart-"+eventID);
    var graph = new Rickshaw.Graph( {
      element: document.querySelector("#chart-"+eventID),
      width: $('#events').width(),
      height: 200,
      interpolation: 'basis',
      renderer: 'area',
      stroke: true,
      series: [ {
        data: largeData[1],
        color: 'lightblue'
      }, {
        data: largeData[2],
        color: 'steelblue'
      } ]
    } );
    graph.renderer.unstack = true;
    graph.render();

    $(window).resize(function() {
      graph.configure({ width: $('#events').width(), height: 200 }); 
      graph.render();
    });
}


$(document).ready(function() {
        //consider putting sse and other load stuff here

        var anOpen = [];
/*

$(document).on('mouseenter', '#routers td.control',  function(){
          var nTr = this.parentNode;
         var i = $.inArray( nTr, anOpen );
          //$('img', this).attr( 'src', sImageUrl+"details_close.png" );
          var oData = $('#routers').dataTable().fnGetData( nTr );
          var nDetailsRow = $('#routers').dataTable().fnOpen( nTr, fnFormatDetails($('#routers').dataTable(), nTr,oData[0]), 'details' );
          $('div.innerDetails', nDetailsRow).slideDown();
          anOpen.push( nTr );
          fnGetLargeData(oData[0]);
}).on('mouseleave', '#routers td.control', function() {
          var nTr = this.parentNode;
         var i = $.inArray( nTr, anOpen );

          //$('img', this).attr( 'src', sImageUrl+"details_open.png" );
          $('div.innerDetails', $(nTr).next()[0]).slideUp( function () {
           $('#routers').dataTable().fnClose( nTr );
           anOpen.splice( i, 1 );
          } );
}); */

  $(document).on("click", '#events td.control', function () {
   var nTr = this.parentNode;
   var i = $.inArray( nTr, anOpen );
     if ( i === -1 ) {
          //$('img', this).attr( 'src', sImageUrl+"details_close.png" );
          var oData = $('#events').dataTable().fnGetData( nTr );
          $.getJSON("/get?id=threatData&timestamp="+oData[7]+"&routerIP=IP("+oData[1]+")&type="+oData[2]+"&startTime="+oData[5]+"&endTime="+oData[6], function(threatData){
          //  $.getJSON("/get?id=largeDataForEvent&router="+oData[1]+"&startTime="+oData[5]+"&endTime="+oData[6], function(threatData){

              var nDetailsRow = $('#events').dataTable().fnOpen( nTr, fnFormatDetails(oData[0],threatData["f1:destIP"],threatData["f1:flowCount"],threatData["f1:flowDataAvg"],threatData["f1:flowDataTotal"],threatData["f1:srcIP"],threatData["f1:packetCount"]), 'details' );
              $('div.innerDetails', nDetailsRow).slideDown();
              anOpen.push( nTr );
              fnMakeGraph(oData[0],threatData["largeData"]);
           // });
          });
        }
        else {
          //$('img', this).attr( 'src', sImageUrl+"details_open.png" );
          $('div.innerDetails', $(nTr).next()[0]).slideUp( function () {
           $('#events').dataTable().fnClose( nTr );
           anOpen.splice( i, 1 );
         } );
        }
 });

});