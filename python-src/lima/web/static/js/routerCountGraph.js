function fnFormatDetails(routerIP, numEvents)
{

  sOut = 
  '<div class="innerDetails"> \
  <b>Number of open events:</b> '+numEvents+' \
  <br /> \
  <div id="routerCountGraph"> \
  <div id="chart-'+routerIP.replace(/\./g,"-")+'"></div> \
  </div> \
  </div>';



  return sOut;
}

/*
function fnGetEventData( routerIP ) {
  $.getJSON("/get?id=eventData&router="+routerIP, function(data){
    console.log("EVENT DATA",data);
  }
}*/


function fnMakeGraph( routerIP ) {
  $.getJSON("/get?id=largeData&router="+routerIP, function(routerData){
    console.log("LARGE DATA",routerData)
    console.log("#chart-"+routerIP.replace(/\./g,"-"));
    var graph = new Rickshaw.Graph( {
      element: document.querySelector("#chart-"+routerIP.replace(/\./g,"-")),
      width: $('#routers').width(),
      height: 200,
      interpolation: 'basis',
      renderer: 'area',
      stroke: true,
      series: [ {
        data: routerData[1],
        color: 'lightblue'
      }, {
        data: routerData[2],
        color: 'steelblue'
      } ]
    } );
    graph.renderer.unstack = true;
    graph.render();

    $(window).resize(function() {
      graph.configure({ width: $('#routers').width(), height: 200 }); 
      graph.render();
    });

  } );
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



  $(document).on("click", '#routers td.control', function () {
   var nTr = this.parentNode;
   var i = $.inArray( nTr, anOpen );

   if ( i === -1 ) {
          var oData = $('#routers').dataTable().fnGetData( nTr );
          $.getJSON("/get?id=eventData&router="+oData[0], function(eventData){
            //$('img', this).attr( 'src', sImageUrl+"details_close.png" );
            var nDetailsRow = $('#routers').dataTable().fnOpen( nTr, fnFormatDetails(oData[0],eventData.length), 'details' );
            $('div.innerDetails', nDetailsRow).slideDown();
            anOpen.push( nTr );
            fnMakeGraph(oData[0]);
          });


    } else {
          //$('img', this).attr( 'src', sImageUrl+"details_open.png" );
          $('div.innerDetails', $(nTr).next()[0]).slideUp( function () {
            $('#routers').dataTable().fnClose( nTr );
            anOpen.splice( i, 1 );
          } );
    }
  } );
});