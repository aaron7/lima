/*
This file contains the scripts to control the Routers Page
*/

/*
This section gets the routers and displays them
*/
function initial(){
  $.getJSON("/get?id=routers", function(data){
    globaldata = data;
//format timestamps
for (var i = 0; i < data.length; i++) {
  var date = new Date(data[i][1]);
  data[i][1] = date.toLocaleString();
  var date = new Date(data[i][5]);
  data[i][5] = date.toLocaleString();
}
$('#routers').dataTable( {
  "aaData": data,
  "bLengthChange": false,
  "bPaginate": false,
  "fnFooterCallback": function ( nRow, aaData, iStart, iEnd, aiDisplay ) {
            var numRouters = data.length;
            var iTotalFlows = 0;
            var iTotalPackets = 0;
            var iTotalBytes = 0;
            for ( var i=0 ; i<aaData.length ; i++ )
            {
                iTotalFlows += aaData[i][2]*1;
                iTotalPackets += aaData[i][3]*1;
                iTotalBytes += aaData[i][4]*1;
            }
            
            $("#avgFlows").text((Math.round(iTotalFlows/numRouters)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ","));
            $("#avgPackets").text((Math.round(iTotalPackets/numRouters)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ","));
            $("#avgBytes").text((Math.round(iTotalBytes/numRouters)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ","));
        },

  "aoColumns": [
  { "sTitle": "Router IP", "sClass": "control"},
  { "sTitle": "Last Seen", "sClass": "control" },
  { "sTitle": "Flows Per Hour", "sClass": "control",
      "mRender": function ( data, type, full ) {
        return data.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      }},
  { "sTitle": "Packets Per Hour", "sClass": "control",
      "mRender": function ( data, type, full ) {
        return data.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      }},
  { "sTitle": "Bytes Per Hour", "sClass": "control",
      "mRender": function ( data, type, full ) {
        return data.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      }},
  { "sTitle": "JobTimestamp", "sClass": "control" },
  { "sTitle": "JobStatus", "sClass": "control",
    "mDataProp": function (source, type, val) {
        if (source[7] === 0){ return 'No jobs running'}
        ratio = (source[6] / source[7]) * 100;
        return '<div class="progress progress-warning progress-striped"><div class="bar" style="width: '+ratio+'%"></div></div>';
    }},
  { "sTitle": "JobMax", "sClass": "control", "bVisible": false}
  ]
} );

//update info
$("#numOfRouters").text(data.length);

});

anOpen = [];

}initial();


/*
This section handles all the router channel
*/
//SSE (server side events - push) - CURRENTLY ONLY WORKS ON ALL BROWSERS EXCEPT IE - will use polyfill later
function sse() {
  var source = new EventSource('/stream?channels=routerUpdates,jobUpdates');
  source.onmessage = function(e) {
    var jsondata = JSON.parse(e.data)[0]; //data
    var channel = JSON.parse(e.data)[1]; //channel
    if(typeof jsondata === 'object' && channel == 'routerUpdates'){
      var nextRowToUpdate = jsondata.shift();
      for (var i = 0; i < globaldata.length; i++) {
        if (globaldata[i][0] == nextRowToUpdate[0]) {
          //found a row to update
          $('#routers').dataTable().fnUpdate(nextRowToUpdate, i);
          if (jsondata.length == 0){
            nextRowToUpdate = null; //set to null as we have just updated this one
            break; //we have just updated the last router
          } else {
            nextRowToUpdate = jsondata.shift(); //pop off next update
          }
        }
      }
      //if we still have routers left they are NEW :O
      if (nextRowToUpdate !== null){
        $('#routers').dataTable().fnAddData(nextRowToUpdate);
      }
    }else if(channel == 'jobUpdates'){

    }
  };
} sse();
