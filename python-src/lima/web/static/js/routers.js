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
  "aoColumns": [
  { "sTitle": "Router IP", "sClass": "control"},
  { "sTitle": "Last Seen", "sClass": "control" },
  { "sTitle": "Flows Per Hour", "sClass": "control" },
  { "sTitle": "Packets Per Hour", "sClass": "control"},
  { "sTitle": "Bytes Per Hour", "sClass": "control"},
  { "sTitle": "JobTimestamp", "sClass": "control" },
  { "sTitle": "JobStatus", "sClass": "control"},
  { "sTitle": "JobMax", "sClass": "control"},
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
