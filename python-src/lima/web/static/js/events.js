/*
This file contains the scripts to control the Events page
*/

/*
This section gets the events and shows them in the table
*/
$.getJSON("/get?id=events", function(data){
  
  //create the table
  $('#events').dataTable( {
    "aaData": data,
    "bLengthChange": false,
    "bPaginate": false,
    "aoColumns": [
    { "sTitle": "Event ID", "sClass": "control"},
    { "sTitle": "Router IP" , "sClass": "control"},
    { "sTitle": "Type", "sClass": "control" },
    { "sTitle": "Status", "sClass": "control"},
    { "sTitle": "Message", "sClass": "control"},
    { "sTitle": "Start Time", "sClass": "control"},
    { "sTitle": "End Time", "sClass": "control"},
    { "sTitle": "Time Submitted", "sClass": "control"}
    ]
  } );

  //update info
  $("#numOfEvents").text(data.length);

});

/*
This section creates the channel streams to the server
*/
//SSE (server side events - push) - CURRENTLY ONLY WORKS ON ALL BROWSERS EXCEPT IE - will use polyfill later
function sse() {
  var source = new EventSource('/stream?channels=events');
  source.onmessage = function(e) {
    var jsondata = JSON.parse(e.data)[0]; //data
    var channel = JSON.parse(e.data)[1]; //channel
    if(typeof jsondata === 'object' && channel == 'events'){
      $('#events').dataTable().fnAddData(jsondata);
    }
  };
} sse();