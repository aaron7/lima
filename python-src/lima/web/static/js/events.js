
$.getJSON("/get?id=events", function(data){
  console.debug("INITAL GET", data)


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

});

$(document).ready(function() {
        //consider putting sse and other load stuff here
      } );

      //SSE (server side events - push) - CURRENTLY ONLY WORKS ON ALL BROWSERS EXCEPT IE - will use polyfill later
      function sse() {
        var source = new EventSource('/stream?channels=events');
        source.onmessage = function(e) {
          console.log("NEW MESSAGE: ", JSON.parse(e.data))
          var jsondata = JSON.parse(e.data)[0]; //data
          var channel = JSON.parse(e.data)[1]; //channel
          console.log("0: ", jsondata);
          console.log("1: ", channel);


          
          if(typeof jsondata === 'object' && channel == 'events'){
            console.log("ADDED NEW ROW");
            $('#events').dataTable().fnAddData(jsondata);
          }
          //$('#packet-count').text(data.count); //update the packet count from json info
          //$('.tablelist').text(data.tables); //update the table list from json info
        };
      } sse();