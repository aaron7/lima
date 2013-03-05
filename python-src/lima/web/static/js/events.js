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
    { "sTitle": "Start Time", "sClass": "control",
      "mRender": function ( data, type, full ) {
        var date = new Date(data);
        return date.toLocaleString();
      }},
    { "sTitle": "End Time", "sClass": "control",
      "mRender": function ( data, type, full ) {
        var date = new Date(data);
        return date.toLocaleString();
      }},
    { "sTitle": "Time Submitted", "sClass": "control",
      "mRender": function ( data, type, full ) {
        var date = new Date(data);
        return date.toLocaleString();
      }
  }
    ]
  } );

  //update info
  $("#numOfEvents").text(data.length);


  //format timestamps
for (var i = 0; i < data.length; i++) {
  var date = new Date(data[i][1]);
  data[i][1] = date.toLocaleString();
  var date = new Date(data[i][5]);
  data[i][5] = date.toLocaleString();
}

});

//pie chart
var data = [
    { label: "TCP flooding",  data: 1},
    { label: "Land Attack",  data: 1},
    { label: "UDP Flooding",  data: 1},
    { label: "Fraggle Attack",  data: 1},
    { label: "PingPong",  data: 1},
    { label: "ICMP Flooding",  data: 1},
    { label: "Port Scan",  data: 1},
    { label: "DOS Attack",  data: 1}];

  // A custom label formatter used by several of the plots

  function labelFormatter(label, series) {
    return "<div style='font-size:8pt; text-align:center; padding:2px; color:black;'>" + label + "<br/>" + Math.round(series.percent) + "%</div>";
  }


$.plot('#piechart', data, {
    series: {
        pie: {
            show: true,
            radius: 1,
            label: {
                show: true,
                radius: 2/3,
                formatter: labelFormatter,
                threshold: 0.1
            }
        }
    },
    legend: {
        show: false
    }
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