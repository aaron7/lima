/*
This file contains the scripts to control the Events page
*/

piechartData = [];

/*
This section gets the events and shows them in the table
*/
$.getJSON("/get?id=events", function(data){
  
  var dos = 0;
  var tcp = 0;
  var udp = 0;
  var fraggle = 0;
  var ping = 0;
  var icmp = 0;
  var portscan = 0;
  var land = 0;
  var unknown = 0;

  //create the table
  $('#events').dataTable( {
    "aaData": data,
    "bLengthChange": false,
    "bPaginate": false,
    "bAutoWidth": false,
  "fnFooterCallback": function ( nRow, aaData, iStart, iEnd, aiDisplay ) {
    //make pie chart
            for ( var i=0 ; i<aaData.length ; i++ )
            {
              switch(aaData[i][2])
              {
                case "dos": dos += 1; break;
                case "tcpFlooding": tcp += 1; break;
                case "udpFlooding": udp += 1; break;
                case "fraggleAttack": fraggle += 1; break;
                case "pingPong": ping += 1; break;
                case "icmpFlooding": icmp += 1; break;
                case "portScan": portscan += 1; break;
                case "landAttack": land += 1; break;
                default: unknown += 1;;
              };

              //last event, put it in summary
              if (i == aaData.length - 1) {
                console.log(aaData[i][2]);
                var lastThreat = "";
                switch(aaData[i][2])
                {
                case "dos": lastThreat = "DoS Attack"; break;
                case "tcpFlooding": lastThreat = "TCP Flooding"; break;
                case "udpFlooding": lastThreat = "UDP Flooding"; break;
                case "fraggleAttack": lastThreat = "Fraggle Attack"; break;
                case "pingPong": lastThreat = "Ping-pong Attack"; break;
                case "icmpFlooding": lastThreat = "ICMP Flooding"; break;
                case "portScan": lastThreat = "Port Scan"; break;
                case "landAttack": lastThreat = "Land Attack"; break;
                default: lastThreat = "Unknown";
              }
              $("#lastThreat").text(lastThreat + " - " + aaData[i][1]);
            }
          }
          piechartData = [
          { label: "TCP flooding",  data: tcp},
          { label: "Land Attack",  data: land},
          { label: "UDP Flooding",  data: udp},
          { label: "Fraggle Attack",  data: fraggle},
          { label: "Ping-pong",  data: ping},
          { label: "ICMP Flooding",  data: icmp},
          { label: "Port Scan",  data: portscan},
          { label: "DoS Attack",  data: dos},
          { label: "Unknown",  data: unknown}];

        },
    "aoColumns": [
    { "sTitle": "Event ID", "sClass": "control"},
    { "sTitle": "Router IP" , "sClass": "control"},
    { "sTitle": "Type", "sClass": "control" ,
      "mRender": function ( data, type, full ) {
        switch(data)
        {
          case "dos": return "DoS Attack"; break;
          case "tcpFlooding": return "TCP Flooding"; break;
          case "udpFlooding": return "UDP Flooding"; break;
          case "fraggleAttack": return "Fraggle Attack"; break;
          case "pingPong": return "Ping-pong Attack"; break;
          case "icmpFlooding": return "ICMP Flooding"; break;
          case "portScan": return "Port Scan"; break;
          case "landAttack": return "Land Attack"; break;
          default: return "Unknown threat type";
        };
      }},
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

//make piechart
          $.plot('#piechart', piechartData, {
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


});

  // A custom label formatter used by several of the plots

  function labelFormatter(label, series) {
    return "<div style='font-size:8pt; text-align:center; padding:2px; color:black;'>" + label + "<br/>" + Math.round(series.percent) + "%</div>";
  }



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