
      $.getJSON("/get?id=events", function(data){
        console.debug("INITAL GET", data)


      $('#events').dataTable( {
        "aaData": data,
        "aoColumns": [
            { "sTitle": "Event ID" },
            { "sTitle": "Router IP" },
            { "sTitle": "Type" },
            { "sTitle": "Status"},
            { "sTitle": "Message"},
            { "sTitle": "Start Time"},
            { "sTitle": "End Time"},
            { "sTitle": "Time Submitted"}
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
          
          if(typeof JSON.parse(e.data) === 'object'){
            console.log("ADDED NEW ROW");
            $('#events').dataTable().fnAddData(JSON.parse(e.data));
          }

          //$('#packet-count').text(data.count); //update the packet count from json info
          //$('.tablelist').text(data.tables); //update the table list from json info
        };
      } sse();