function initial(){
  $.getJSON("/get?id=routers", function(data){
    globaldata = data;

    console.debug("INITAL GET", data);


    $('#routers').dataTable( {
      "aaData": data,
      "aoColumns": [
      { "sTitle": "Router IP" },
      { "sTitle": "Last Seen" },
      { "sTitle": "Flows Per Hour" },
      { "sTitle": "Packets Per Hour"},
      { "sTitle": "Bytes Per Hour"}
      ]
    } );

  });
}initial();

$(document).ready(function() {
        //consider putting sse and other load stuff here
} );

//SSE (server side events - push) - CURRENTLY ONLY WORKS ON ALL BROWSERS EXCEPT IE - will use polyfill later
function sse() {
  var source = new EventSource('/stream?channels=routerUpdates');
  source.onmessage = function(e) {
    console.log("NEW MESSAGE: ", JSON.parse(e.data));
    var jsondata = JSON.parse(e.data);

    if(typeof jsondata === 'object'){
      console.log("ROUTER UDPATES");
      var nextRowToUpdate = jsondata.shift();
      for (var i = 0; i < globaldata.length; i++) {
        if (globaldata[i][0] == nextRowToUpdate[0]) {
          //found a row to update
          $('#routers').dataTable().fnDeleteRow(i);
          $('#routers').dataTable().fnAddData(nextRowToUpdate);
          if (jsondata.length == 0){
            nextRowToUpdate = null; //set to null as we have just updated this one
            break; //we have just updated the last router
          } else {
            nextRowToUpdate = jsondata.shift(); //pop off next update
          }
        }
      }
      //if we still have routers left they are NEW :O
      if (typeof nextRowToUpdate !== null){
        $('#routers').dataTable().fnAddData(nextRowToUpdate);
        $('#routers').dataTable().fnAddData(jsondata);
      }
    }
  };
} sse();
