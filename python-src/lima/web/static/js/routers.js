function initial(){
  $.getJSON("/get?id=routers", function(data){
    globaldata = data;

    console.debug("INITAL GET ROUTERS", data);


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
  });
/*
  $.getJSON("/get?id=jobs", function(data){
    var jobs = data;

    console.debug("INITAL GET JOBS", data);
    var nextJob = jobs.shift();
    console.debug("Look for job", nextJob);
    for (var i = 0; i < globaldata.length; i++) {
      if (globaldata[i][0].replace(/ /g, '') === nextJob[0]){
          //we found the global router which has job info
          nextJob.push("hi");
          $('#routers').dataTable().fnDeleteRow(i);
          $('#routers').dataTable().fnAddData(nextJob);
        console.debug("FOUND JOB INFO FOR: ", nextJob[0], i);
      }
    }
  });*/

anOpen = [];

}initial();



//SSE (server side events - push) - CURRENTLY ONLY WORKS ON ALL BROWSERS EXCEPT IE - will use polyfill later
function sse() {
  var source = new EventSource('/stream?channels=routerUpdates,jobUpdates');
  source.onmessage = function(e) {
    console.log("NEW MESSAGE: ", e);
    var jsondata = JSON.parse(e.data)[0]; //data
    var channel = JSON.parse(e.data)[1]; //channel
    console.log("0: ", jsondata);
    console.log("1: ", channel);
    
    if(typeof jsondata === 'object' && channel == 'routerUpdates'){
      console.log("ROUTER UDPATES");
      var nextRowToUpdate = jsondata.shift();
      console.log(nextRowToUpdate);
      for (var i = 0; i < globaldata.length; i++) {
        if (globaldata[i][0] == nextRowToUpdate[0]) {
          console.log("FOUND ROW",i);
          //found a row to update
          $('#routers').dataTable().fnUpdate(nextRowToUpdate, i);
          //$('#routers').dataTable().fnDeleteRow(i);
          //$('#routers').dataTable().fnAddData(nextRowToUpdate);
          console.log(jsondata.length);
          if (jsondata.length == 0){
            nextRowToUpdate = null; //set to null as we have just updated this one
            break; //we have just updated the last router
          } else {
            nextRowToUpdate = jsondata.shift(); //pop off next update
          }
        }
      }
      //if we still have routers left they are NEW :O
      console.log(nextRowToUpdate);
      console.log(nextRowToUpdate !== null);
      if (nextRowToUpdate !== null){
        console.log(nextRowToUpdate);
        //$('#routers').dataTable().fnUpdate(nextRowToUpdate, i);
        $('#routers').dataTable().fnAddData(nextRowToUpdate);
        //$('#routers').dataTable().fnAddData(jsondata);
      }
    }else if(channel == 'jobUpdates'){

    }
  };
} sse();
