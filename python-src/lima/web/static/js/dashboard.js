
$.getJSON("/get?id=latestEvents", function(data){
  console.debug("INITAL GET", data)


var myStringArray = ["Hello","World"];



for (var i = 0; i < data.length; i++) {
    var date = new Date(data[i][3]);
    data[i][3] = date.toLocaleString();
}



  $('#latestEvents').dataTable( {
    "aaData": data,
    "bLengthChange": false,
    "bPaginate": false,
    //"sPaginationType": "bootstrap",
    "bSort": false,
    "bFilter": false,
    "bInfo": false,
    //"sDom": "<'row'<'span6'l><'span6'f>r>t<'row'<'span6'i><'span6'p>>",
    //"oLanguage": {
    //        "sLengthMenu": "_MENU_ records per page"
    //    },
    "aoColumns": [
    { "sTitle": "Event ID", "sClass": "control", "bVisible": false },
    { "sTitle": "Router IP" , "sClass": "control"},
    { "sTitle": "Type", "sClass": "control" },
    { "sTitle": "Time Submitted", "sClass": "control" }
    ]
  } );



/*
$('#example').dataTable( {
        "sDom": "<'row'<'span6'l><'span6'f>r>t<'row'<'span6'i><'span6'p>>",
        "sPaginationType": "bootstrap",
        "oLanguage": {
            "sLengthMenu": "_MENU_ records per page"
        }
    } );
*/
});


$.getJSON("/get?id=runningJobs", function(data){
  console.debug("INITAL GET", data)

for (var i = 0; i < data.length; i++) {
    var date = new Date(data[i][1]);
    data[i][1] = date.toLocaleString();
}


  $('#runningJobs').dataTable( {
    "aaData": data,
    "bLengthChange": false,
    "bPaginate": false,
    "bSort": false,
    "bFilter": false,
    "bInfo": false,
    "aoColumns": [
    { "sTitle": "Router IP", "sClass": "control", "sWidth": "10px"},
    { "sTitle": "Last Seen", "sClass": "control", "sWidth": "140px"},
    { "sTitle": "Progress" , "sClass": "control",

    "mDataProp": function (source, type, val) {
        ratio = (source[2] / source[3]) * 100;
        return '<div class="progress progress-success progress-striped"><div class="bar" style="width: '+ratio+'%"></div></div>';}

    },


    { "sTitle": "Max Jobs", "sClass": "control", "bVisible": false}
    ]
  } );

});