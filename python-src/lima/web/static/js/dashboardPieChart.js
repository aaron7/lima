/*
This file contains the scripts to control the piechart on the dashboard
*/

/*
This section gets the large data for the pie chart information and draws the pie chart
*/
$.getJSON("/get?id=allLargeData", function(largeData){
    var data = [
    { label: "0.0.0.0",  data: 40},
    { label: "10.0.10.1",  data: 20},
    { label: "10.0.10.2",  data: 10},
    { label: "10.0.10.3",  data: 15},
    { label: "10.0.10.4",  data: 15}];


    $.plot('#piechart', data, {
        series: {
            pie: {
                show: true
            }
        },

        grid: {
            hoverable: true,
            clickable: true
        }
    });

	// A custom label formatter used by several of the plots
	function labelFormatter(label, series) {
		return "<div style='font-size:8pt; text-align:center; padding:2px; color:white;'>" + label + "<br/>" + Math.round(series.percent) + "%</div>";
	}

    //when resizing the window then redraw the pie chart
    $(window).resize(function() {
        $("#piechart").height( $('#piechart').width() );
        //re-render
        $.plot('#piechart', data, {
            series: {
                pie: {
                    show: true
                }
            },
            grid: {
                hoverable: true,
                clickable: true
            }
        });
    });
});