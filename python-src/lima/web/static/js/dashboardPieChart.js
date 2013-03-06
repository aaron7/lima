/*
This file contains the scripts to control the piechart on the dashboard
*/

/*
This section gets the large data for the pie chart information and draws the pie chart
*/
$.getJSON("/get?id=pieChart", function(largeData){
    var data = [
    { label: "0.0.0.0",  data: 40},
    { label: "0.0.0.0",  data: 40}];

console.log("HERE",largeData)
$.plot('#piechart', largeData, {
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

	// A custom label formatter used by several of the plots
	function labelFormatter(label, series) {
		return "<div style='font-size:8pt; text-align:center; padding:2px; color:black;'>" + label + "<br/>" + Math.round(series.percent) + "%</div>";
	}

    //when resizing the window then redraw the pie chart
    $(window).resize(function() {
        $("#piechart").height( $('#piechart').width() );
        //re-render
$.plot('#piechart', largeData, {
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
});