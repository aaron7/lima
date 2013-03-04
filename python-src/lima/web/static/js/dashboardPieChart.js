var data = [
{ label: "Series1",  data: 10},
{ label: "Series2",  data: 30},
{ label: "Series3",  data: 90},
{ label: "Series4",  data: 70},
{ label: "Series5",  data: 80},
{ label: "Series6",  data: 110}];


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
