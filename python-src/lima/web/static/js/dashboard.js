//SSE (server side events - push) - CURRENTLY ONLY WORKS ON ALL BROWSERS EXCEPT IE - will use polyfill later
function sse() {
        var source = new EventSource('/stream');
        source.onmessage = function(e) {
          var data = JSON.parse(e.data);
          $('#packet-count').text(data.count); //update the packet count from json info
          $('.tablelist').text(data.tables); //update the table list from json info
        };
} sse();