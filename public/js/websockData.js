var countsConn = new WebSocket('wss://crawler.ws.cnot.es/counts')
var rateConn = new WebSocket('wss://crawler.ws.cnot.es/rate')

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}


// Log errors
countsConn.onerror = rateConn.onerror = function (error) {
  console.log('WebSocket Error ' + error);
};

var completeSpan = document.querySelector("#numCompleted")
var queuedSpan = document.querySelector("#numQueued")
var rateSpan = document.querySelector("#rate")

// Log messages from the server
countsConn.onmessage = function (e) {
	var elem = JSON.parse(e.data)
	completeSpan.textContent = numberWithCommas(elem.complete)
	queuedSpan.textContent = numberWithCommas(elem.queued)
};

rateConn.onmessage = function (e) {
	rateSpan.textContent = e.data
};