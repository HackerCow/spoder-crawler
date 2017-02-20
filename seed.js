var monq = require('monq')

var client = monq('mongodb://localhost:27017/crawler')

var queue = client.queue('urls');

for(var i = 0; i < 1; i++) {
	queue.enqueue('checkUrl', { url: 'https://en.wikipedia.org/' }, function (err, job) {
	    console.log('enqueued:', job.data);
	});
}

client.close()