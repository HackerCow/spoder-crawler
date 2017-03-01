const monq = require('monq');

const client = monq('mongodb://localhost:27017/crawler');

const queue = client.queue('urls');

for(let i = 0; i < 1; i++) {
	queue.enqueue('checkUrl', { url: 'https://en.wikipedia.org/' }, function (err, job) {
	    console.log('enqueued:', job.data);
	});
}

client.close();