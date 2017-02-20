var monq = require("monq")
var request = require("request")
var cheerio = require("cheerio")
var url = require("url")
var MongoClient = require('mongodb').MongoClient

var client = monq('mongodb://localhost:27017/crawler')
var worker = client.worker(['urls']);

function nthIndex(str, pat, n){
    var L= str.length, i= -1;
    while(n-- && i++<L){
        i= str.indexOf(pat, i);
        if (i < 0) break;
    }
    return i;
}
process.setMaxListeners(0);

var queue = client.queue('urls');

MongoClient.connect("mongodb://localhost:27017/crawler", (err, db) => {

	function insertDead(url, error) {
		db.collection("dead-urls").insertOne({
			url: url,
			error: error,
			lastChecked: new Date()
		}, (err, result) => {
			console.log("dead url: " + url)
		})
	}

	worker.register({
	    checkUrl: (params, callback) => {
	    	var base = params.url.substring(0, nthIndex(params.url, "/", 3))
	    	console.log(new Date().toISOString() + " processing " + params.url)

			db.collection("known-urls").findOne({url: params.url}, (err, document) => {
				if (document != null) {
					console.log("already known")
					callback()
					return
				}
			})

			//console.time("get request")
			request.get(params.url, {timeout: 5000, gzip: true}, (error, response, body) => {
				//console.timeEnd("get request")
				if(!response || !response.headers || !response.headers["content-type"] || response.headers["content-type"].indexOf("text/html") === -1) {
	    			if (error) {
	    				insertDead(params.url, error.code)
	    				console.log("error (" + error.code + ")")
	    			} else {
	    				console.log("not text, skipping (was " + response.headers["content-type"] + ")")
	    			}
	    			callback()
	    			return
				}

				if (error || response.statusCode !== 200) {
					if (response) {
						console.log("http error " + response.statusCode)
						insertDead(params.url, response.statusCode)
					} else if (error) {

						console.log("error " + error.code)
						insertDead(params.url, error.code)
					} else {
						console.log("unknown error")
						insertDead(params.url, "UNKNOWN")
					}
					callback(error)
					return
				}

				//console.time("cheerio load")
				$ = cheerio.load(body)
				//console.timeEnd("cheerio load")

				var titleElem = $("title")
				var title = titleElem ? titleElem.text() : null

				var metaElem = $("meta[name=description][content]")
				var meta = metaElem ? metaElem.attr("content") : null;

				//console.time("insert")
				db.collection("known-urls").insertOne({
					url: params.url,
					title: title,
					meta: meta,
					lastChecked: new Date()
				}, (err, result) => {
					$("a").each((i, elem) => {
						if(elem.attribs && elem.attribs.href) {
							var href = url.resolve(base, elem.attribs.href)
							db.collection("jobs").findOne({"params.url": href}, (err, document) => {
								if (document === null || document === undefined) {
									queue.enqueue('checkUrl', { url: href }, function (err, job) {
										//console.log('enqueued:', href);
									})
								}
							})
						}
					})
					//console.timeEnd("insert")
				})
				callback()
			})
	    }
	})

	worker.interval = 1

	worker.start();
})
