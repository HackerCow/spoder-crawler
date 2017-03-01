const cluster = require('cluster');
const MongoClient = require('mongodb').MongoClient;
const winston = require('winston');
const logger = new (winston.Logger)({
    transports: [
        // colorize the output to the console
        new (winston.transports.Console)({ colorize: true, timestamp: true })
    ],
    level: "debug"
});

MongoClient.connect("mongodb:///tmp/mongodb-27017.sock/crawler", (err, db) => {
	if (cluster.isMaster) {
		const CircularBuffer = require("circular-buffer");
		const child_process = require("child_process");
		const express = require('express');
		const path = require("path");
		const cookieParser = require('cookie-parser');
		const bodyParser = require('body-parser');
		const ws = require("nodejs-websocket");

		const index = require('./routes/index');
		const search = require('./routes/search');

		const app = express();
		app.locals.db = db;


		const rateConnections = [];
		const countsConnections = [];
		const urlConnections = [];

		let urlsSinceTick = 0;
		const circBuf = new CircularBuffer(100);

		setInterval(() => {
			circBuf.enq(urlsSinceTick);
			urlsSinceTick = 0;

			const size = circBuf.size();
			let total = 0;
			for(let i = 0; i < size; i++) {
			    total += circBuf.get(i)
			}
			const avg = total / size;

            rateConnections.forEach((conn) => {
                conn.sendText(avg.toFixed(2))
			})

		}, 1000);

		setInterval(() => {
            db.collection("jobCounts").findOne({}, {"_id": false}, (err, document) => {
            	let str = JSON.stringify(document);
				countsConnections.forEach((conn, index) => {
					conn.sendText(str)
				})
            })
		}, 500);

		function messageHandler(msg) {
			if (msg.cmd) {
				if (msg.cmd === "urlCompleted") {
					urlConnections.forEach((conn, index) => {
						conn.sendText(msg.val)
					});
					urlsSinceTick++
				}
			}
		}

		for(let i = 0; i < 16; i++) {
			let worker = cluster.fork();
			worker.on('message', messageHandler)
		}

		app.set('views', path.join(__dirname, 'views'));
		app.set('view engine', 'jade');

		app.use(bodyParser.json());
		app.use(bodyParser.urlencoded({ extended: false }));
		app.use(cookieParser());
		app.use(express.static(path.join(__dirname, 'public')));

		app.use('/', index);
		app.use('/', search);

		// catch 404 and forward to error handler
		app.use((req, res, next) => {
		  let err = new Error('Not Found');
		  err.status = 404;
		  next(err);
		});

		// error handler
        app.use((err, req, res) => {
            // set locals, only providing error in development
            res.locals.message = err.message;
            res.locals.error = req.app.get('env') === 'development' ? err : {};

            // render the error page
            res.status(err.status || 500);
            res.render('error');
		});

		app.listen(3000, () =>{
            logger.info('Listening on port 3000!');
		});

		const server = ws.createServer(conn => {
            let index;
            if (conn.path === "/rate") {
                index = rateConnections.push(conn) - 1;

                conn.on("close", () => {
                    conn.on("error", () => {});
                    rateConnections.splice(index, 1)
                })
            } else if (conn.path === "/urls") {
                index = urlConnections.push(conn) - 1;

                conn.on("close", () => {
                    conn.on("error", () => {});
                    urlConnections.splice(index, 1)
                })
            } else if (conn.path === "/counts") {
                index = countsConnections.push(conn) - 1;

                conn.on("close", () => {
                    conn.on("error", () => {});
                    countsConnections.splice(index, 1)
                })
            } else {
                conn.close()
            }
        }).listen(1337);

	} else {
		const monq = require("monq");
		const request = require("request");
		const cheerio = require("cheerio");
		const url = require("url");
		const XXHash = require("xxhash");

		const client = monq('mongodb:///tmp/mongodb-27017.sock/crawler');
		const worker = client.worker(['urls']);

		function nthIndex(str, pat, n){
		    let L = str.length, i = -1;
		    while(n-- && i++<L){
		        i= str.indexOf(pat, i);
		        if (i < 0) break;
		    }
		    return i;
		}
		process.setMaxListeners(0);

		const queue = client.queue('urls');

		function insertDead(url, error) {
			db.collection("dead-urls").insertOne({
				url: url,
				error: error,
				lastChecked: new Date()
			}, (err, result) => {
				logger.warn("dead url: " + url);
			});
		}

		worker.register({
		    checkUrl: (params, callback) => {
		    	const base = params.url.substring(0, nthIndex(params.url, "/", 3));
		    	logger.debug(` [worker ${cluster.worker.id}] processing ${params.url}`);

				let prKnown = new Promise((resolve, reject) => {
					db.collection("known-urls").findOne({url: params.url}, (err, document) => {
                        return document ? reject("already known") : resolve();
					})
				});

				let prDead = new Promise((resolve, reject) => {
					db.collection("dead-urls").findOne({url: params.url}, (err, document) => {
                        return document ? reject("already known (dead)") : resolve();
					})
				});

                Promise.all([prKnown, prDead]).then(() => {
                	let options = {
                    	timeout: 3000,
						gzip: true,
						headers: {
                    		"Accept": "text/plain, text/html"
						}
					};

					request.get(params.url, options, (error, response, body) => {
						if(!response || !response.headers || !response.headers["content-type"] || response.headers["content-type"].indexOf("text/html") === -1) {
		                	if (error) {
			    				insertDead(params.url, error.code);
			    				logger.warn("error (" + error.code + ")");
			    			} else {
			    				logger.warn("not text, skipping " + params.url + " (was " + response.headers["content-type"] + ")");
			    			}
			    			
		                    return callback()
						}

						if (error || response.statusCode !== 200) {
							if (response) {
								logger.warn("http error " + response.statusCode);
								insertDead(params.url, response.statusCode)
							} else if (error) {
								logger.warn("error " + error.code);
								insertDead(params.url, error.code)
							} else {
								logger.warn("unknown error");
								insertDead(params.url, "UNKNOWN")
							}

							return callback(error)
						}

						const hash = XXHash.hash64(Buffer.from(body), 0xCAFEBABE, "hex");
						db.collection("known-urls").findOne({xxhash: hash}, (err, document) => {
							if (document != null) {
								logger.warn("already known (hash) (" + params.url + " === " + document.url + ")");
								return callback()
							}

							//console.time("cheerio load")
							try {
							    $ = cheerio.load(body)
			                } catch (err) {
			                    logger.warn("cheerio couldn't load, probably not html");
			                    insertDead(params.url, "CHEERIO");
			                    return callback()
			                }
							//console.timeEnd("cheerio load")

							const titleElem = $("title");
							const title = titleElem ? titleElem.text() : null;

							const metaElem = $("meta[name=description][content]");
							const meta = metaElem ? metaElem.attr("content") : null;

							let as = $("a");
							let urlCounts = as.length;

							let urlArr = [];
							let prEach = new Promise((resolve, reject) => {
                                as.each((i, elem) => {
                                    if(elem.attribs && elem.attribs.href && !elem.attribs.href.startsWith("javascript:")) {
                                        urlCounts++;
                                        const href = url.resolve(params.url, elem.attribs.href);
                                        if (href !== params.url) {
                                            db.collection("jobs").findOne({"params.url": href}, (err, document) => {
                                                if (document === null || document === undefined) {
                                                    queue.enqueue('checkUrl', { url: href }, (err, job) =>{
                                                        db.collection("jobCounts").findOneAndUpdate({}, {
                                                            "$inc": {"queued": 1}
                                                        }, {}, () => {
                                                            return resolve()
                                                        })
                                                    });
                                                }
                                            })
                                        }
                                    }
                                });
                            });

							//console.time("insert")
							let prIns = new Promise((resolve, reject) => {
							    db.collection("known-urls").insertOne({
                                    url: params.url,
                                    title: title,
                                    description: meta,
                                    descriptionFrom: "META_TAG",
                                    xxhash: hash,
                                    lastChecked: new Date(),
                                    subUrls: urlCounts
                                }, (err, result) => {
							        resolve()
                                });
                            });

							return callback()
						})
					})
				}).catch((err) => {
					logger.warn(err);
					return callback()
	            });
		    }
		});

		const completeFunc = (job, err) => {
            process.send({cmd: 'urlCompleted', val: job.params.url});

            db.collection("jobCounts").findOneAndUpdate({}, {
                "$inc": {"complete": 1}
            })
        };

		worker.on("complete", completeFunc);
		worker.on("error", completeFunc);

		worker.on("failed", (job, err) => {
			db.collection("jobCounts").findOneAndUpdate({}, {
				"$inc": {"failed": 1}
			})
		});

		worker.on("dequeued", (job, err) => {
			db.collection("jobCounts").findOneAndUpdate({}, {
				"$inc": {"queued": -1}
			})
		});

		worker.start();
	}
});