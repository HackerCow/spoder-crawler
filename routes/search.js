var express = require('express');
var router = express.Router();

function search(q, db) {
	return db.collection("known-urls").find({"$text": {"$search" : q}}, {"_id": false}).toArray()
}

router.get('/search', (req, res, next) => {
	if (!req.query || !req.query.q) {
		return res.status(400).send("Invalid request")
	}

	var results = search(req.query.q, res.app.locals.db)

	results.then((r) => {
		return res.render('search', {results: r})
	})
})

module.exports = router;