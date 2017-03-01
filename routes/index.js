var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', (req, res, next) => {
	return res.render('index', {title: "Crawler"});
})

module.exports = router;