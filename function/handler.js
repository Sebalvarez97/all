"use strict"

let mongo_user = "openfaas"
let mongo_pass = "VAoOfJLVwX5W86Im"
let dbname = "files"
let collname = "metadata"

const MongoClient = require('mongodb').MongoClient;

var clientsDB;  // Cached connection-pool for further requests.

module.exports = (event, context) => {
	prepareDB()
		.then((database) => {

			var splitByLastDot = function(text) {
				var index = text.lastIndexOf('.');
				return [text.slice(0, index), text.slice(index + 1)]
			}

			const record = {
				_id: event.body.Key,
				creation_date: Date.now(),
				name: splitByLastDot(event.body.Records[0].s3.object.key)[0],
				type: splitByLastDot(event.body.Records[0].s3.object.key)[1],
				size: event.body.Records[0].s3.object.size
			}
			database.collection(collname).insertOne(record, (insertErr) => {
				if (insertErr) {
					{
						if(insertErr.toString().includes('MongoError: E11000')){
							database.collection(collname).updateOne(
								{ _id: record._id },
								{ $set: {
									creation_date: Date.now(),
									name: splitByLastDot(event.body.Records[0].s3.object.key)[0],
									type: splitByLastDot(event.body.Records[0].s3.object.key)[1],
									size: event.body.Records[0].s3.object.size
								},
								  $currentDate: { lastModified: true } })
							  .then(function(result) {
									console.log('Updated')
							  })    
						} else
					return context.fail(insertErr.toString());
				}

				context
					.status(200)
					.succeed(record);
                }
            })
        })
		.catch(err => {
			context.fail(err.toString());
		});

}
const prepareDB = () => {

	const uri = "mongodb+srv://" + mongo_user + ":" + mongo_pass + "@cluster0-uc6in.gcp.mongodb.net/test?retryWrites=true&w=majority";

	const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

	return new Promise((resolve, reject) => {
		if (clientsDB) {
			console.error("DB already connected.");
			return resolve(clientsDB);
		}

		console.error("DB connecting");

		client.connect((err, client) => {
			if (err) {
				return reject(err)
			}

			clientsDB = client.db(dbname);
			return resolve(clientsDB)
		});
	});
}
