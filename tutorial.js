let colors = require('colors')
let util = require('util')
let PubSub = require('@google-cloud/pubsub')
let Datastore = require('@google-cloud/datastore')

/* CONFIGURATION */
let config = {
    gcpProjectId: 'c177-sensors',
    gcpPubSubSubscriptionName: 'projects/c177-sensors/subscriptions/ingest',
    gcpServiceAccountKeyFilePath: './gcp_private_key.json'
}
_checkConfig();
/* END CONFIGURATION */

/* PUBSUB */
console.log(colors.magenta('Authenticating PubSub with Google Cloud...'))
const pubsub = new PubSub({
    projectId: config.gcpProjectId,
    keyFilename: config.gcpServiceAccountKeyFilePath,
})
console.log(colors.magenta('Authentication successful!'))

const subscription = pubsub.subscription(config.gcpPubSubSubscriptionName);
subscription.on('message', message => {
    console.log(colors.cyan('Particle event received from Pub/Sub!\r\n'), _createParticleEventObjectForStorage(message, true));
    // Called every time a message is received.
    // message.id = ID used to acknowledge its receival.
    // message.data = Contents of the message.
    // message.attributes = Attributes of the message.

    storeEvent(message);
    message.ack();
});
/* END PUBSUB */

/* DATASTORE */
console.log(colors.magenta('Authenticating Datastore with Google Cloud...'))
const datastore = new Datastore({
    projectId: config.gcpProjectId,
    keyFilename: config.gcpServiceAccountKeyFilePath,
})
console.log(colors.magenta('Authentication successful!'))

function storeEvent(message) {
    let key = datastore.key(message.attributes.device_id);

    // Process incoming data
    try {
        const dataJson = JSON.parse(message.data);
    } catch (err) {
        // Incoming data is not in format of the sensors' data, thus process as the original default
        datastore
        .save({
            key: key,
            data: _createParticleEventObjectForStorage(message)
        })
        .then(() => {
            console.log(colors.green('Particle event stored in Datastore!\r\n'), _createParticleEventObjectForStorage(message, true))
        })
        .catch(err => {
            console.log(colors.red('There was an error storing the event:'), err);
        });

        return;
    }
    
    // Process the incoming sensor data sets for Database insertion
    Object.values(dataJson).forEach(readingsSet => {
        let obj = {};

        // Add each entry of sensor reading into obj, checking if value is a Number
        Object.entries(readingsSet).forEach(([key, value]) => {
            if (Number.isNaN(`${value}`)) {
                obj[`${key}`] = `${value}`;
            } else if (`${key}` === "TS") {
                // Special catch for Timestamp, as it apparently is a number
                obj[`${key}`] = `${value}`;
            } else {
                obj[`${key}`] = Number(`${value}`);
            }
        })

        // Add processed sensor data set to Database
        datastore
            .save({
                key: key,
                data: obj
            })
            .then(() => {
                console.log(colors.green('Particle event stored in Datastore!\r\n'), colors.grey(util.inspect(obj)))
            })
            .catch(err => {
                console.log(colors.red('There was an error storing the event:'), err);
            });
    });

};
/* END DATASTORE */

/* HELPERS */
function _checkConfig() {
    if (config.gcpProjectId === '' || !config.gcpProjectId) {
        console.log(colors.red('You must set your Google Cloud Platform project ID in pubSubToDatastore.js'));
        process.exit(1);
    }
    if (config.gcpPubSubSubscriptionName === '' || !config.gcpPubSubSubscriptionName) {
        console.log(colors.red('You must set your Google Cloud Pub/Sub subscription name in pubSubToDatastore.js'));
        process.exit(1);
    }
};

function _createParticleEventObjectForStorage(message, log) {

    let obj = {
        gc_pub_sub_id: message.id,
        device_id: message.attributes.device_id,
        event: message.attributes.event,
        data: String(message.data),
        published_at: message.attributes.published_at
    }

    if (log) {
        return colors.grey(util.inspect(obj));
    } else {
        return obj;
    }
};

/* END HELPERS */
