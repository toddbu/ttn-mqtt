const mqtt = require('mqtt')
const cassandra = require('cassandra-driver');

const cassandraClient = new cassandra.Client({
  contactPoints: ['10.0.0.80'],
  localDataCenter: 'datacenter1',
  keyspace: 'temperature'
});

const mqttClient = mqtt.connect('mqtts://nam1.cloud.thethings.network:8883', {
  username: 'toddbu-temperature@ttn',
  password: 'NNSXS.OGAANBBTJQZCAG7OL6RVKB4LGXMY4EXATQUNMUQ.7YYUJILVXUKXISLEUWSDROPA45L52HQK3T4VJSUCTB53EJGOGDDA'
})

mqttClient.on('connect', () => {
  mqttClient.subscribe('v3/toddbu-temperature@ttn/devices/eui-9876b60000120438/up', (err) => {
    if (err) {
      console.log(err)

      return
    }

    //$ mqttClient.publish('presence', 'Hello mqtt')
  })
})

mqttClient.on('message', async (topic, message) => {
  // message is Buffer
  const uplinkMessage = JSON.parse(message.toString()).uplink_message
  console.log(`${uplinkMessage.received_at} - ${uplinkMessage.decoded_payload.temp}F`)


  const query = 'INSERT INTO temperature.temperature (station_id, received_at, temperature_in_f) VALUES(?, ?, ?)'
  const params = [
    1,
    uplinkMessage.received_at,
    uplinkMessage.decoded_payload.temp
  ]

  await cassandraClient.execute(query, params, { prepare: true });

  //$ mqttClient.end()
})
