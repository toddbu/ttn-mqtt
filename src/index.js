const mqtt = require('mqtt')
const request = require('request')
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

  //$ How to make this aync call wait???
  const bytes = uplinkMessage.decoded_payload.bytes
  request.post('https://nam1.cloud.thethings.network/api/v3/as/applications/toddbu-temperature/devices/eui-9876b60000120438/down/replace', {
    headers: {
      Authorization: 'Bearer NNSXS.OGAANBBTJQZCAG7OL6RVKB4LGXMY4EXATQUNMUQ.7YYUJILVXUKXISLEUWSDROPA45L52HQK3T4VJSUCTB53EJGOGDDA',
      'Content-Type': 'application/json',
      'User-Agent': 'my-integration/my-integration-version'
    },
    json: true,
    body: {
      downlinks: [
        {
          frm_payload: Buffer.from([bytes[0], bytes[1], bytes[2], bytes[3]]).toString('base64'),
          f_port: 1,
          priority: 'NORMAL'
        }
      ]
    }
  }, (err, response, body) => {
    if (err) {
      console.log(err)
      return
    }

    console.log('statusCode:', response && response.statusCode)
  })

  //$ mqttClient.end()
})

//$
/*
curl -i --location \
  --header 'Authorization: Bearer NNSXS.OGAANBBTJQZCAG7OL6RVKB4LGXMY4EXATQUNMUQ.7YYUJILVXUKXISLEUWSDROPA45L52HQK3T4VJSUCTB53EJGOGDDA' \
  --header 'Content-Type: application/json' \
  --header 'User-Agent: my-integration/my-integration-version' \
  --request POST \
  --data '{"downlinks":[{
      "frm_payload":"AQ==",
      "f_port":1,
      "priority":"NORMAL"
    }]
  }' \
  https://nam1.cloud.thethings.network/api/v3/as/applications/toddbu-temperature/devices/eui-9876b60000120438/down/replace
*/