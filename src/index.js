const process = require('process')
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

function formatDateComponent(value) {
  return value.toString().padStart(2, 0)
}

mqttClient.on('message', async (topic, message) => {
  // message is Buffer
  const uplinkMessage = JSON.parse(message.toString()).uplink_message
  if ((typeof uplinkMessage.f_port === 'undefined') ||
      (uplinkMessage.f_port !== 2)) {
    return
  }

  const decoded_payload = uplinkMessage.decoded_payload
  //$ console.log(decoded_payload)

  process.stdout.write(`${uplinkMessage.received_at} - `)
  let payload = 'AA==' // One byte binary 0
  const bytes = uplinkMessage.decoded_payload.bytes
  console.log(bytes)
  switch (decoded_payload.type) {
    case 0:
      console.log(`datetime request`)
      clientDate = new Date(`${formatDateComponent(bytes[4])}${formatDateComponent(bytes[5])}-${formatDateComponent(bytes[6])}-${formatDateComponent(bytes[7])} ${formatDateComponent(bytes[8])}:${formatDateComponent(bytes[9])}:${formatDateComponent(bytes[10])}Z`)
      timeDelta = parseInt((new Date().valueOf() - clientDate.valueOf()) / 1000)
      const payloadBytes = Buffer.from(bytes.slice(0, 8))
      payloadBytes[4] = timeDelta & 0xFF
      payloadBytes[5] = timeDelta >> 8 & 0xFF
      payloadBytes[6] = timeDelta >> 16 & 0xFF
      payloadBytes[7] = timeDelta >> 24 & 0xFF
      payload = payloadBytes.toString('base64')
      console.log(`${formatDateComponent(bytes[4] * 100)}${formatDateComponent(bytes[5])}-${formatDateComponent(bytes[6])}-${formatDateComponent(bytes[7])} ${formatDateComponent(bytes[8])}:${formatDateComponent(bytes[9])}:${formatDateComponent(bytes[10])}Z`, clientDate, timeDelta, payload)
      break

    case 1:
      console.log(`temperature request: ${decoded_payload.temp}F`)
      payload = Buffer.from([bytes[0], bytes[1], bytes[2], bytes[3]]).toString('base64')
      break

    default:
      console.log(`unknown type: ${decoded_payload.type}`)
      break

  }
  for (i = 0; i < 4 + decoded_payload.contentLength; i++) {
    process.stdout.write(`${decoded_payload.bytes[i]} `)
  }
  console.log()

  const query = 'INSERT INTO temperature.temperature (station_id, received_at, temperature_in_f) VALUES(?, ?, ?)'
  const params = [
    1,
    uplinkMessage.received_at,
    uplinkMessage.decoded_payload.temp
  ]

  await cassandraClient.execute(query, params, { prepare: true });

  //$ How to make this aync call wait???
  request.post('https://nam1.cloud.thethings.network/api/v3/as/applications/toddbu-temperature/devices/eui-9876b60000120438/down/push', {
    headers: {
      Authorization: 'Bearer NNSXS.OGAANBBTJQZCAG7OL6RVKB4LGXMY4EXATQUNMUQ.7YYUJILVXUKXISLEUWSDROPA45L52HQK3T4VJSUCTB53EJGOGDDA',
      'Content-Type': 'application/json',
      'User-Agent': 'my-integration/my-integration-version'
    },
    json: true,
    body: {
      downlinks: [
        {
          frm_payload: payload,
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

    if (response && (response.statusCode !== 200)) {
      console.log('statusCode:', response.statusCode)
    }
  })

  //$ mqttClient.end()
})
