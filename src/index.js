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
      const clientDate = new Date(`${formatDateComponent(bytes[4])}${formatDateComponent(bytes[5])}-${formatDateComponent(bytes[6])}-${formatDateComponent(bytes[7])} ${formatDateComponent(bytes[8])}:${formatDateComponent(bytes[9])}:${formatDateComponent(bytes[10])}Z`)
      const serverDate = new Date()
      const payloadBytes = Buffer.from(bytes)
      // The Pico is pretty limited in datetime processing capabillites,
      // so we'll calcullate the RTC offsets here. Also, because we can
      // only send unsigned bytes then we'll add 128 to the calculated
      // value so that we can have negative numbers
      payloadBytes[4] = parseInt((serverDate.getFullYear() - clientDate.getFullYear()) / 100) + 128
      payloadBytes[5] = parseInt((serverDate.getFullYear() - clientDate.getFullYear()) % 100) + 128
      payloadBytes[6] = serverDate.getMonth() - clientDate.getMonth() + 128
      payloadBytes[7] = serverDate.getDate() - clientDate.getDate() + 128
      payloadBytes[8] = serverDate.getHours() - clientDate.getHours() + 128
      payloadBytes[9] = serverDate.getMinutes() - clientDate.getMinutes() + 128
      payloadBytes[10] = serverDate.getSeconds() - clientDate.getSeconds() + 128
      payload = payloadBytes.toString('base64')
      console.log(clientDate, payload)
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
