const process = require('process')
const mqtt = require('mqtt')
const request = require('request')
const cassandra = require('cassandra-driver')
const config = require('./config')(process.env.ENV)

const DEFAULT_RETURN_PAYLOAD = 'AA==' // One byte binary 0 as a default

const cassandraClient = new cassandra.Client({
  contactPoints: ['10.0.0.80'],
  localDataCenter: 'datacenter1',
  keyspace: 'temperature'
});

const mqttClient = mqtt.connect('mqtts://nam1.cloud.thethings.network:8883', {
  username: config.username,
  password: config.password
})

mqttClient.on('connect', () => {
  mqttClient.subscribe(`v3/${config.applicationName}@ttn/devices/${config.deviceEui}/up`, (err) => {
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

function handle_time_sync(message_type, message_bytes, serverDate) {
  let return_payload = DEFAULT_RETURN_PAYLOAD

  switch (message_type) {
    case 0:
      const payloadBytes = Buffer.from(message_bytes)

      console.log(`datetime request`)

      // Check to see if we get a NOP. If we did then send back a packet with no
      // adjustments since we're just trying to force a download
      if (message_bytes[4] +
          message_bytes[5] +
          message_bytes[6] +
          message_bytes[7] +
          message_bytes[8] +
          message_bytes[9] +
          message_bytes[10] === 0) {
        for (let i = 4; i < 11; i++) {
          payloadBytes[i] = 128
        }
        return_payload = payloadBytes.toString('base64')
        console.log('NOP', return_payload, payloadBytes)
        break;
      }

      const clientDate = new Date(`${formatDateComponent(message_bytes[4])}${formatDateComponent(message_bytes[5])}-${formatDateComponent(message_bytes[6])}-${formatDateComponent(message_bytes[7])} ${formatDateComponent(message_bytes[8])}:${formatDateComponent(message_bytes[9])}:${formatDateComponent(message_bytes[10])}Z`)

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
      return_payload = payloadBytes.toString('base64')
      console.log(clientDate, return_payload, payloadBytes)
      break

    default:
      console.log(`unknown message type: ${message_type}`)
      break
  }

  return return_payload
}

async function handle_app_update(message_type, message_bytes, clientMessageDate, decoded_payload) {
  let return_payload = DEFAULT_RETURN_PAYLOAD

  switch (message_type) {
    case 1:
      console.log(`temperature request: ${decoded_payload.temp}F`)
      const query = 'INSERT INTO temperature.temperature (station_id, received_at, temperature_in_f) VALUES(?, ?, ?)'
      const params = [
        1,
        clientMessageDate,
        decoded_payload.temp
      ]

      await cassandraClient.execute(query, params, { prepare: true });
      return_payload = Buffer.from([message_bytes[0], message_bytes[1], message_bytes[2], message_bytes[3]]).toString('base64')
      break

    default:
      console.log(`unknown message type: ${message_type}`)
      break
  }

  return return_payload
}

mqttClient.on('message', async (topic, message) => {
  let return_payload

  // message is a Buffer type
  const uplinkMessage = JSON.parse(message.toString()).uplink_message
  const fPort = uplinkMessage.f_port
  if (typeof fPort === 'undefined') {
    // Toss the message if we don't have an fPort
    return
  }

  const decoded_payload = uplinkMessage.decoded_payload

  process.stdout.write(`${uplinkMessage.received_at} - `)
  const message_bytes = uplinkMessage.decoded_payload.bytes
  console.log(message_bytes)

  // Figure out the date based on the timestamp
  //$ const serverDate = new Date('2023-02-28 04:00:33Z')
  const serverDate = new Date()
  const serverDow = serverDate.getUTCDay()
  const serverBeginningOfDay = new Date(serverDate).setUTCHours(0, 0, 0, 0)

  const clientDow = decoded_payload.timestamp >> 17
  const clientSecondsPastMidnight = decoded_payload.timestamp & 0x1FFFF
  let offsetDays = clientDow - serverDow
  if (offsetDays > 1) {
    offsetDays -= 7
  }
  console.log(`fport = ${fPort}, ts = ${decoded_payload.timestamp}, sDOW = ${serverDow}, cDOW = ${clientDow}, offsetDays = ${offsetDays}, clientSecondsPastMidnight = ${clientSecondsPastMidnight}`)

  const clientMessageDate = new Date(serverBeginningOfDay.valueOf() + (offsetDays * 86400000) + (clientSecondsPastMidnight * 1000))
  console.log(`clientMessageDate = ${clientMessageDate}`)

  process.stdout.write('raw bytes: ')
  for (i = 0; i < 4 + decoded_payload.contentLength; i++) {
    process.stdout.write(`${message_bytes[i]} `)
  }
  process.stdout.write('\n')

  switch (fPort) {
    case 1:
      return_payload = await handle_app_update(decoded_payload.type, message_bytes, clientMessageDate, decoded_payload)
      break

    case 222:
      return_payload = handle_time_sync(decoded_payload.type, message_bytes, serverDate)
      break

    default:
      console.log(`Unknown port: ${fPort}`)
      return_payload = DEFAULT_RETURN_PAYLOAD
      break
  }

  //$ How to make this async call wait, or do we really care???
  console.log(`Return payload = ${return_payload}`)
  request.post(`https://nam1.cloud.thethings.network/api/v3/as/applications/${config.applicationName}/devices/${config.deviceEui}/down/push`, {
    headers: {
      Authorization: `Bearer ${config.password}`,
      'Content-Type': 'application/json',
      'User-Agent': 'my-integration/my-integration-version'
    },
    json: true,
    body: {
      downlinks: [
        {
          frm_payload: return_payload,
          f_port: fPort,
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
