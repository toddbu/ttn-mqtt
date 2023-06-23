const process = require('process')
const mqtt = require('mqtt')
const request = require('request')
const cassandra = require('cassandra-driver')
const config = require('./config')(process.env.ENV)

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

async function updateBusylight(params) {
  const { r, g, b, on, off } = params

  await new Promise((resolve, reject) => {
    request.post(`https://nam1.cloud.thethings.network/api/v3/as/applications/${config.applicationName}/devices/busylight/down/replace`, {
      headers: {
        Authorization: `Bearer ${config.password}`,
        'Content-Type': 'application/json',
        'User-Agent': 'my-integration/my-integration-version'
      },
      json: true,
      body: {
        downlinks: [
          {
            frm_payload: Buffer.from([r, b, g, on, off]).toString('base64'),
            f_port: 15,
            priority: 'NORMAL'
          }
        ]
      }
    }, (err, response, body) => {
      if (err) {
        console.log(err)
        return resolve()
      }

      if (response && (response.statusCode !== 200)) {
        console.log('statusCode:', response.statusCode)
      }

      resolve()
    })
  })
}

function handleTimeSync(messageType, contentBytes, serverDate) {
  let returnPayload = null

  switch (messageType) {
    case 0:
      const payloadBytes = Buffer.from(contentBytes)

      console.log(`datetime request`)

      // Check to see if we get a NOP. If we did then send back a packet with no
      // adjustments since we're just trying to force a download
      if (contentBytes[0] +
          contentBytes[1] +
          contentBytes[2] +
          contentBytes[3] +
          contentBytes[4] +
          contentBytes[5] +
          contentBytes[6] === 0) {
        for (let i = 0; i < 7; i++) {
          payloadBytes[i] = 128
        }
        returnPayload = payloadBytes
        console.log('NOP', returnPayload, payloadBytes)
        break
      }

      const clientDate = new Date(`${formatDateComponent(contentBytes[0])}${formatDateComponent(contentBytes[1])}-${formatDateComponent(contentBytes[2])}-${formatDateComponent(contentBytes[3])} ${formatDateComponent(contentBytes[4])}:${formatDateComponent(contentBytes[5])}:${formatDateComponent(contentBytes[6])}Z`)

      // The Pico is pretty limited in datetime processing capabillites,
      // so we'll calcullate the RTC offsets here. Also, because we can
      // only send unsigned bytes then we'll add 128 to the calculated
      // value so that we can have negative numbers
      payloadBytes[0] = parseInt((serverDate.getFullYear() - clientDate.getFullYear()) / 100) + 128
      payloadBytes[1] = parseInt((serverDate.getFullYear() - clientDate.getFullYear()) % 100) + 128
      payloadBytes[2] = serverDate.getMonth() - clientDate.getMonth() + 128
      payloadBytes[3] = serverDate.getDate() - clientDate.getDate() + 128
      payloadBytes[4] = serverDate.getHours() - clientDate.getHours() + 128
      payloadBytes[5] = serverDate.getMinutes() - clientDate.getMinutes() + 128
      payloadBytes[6] = serverDate.getSeconds() - clientDate.getSeconds() + 128
      returnPayload = payloadBytes
      console.log(clientDate, returnPayload, payloadBytes)
      break

    default:
      console.log(`unknown message type: ${messageType}`)
      break
  }

  return returnPayload
}

async function handleAppUpdate(messageType, contentBytes, clientMessageDate, decodedPayload) {
  let returnPayload = null

  switch (messageType) {
    case 1:
      console.log(`temperature request: ${decodedPayload.temp}F`)
      const query = 'INSERT INTO temperature.temperature (station_id, received_at, temperature_in_f) VALUES(?, ?, ?)'
      const params = [
        1,
        clientMessageDate,
        decodedPayload.temp
      ]

      await cassandraClient.execute(query, params, { prepare: true });
      //$ returnPayload = Buffer.from([1])
      break

    case 2:
      console.log(`upper door: ${contentBytes[0] === 0 ? 'open' : 'closed'}`)
      if (contentBytes[0] === 0) {
        await updateBusylight({
          r: 0,
          g: 255,
          b: 0,
          on: 255,
          off: 0
        })
      }
      break

    case 3:
      console.log(`lower door: ${contentBytes[0] === 0 ? 'open' : 'closed'}`)
      if (contentBytes[0] === 0) {
        await updateBusylight({
          r: 0,
          g: 0,
          b: 255,
          on: 255,
          off: 0
        })
      }
      break

    default:
      console.log(`unknown message type: ${messageType}`)
      break
  }

  return returnPayload
}

mqttClient.on('message', async (topic, message) => {
  let returnPayload

  console.log('---')

  // message is a Buffer type
  const uplinkMessage = JSON.parse(message.toString()).uplink_message
  const fPort = uplinkMessage.f_port
  if (typeof fPort === 'undefined') {
    // Toss the message if we don't have an fPort
    return
  }

  const decodedPayload = uplinkMessage.decoded_payload

  process.stdout.write(`${uplinkMessage.received_at} - `)
  const allBytes = decodedPayload.bytes
  console.log(allBytes)

  const headerBytes = Buffer.from(allBytes.slice(0, 4))
  const contentBytes = allBytes.slice(4)
  process.stdout.write('raw bytes: ')
  for (i = 0; i < 4 + decodedPayload.contentLength; i++) {
    process.stdout.write(`${allBytes[i]} `)
  }
  process.stdout.write('\n')

  // Figure out the date based on the timestamp
  const serverDate = new Date()
  const serverDow = serverDate.getUTCDay()
  const serverBeginningOfDay = new Date(serverDate).setUTCHours(0, 0, 0, 0)

  const clientDow = decodedPayload.timestamp >> 17
  const clientSecondsPastMidnight = decodedPayload.timestamp & 0x1FFFF
  let offsetDays = clientDow - serverDow
  if (offsetDays > 1) {
    offsetDays -= 7
  }
  console.log(`fport = ${fPort}, ts = ${decodedPayload.timestamp}, sDOW = ${serverDow}, cDOW = ${clientDow}, offsetDays = ${offsetDays}, clientSecondsPastMidnight = ${clientSecondsPastMidnight}`)

  const clientMessageDate = new Date(serverBeginningOfDay.valueOf() + (offsetDays * 86400000) + (clientSecondsPastMidnight * 1000))
  console.log(`clientMessageDate = ${clientMessageDate}`)

  switch (fPort) {
    case 1:
      returnPayload = await handleAppUpdate(decodedPayload.type, contentBytes, clientMessageDate, decodedPayload)
      break

    case 222:
      returnPayload = handleTimeSync(decodedPayload.type, contentBytes, serverDate)
      break

    default:
      console.log(`Unknown port: ${fPort}`)
      returnPayload = null
      break
  }

  console.log(`Return payload = ${ returnPayload ? [...returnPayload] : 'null' }`)
  if (returnPayload) {
    await new Promise((resolve, reject) => {
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
              frm_payload: (returnPayload ? Buffer.concat([headerBytes, returnPayload]) : headerBytes).toString('base64'),
              f_port: fPort,
              priority: 'NORMAL'
            }
          ]
        }
      }, (err, response, body) => {
        if (err) {
          console.log(err)
          return resolve()
        }

        if (response && (response.statusCode !== 200)) {
          console.log('statusCode:', response.statusCode)
        }

        resolve()
      })
    })
  }

  //$ mqttClient.end()
})
