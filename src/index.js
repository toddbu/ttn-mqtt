const mqtt = require('mqtt')
const client = mqtt.connect('mqtts://nam1.cloud.thethings.network:8883', {
  username: 'toddbu-temperature@ttn',
  password: 'NNSXS.OGAANBBTJQZCAG7OL6RVKB4LGXMY4EXATQUNMUQ.7YYUJILVXUKXISLEUWSDROPA45L52HQK3T4VJSUCTB53EJGOGDDA'
})

client.on('connect', () => {
  client.subscribe('v3/toddbu-temperature@ttn/devices/eui-9876b60000120438/up', (err) => {
    if (err) {
      console.log(err)

      return
    }

    //$ client.publish('presence', 'Hello mqtt')
  })
})

client.on('message', (topic, message) => {
  // message is Buffer
  //$ console.log(new Date())
  const uplinkMessage = JSON.parse(message.toString()).uplink_message
  console.log(`${uplinkMessage.received_at} - ${uplinkMessage.decoded_payload.temp}F`)
  //$ console.log()
  //$ client.end()
})
