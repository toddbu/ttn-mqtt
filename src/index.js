const mqtt = require('mqtt')
const client = mqtt.connect('mqtts://nam1.cloud.thethings.network:8883', {
  username: 'toddbu-temperature@ttn',
  password: 'NNSXS.OGAANBBTJQZCAG7OL6RVKB4LGXMY4EXATQUNMUQ.7YYUJILVXUKXISLEUWSDROPA45L52HQK3T4VJSUCTB53EJGOGDDA'
})

client.on('connect', () => {
  client.subscribe('v3/toddbu-temperature@ttn/devices/pico-98-76-b6-12-04-38/up', (err) => {
    if (err) {
      console.log(err)

      return
    }

    //$ client.publish('presence', 'Hello mqtt')
  })
})

client.on('message', (topic, message) => {
  // message is Buffer
  console.log(new Date())
  console.log(message.toString())
  console.log()
  //$ client.end()
})
