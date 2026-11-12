import * as polyfill from 'node-datachannel/polyfill'

globalThis.RTCPeerConnection = polyfill.RTCPeerConnection
globalThis.RTCSessionDescription = polyfill.RTCSessionDescription
globalThis.RTCIceCandidate = polyfill.RTCIceCandidate

const { joinRoom } = await import('trystero')

const name = process.argv[2] || 'peer'
const roomId = process.argv[3] || 'dafeiyu-test-1'

const room = joinRoom({ appId: 'dafeiyu-room-p2p' }, roomId)
const peers = new Set()

room.onPeerJoin((id) => {
  peers.add(id)
  console.log(`[${name}] PEER JOIN ${id}`)
})
room.onPeerLeave((id) => {
  peers.delete(id)
  console.log(`[${name}] PEER LEAVE ${id}`)
})

const [sendPos, onPos] = room.makeAction('pos')
onPos((data, id) => {
  console.log(`[${name}] POS from ${id}: ${JSON.stringify(data)}`)
})

let n = 0
setInterval(() => {
  n++
  sendPos({ x: n, y: Math.sin(n / 5), name, at: Date.now() }).catch?.(() => {})
}, 2000)

setTimeout(() => {
  console.log(`[${name}] DONE peers=${peers.size}`)
  process.exit(peers.size > 0 ? 0 : 3)
}, 30000)
