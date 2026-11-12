// 大肥鱼的家 —— 联机层（P2P）
//
// 默认私密：每个人有自己的房间码，陌生人不进来。
// 点「开放房间」把房间发布到大厅，别人在大厅看到即可一键进入；
// 也可以直接把房间码发给朋友，对方输入后加入。
// 位置数据走 WebRTC P2P 直连，公共 MQTT broker 只负责找人和协商（SDP）。
//
// 【隔离边界 · 硬规则】
// 基础的文字沟通（聊天、打招呼）自由互通：只当纯文本渲染，绝不执行、绝不当命令、绝不改本地状态。
// 渲染层只有：远端鱼的位置/朝向/名字、聊天气泡、表情气泡。
// 任何消息都不允许触发本地的游戏逻辑、动作、命令、存档或状态修改。
// 动作类互动（邀请对方做事）默认关闭，且每次都要本人点同意。
import * as THREE from './vendor/three.module.js'
import { joinRoom, selfId as netSelfId } from './trystero.mqtt.bundle.mjs'

const params = new URLSearchParams(location.search)
const APP_ID = ((params.get('app') || 'dafeiyu-room').trim() || 'dafeiyu-room').slice(0, 64)
const NAME = ((params.get('name') || '大肥鱼').trim() || '大肥鱼').slice(0, 20)
const FISH = ((params.get('fish') || '').trim() || `fish_${Math.random().toString(36).slice(2, 8)}`).slice(0, 40)

const SEND_MS = 120
const MAX_REMOTE = 6
const STALE_MS = 8000
const DIRECTORY_TTL = 12000
const ROOM_KEY = 'dafeiyu-room:room'

function localRoomId() {
  try {
    const saved = localStorage.getItem(ROOM_KEY)
    if (saved && /^[a-z0-9_-]{3,40}$/i.test(saved)) return saved
  } catch { /* ignore */ }
  const created = 'u-' + Math.random().toString(36).slice(2, 10)
  try { localStorage.setItem(ROOM_KEY, created) } catch { /* ignore */ }
  return created
}

let currentRoom = ((params.get('room') || '').trim() || localRoomId()).slice(0, 48)

let scene = null
let camera = null
let selfPos = [6.3, 5.1]
let selfFacing = 0
let selfWalking = false
let lastSend = 0
let sentCount = 0
let recvCount = 0

// 交互开关：默认关闭。开启后收到的邀请仍需逐次点同意。
let allowInteract = false

let ModelActorCls = null
let loading = null

const remotes = new Map() // peerId -> { name, actor, lastSeen, pos, facing, walking, ready }
const inbox = new Map()   // peerId -> { data, at }

// 大厅：谁开放了房间
const directory = new Map() // roomId -> { room, name, ts }
let isPublic = false
let lastAnnounce = 0

// ── 连接 ─────────────────────────────────────────────────────────────
// 公共 MQTT broker：broker.hivemq.com 实测可连，其余兜底
const RELAYS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt'
]

// 互动白名单：只有这几个"打招呼"类动作，没有移动/命令/本地逻辑
const INTERACT_ACTS = new Set(['wave', 'hello', 'party'])
const ACT_EMOJI = { wave: '👋', hello: '👋', party: '🎉' }

// 网络输入白名单：只允许这些字段进渲染层，数值一律 clamp。
// 任何网络消息都不能触发本地动作、命令或状态修改。
function sanitizePos(d) {
  if (!d || typeof d !== 'object') return null
  const x = Number(d.x)
  const z = Number(d.z)
  if (!Number.isFinite(x) || !Number.isFinite(z)) return null
  return {
    x: Math.max(-120, Math.min(120, x)),
    z: Math.max(-120, Math.min(120, z)),
    n: String(d.n || '大肥鱼').slice(0, 20),
    fa: Number.isFinite(Number(d.fa)) ? Number(d.fa) : 0,
    w: !!d.w
  }
}

let room = null
let sendPos = null
let askSend = null
let replySend = null
let chatSend = null
let roomGen = 0

function teardownRoom() {
  roomGen++
  if (room) {
    try { room.leave() } catch { /* ignore */ }
    room = null
  }
  sendPos = null
  askSend = null
  replySend = null
  chatSend = null
  for (const pid of [...remotes.keys()]) dropRemote(pid)
  inbox.clear()
}

function buildRoom() {
  roomGen++
  const gen = roomGen
  const r = joinRoom({ appId: APP_ID, relayUrls: RELAYS }, currentRoom)
  const [send, on] = r.makeAction('pos')
  sendPos = send
  r.onPeerJoin((pid) => {
    if (gen === roomGen) console.log('[net] peer join', pid)
  })
  r.onPeerLeave((pid) => {
    if (gen === roomGen) {
      console.log('[net] peer leave', pid)
      dropRemote(pid)
    }
  })
  on((data, pid) => {
    if (gen !== roomGen) return
    const clean = sanitizePos(data)
    if (!clean) return
    recvCount++
    inbox.set(pid, { data: clean, at: performance.now() })
  })

  // 打招呼邀请：默认拒绝；只有开启"允许朋友互动"并本人点同意才回 ok，
  // 且对方收到 ok 后只在自己屏幕上冒个气泡，不触发任何本地行为。
  const [sendAsk, onAsk] = r.makeAction('ask')
  const [sendReply, onReply] = r.makeAction('reply')
  askSend = sendAsk
  replySend = sendReply
  onAsk((data, pid) => {
    if (gen !== roomGen) return
    if (!data || typeof data.act !== 'string' || !INTERACT_ACTS.has(data.act)) return
    if (!allowInteract) {
      try { sendReply({ ok: false, act: data.act, to: pid })?.catch?.(() => {}) } catch { /* ignore */ }
      return
    }
    showAskBar(pid, data.act, String(data.name || '同伴').slice(0, 20))
  })
  onReply((data, pid) => {
    if (gen !== roomGen) return
    if (!data || data.ok !== true || !INTERACT_ACTS.has(data.act)) return
    bubbleFor(pid, ACT_EMOJI[data.act] || '👋', true)
  })

  // 基础聊天：只当纯文本气泡渲染，不执行、不当命令、不碰本地状态。
  const [sendChat, onChat] = r.makeAction('chat')
  chatSend = sendChat
  onChat((data, pid) => {
    if (gen !== roomGen) return
    if (!data || typeof data.msg !== 'string') return
    const text = data.msg.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 140)
    if (!text) return
    bubbleFor(pid, text, false)
  })
  room = r
}

function enterRoom(id) {
  const next = String(id || '').trim().slice(0, 48)
  if (!next || next === currentRoom) return
  teardownRoom()
  currentRoom = next
  buildRoom()
  lastAnnounce = 0
}

buildRoom()

// ── 大厅（所有人共用） ──────────────────────────────────────────────
const dirRoom = joinRoom({ appId: 'dafeiyu-room', relayUrls: RELAYS }, 'lobby-directory')
const [sendAnnounce, onAnnounce] = dirRoom.makeAction('ann')
const [sendGone, onGone] = dirRoom.makeAction('gone')

onAnnounce((data, id) => {
  if (!data || id === netSelfId || typeof data.room !== 'string' || !data.room) return
  if (data.room === currentRoom) return
  directory.set(data.room, {
    room: data.room.slice(0, 48),
    name: String(data.name || '大肥鱼的家').slice(0, 20),
    ts: performance.now()
  })
})
onGone((data) => {
  if (data && typeof data.room === 'string') directory.delete(data.room)
})

function announce() {
  if (!isPublic) return
  try {
    sendAnnounce({ room: currentRoom, name: NAME })?.catch?.(() => {})
  } catch { /* ignore */ }
}

function setPublic(v) {
  if (isPublic === v) return
  isPublic = v
  lastAnnounce = 0
  if (!v) {
    try { sendGone({ room: currentRoom })?.catch?.(() => {}) } catch { /* ignore */ }
  }
}

// ── 远端鱼的渲染 ─────────────────────────────────────────────────────
function ensureActorClass() {
  if (ModelActorCls) return Promise.resolve(ModelActorCls)
  if (loading) return loading
  loading = import('./model-actor.js').then((mod) => {
    ModelActorCls = mod?.ModelActor || null
    return ModelActorCls
  }).catch((err) => {
    console.warn('[net] 模型加载模块失败', err)
    return null
  }).finally(() => { loading = null })
  return loading
}

function makeLabel(text) {
  const canvas = document.createElement('canvas')
  canvas.width = 512
  canvas.height = 128
  const ctx = canvas.getContext('2d')
  ctx.clearRect(0, 0, 512, 128)
  ctx.fillStyle = 'rgba(10,16,24,.62)'
  const r = 34
  ctx.beginPath()
  ctx.moveTo(24 + r, 30)
  ctx.arcTo(488, 30, 488, 98, r)
  ctx.arcTo(488, 98, 24, 98, r)
  ctx.arcTo(24, 98, 24, 30, r)
  ctx.arcTo(24, 30, 488, 30, r)
  ctx.closePath()
  ctx.fill()
  ctx.font = 'bold 52px "PingFang SC","Microsoft YaHei",system-ui,sans-serif'
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.fillStyle = '#dbeafe'
  ctx.fillText(text, 256, 64)
  const tex = new THREE.CanvasTexture(canvas)
  tex.colorSpace = THREE.SRGBColorSpace
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, transparent: true, depthWrite: false }))
  sprite.scale.set(1.55, 0.39, 1)
  sprite.position.set(0, 1.95, 0)
  return sprite
}

function makeEmojiBubble(text) {
  const canvas = document.createElement('canvas')
  canvas.width = 256
  canvas.height = 256
  const ctx = canvas.getContext('2d')
  ctx.font = '150px "Apple Color Emoji","Segoe UI Emoji",serif'
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.fillText(text, 128, 140)
  const tex = new THREE.CanvasTexture(canvas)
  tex.colorSpace = THREE.SRGBColorSpace
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, transparent: true, depthWrite: false }))
  sprite.scale.set(1.0, 1.0, 1)
  sprite.position.set(0, 2.55, 0)
  return sprite
}

// 文字气泡：只画文字，不解析任何标记，绝不执行内容
function makeTextBubble(text) {
  const w = 720
  const h = 240
  const canvas = document.createElement('canvas')
  canvas.width = w
  canvas.height = h
  const ctx = canvas.getContext('2d')
  ctx.font = '34px "PingFang SC","Microsoft YaHei",system-ui,sans-serif'
  const lines = []
  let cur = ''
  for (const ch of text) {
    if (ctx.measureText(cur + ch).width > w - 90) { lines.push(cur); cur = ch } else cur += ch
  }
  if (cur) lines.push(cur)
  const shown = lines.slice(0, 2)
  const bh = 44 + shown.length * 46
  const bw = Math.min(w - 24, 90 + Math.max(0, ...shown.map((l) => ctx.measureText(l).width)))
  const bx = (w - bw) / 2
  const by = (h - bh) / 2
  ctx.fillStyle = 'rgba(10,16,24,.84)'
  ctx.beginPath()
  if (typeof ctx.roundRect === 'function') ctx.roundRect(bx, by, bw, bh, 22)
  else ctx.rect(bx, by, bw, bh)
  ctx.fill()
  ctx.fillStyle = '#e2e8f0'
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  shown.forEach((line, i) => ctx.fillText(line, w / 2, by + 40 + i * 46))
  const tex = new THREE.CanvasTexture(canvas)
  tex.colorSpace = THREE.SRGBColorSpace
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, transparent: true, depthWrite: false }))
  sprite.scale.set(2.8, 0.93, 1)
  sprite.position.set(0, 2.75, 0)
  return sprite
}

function bubbleFor(pid, text, isEmoji = true) {
  const rec = remotes.get(pid)
  if (!rec?.actor) return
  if (rec.bubble) {
    try {
      rec.actor.group.remove(rec.bubble)
      rec.bubble.material?.map?.dispose()
      rec.bubble.material?.dispose()
    } catch { /* ignore */ }
    rec.bubble = null
  }
  const b = isEmoji ? makeEmojiBubble(text) : makeTextBubble(text)
  try { rec.actor.group.add(b) } catch { return }
  rec.bubble = b
  clearTimeout(rec.bubbleTimer)
  rec.bubbleTimer = setTimeout(() => {
    try {
      rec.actor?.group.remove(b)
      b.material?.map?.dispose()
      b.material?.dispose()
    } catch { /* ignore */ }
    if (rec.bubble === b) rec.bubble = null
  }, 5000)
}

// ── 打招呼：对方必须本人在场点同意 ───────────────────────────────────
function invite(pid) {
  if (!askSend) return
  try { askSend({ act: 'wave', name: NAME })?.catch?.(() => {}) } catch { /* ignore */ }
}

// 聊天：只发纯文本，接收方只画文字气泡
function sendChatMsg(text) {
  if (!chatSend) return
  const msg = String(text).replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 140)
  if (!msg) return
  try { chatSend({ msg })?.catch?.(() => {}) } catch { /* ignore */ }
}

let askBar = null
function hideAskBar() {
  if (!askBar) return
  try { askBar.remove() } catch { /* ignore */ }
  askBar = null
}

function showAskBar(pid, act, name) {
  hideAskBar()
  const bar = document.createElement('div')
  bar.style.cssText = 'position:fixed;left:50%;bottom:96px;transform:translateX(-50%);z-index:99999;display:flex;gap:10px;align-items:center;font:13px/1.5 system-ui;color:#e2e8f0;background:rgba(10,16,24,.94);border:1px solid rgba(120,160,220,.35);border-radius:12px;padding:10px 14px;box-shadow:0 10px 30px rgba(0,0,0,.5)'
  bar.innerHTML = `<span><b>${esc(name)}</b> 想和你打招呼</span>`
  const okBtn = document.createElement('button')
  okBtn.type = 'button'
  okBtn.textContent = '同意'
  okBtn.style.cssText = BTN_CSS
  const noBtn = document.createElement('button')
  noBtn.type = 'button'
  noBtn.textContent = '拒绝'
  noBtn.style.cssText = BTN_CSS
  okBtn.addEventListener('click', () => {
    try { replySend?.({ ok: true, act, to: pid })?.catch?.(() => {}) } catch { /* ignore */ }
    bubbleFor(pid, ACT_EMOJI[act] || '👋')
    hideAskBar()
  })
  noBtn.addEventListener('click', () => {
    try { replySend?.({ ok: false, act, to: pid })?.catch?.(() => {}) } catch { /* ignore */ }
    hideAskBar()
  })
  bar.append(okBtn, noBtn)
  document.body.appendChild(bar)
  askBar = bar
}

async function spawnRemote(id, data) {
  const Cls = await ensureActorClass()
  if (!Cls || !scene) return
  const actor = new Cls(scene, { url: './assets/models/da-feiyu.opt.glb', quality: 'optimized' })
  const ok = await actor.load()
  if (!ok) {
    console.warn('[net] 远端模型加载失败', id)
    try { actor.dispose() } catch { /* ignore */ }
    return
  }
  try { actor.group.add(makeLabel(String(data.n || '大肥鱼').slice(0, 20))) } catch { /* ignore */ }
  const rec = remotes.get(id)
  if (!rec) {
    // 加载期间对方已经走了
    try { actor.dispose() } catch { /* ignore */ }
    return
  }
  rec.actor = actor
  rec.ready = true
  rec.actor.setVisible(true)
  rec.actor.setPose({ pos: [data.x, data.z], facing: data.fa || 0, walking: false })
}

function dropRemote(id) {
  const rec = remotes.get(id)
  if (!rec) return
  try { rec.actor?.dispose?.() } catch { /* ignore */ }
  remotes.delete(id)
}

// ── 每帧调用（app.js 的 frame 里） ───────────────────────────────────
export function tick(dt, info) {
  if (!info) return
  if (info.scene) scene = info.scene
  if (info.camera) camera = info.camera
  if (Array.isArray(info.pos) && info.pos.length === 2) {
    selfPos = info.pos
    selfFacing = Number(info.facing) || 0
    selfWalking = !!info.walking
  }

  const now = performance.now()
  if (now - lastSend >= SEND_MS) {
    lastSend = now
    try {
      sendPos({ f: FISH, n: NAME, x: selfPos[0], z: selfPos[1], fa: selfFacing, w: selfWalking })?.catch?.(() => {})
      sentCount++
    } catch { /* ignore */ }
  }

  // 开放中的房间：定期在大厅广播自己
  if (isPublic && now - lastAnnounce >= 4000) {
    lastAnnounce = now
    announce()
  }

  // 处理收到的位置
  for (const [id, item] of inbox) {
    inbox.delete(id)
    if (id === netSelfId) continue
    let rec = remotes.get(id)
    if (!rec) {
      if (remotes.size >= MAX_REMOTE) continue
      rec = { name: '', actor: null, lastSeen: item.at, path: [], ready: false }
      remotes.set(id, rec)
      void spawnRemote(id, item.data)
    }
    rec.lastSeen = item.at
    rec.name = String(item.data.n || '大肥鱼').slice(0, 20)
    rec.pos = [item.data.x, item.data.z]
    rec.facing = Number(item.data.fa) || 0
    rec.walking = !!item.data.w
    if (rec.ready && rec.actor) {
      // 和本地鱼/其他远端鱼几乎重叠时错开渲染，避免叠成一条看不出来
      const dx = rec.pos[0] - selfPos[0]
      const dz = rec.pos[1] - selfPos[1]
      let px = rec.pos[0]
      let pz = rec.pos[1]
      if (Math.hypot(dx, dz) < 0.8) {
        // 按 id 散开角度：多个同伴不会挤在同一个偏移点
        let h = 0
        for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) >>> 0
        const ang = ((h % 360) * Math.PI) / 180
        px += Math.cos(ang) * 0.9
        pz += Math.sin(ang) * 0.9
      }
      rec.actor.setPose({ pos: [px, pz], facing: rec.facing, walking: rec.walking })
    }
  }

  // 更新远端鱼 + 清理超时
  for (const [id, rec] of remotes) {
    if (!rec.ready || !rec.actor) continue
    if (now - rec.lastSeen > STALE_MS) { dropRemote(id); continue }
    try {
      rec.actor.update(dt || 0.016, camera || { position: { x: selfPos[0], z: selfPos[1] } }, { mode: 'overview', lookAtCamera: false })
    } catch { /* ignore */ }
  }

  updateBadge(now)
  updatePanel(now)
}

// ── 左下角徽章 ───────────────────────────────────────────────────────
let badge = null
let badgeAt = 0
function updateBadge(now) {
  if (!badge) {
    badge = document.createElement('div')
    badge.style.cssText = 'position:fixed;left:12px;bottom:12px;z-index:9999;font:12px/1.4 system-ui;color:#cfe6ff;background:rgba(10,16,24,.72);padding:4px 10px;border-radius:999px;pointer-events:none'
    document.body.appendChild(badge)
  }
  if (now - badgeAt < 500) return
  badgeAt = now
  const n = [...remotes.values()].filter((r) => r.ready && r.actor).length
  if (n > 0) {
    badge.textContent = `🟢 ${currentRoom} · 同伴 ${n} 条`
  } else if (isPublic) {
    badge.textContent = `🟡 ${currentRoom} · 已开放，等同伴`
  } else {
    badge.textContent = `⚪ ${currentRoom} · 私密房间`
  }
}

// ── 联机面板 ─────────────────────────────────────────────────────────
const TOGGLE_CSS = 'position:fixed;left:12px;bottom:44px;z-index:9999;font:12px/1.4 system-ui;color:#cfe6ff;background:rgba(10,16,24,.72);padding:4px 10px;border-radius:999px;border:1px solid rgba(120,160,220,.25);cursor:pointer'
const PANEL_CSS = 'position:fixed;left:12px;bottom:44px;z-index:9999;width:252px;font:12px/1.5 system-ui;color:#cfe6ff;background:rgba(10,16,24,.92);border:1px solid rgba(120,160,220,.25);border-radius:12px;padding:10px 12px;display:none;box-shadow:0 10px 30px rgba(0,0,0,.5)'
const BTN_CSS = 'padding:3px 8px;border-radius:8px;border:1px solid rgba(120,160,220,.35);background:rgba(30,41,59,.9);color:#cfe6ff;cursor:pointer;font:inherit'
const INPUT_CSS = 'width:100%;box-sizing:border-box;padding:3px 8px;border-radius:8px;border:1px solid rgba(120,160,220,.35);background:rgba(15,23,42,.9);color:#e2e8f0;font:inherit;outline:none'

let panelEl = null
let toggleEl = null
let listEl = null
let peerListEl = null
let codeEl = null
let publicBtnEl = null
let joinInputEl = null
let panelAt = 0

function esc(v) {
  return String(v).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]))
}

function buildPanelUI() {
  if (panelEl) return
  toggleEl = document.createElement('button')
  toggleEl.type = 'button'
  toggleEl.textContent = '🌐 联机'
  toggleEl.style.cssText = TOGGLE_CSS
  toggleEl.addEventListener('click', () => {
    panelEl.style.display = 'block'
    toggleEl.style.display = 'none'
    renderPanel()
  })
  document.body.appendChild(toggleEl)

  panelEl = document.createElement('div')
  panelEl.style.cssText = PANEL_CSS
  panelEl.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
      <b>🌐 联机</b>
      <span id="net-close" style="cursor:pointer;opacity:.7">✕</span>
    </div>
    <div style="margin-bottom:6px;opacity:.8">房间码 <code id="net-code" style="color:#7dd3fc"></code></div>
    <div style="display:flex;gap:6px;margin-bottom:8px">
      <button id="net-public" type="button" style="${BTN_CSS};flex:1"></button>
      <button id="net-copy" type="button" style="${BTN_CSS}">复制</button>
    </div>
    <div style="display:flex;gap:6px;margin-bottom:8px">
      <input id="net-join-input" placeholder="输入房间码" style="${INPUT_CSS}" />
      <button id="net-join" type="button" style="${BTN_CSS}">加入</button>
    </div>
    <div style="margin-bottom:6px;opacity:.8">房间里</div>
    <div id="net-peers" style="margin-bottom:8px"></div>
    <div style="margin-bottom:6px;opacity:.8">大厅</div>
    <div id="net-list" style="max-height:110px;overflow:auto"></div>
    <div style="display:flex;gap:6px;margin:8px 0">
      <input id="net-chat" placeholder="说一句（140 字内）" maxlength="140" style="${INPUT_CSS}" />
      <button id="net-chat-send" type="button" style="${BTN_CSS}">发</button>
    </div>
    <label style="display:flex;gap:6px;align-items:center;margin:10px 0 8px;cursor:pointer;opacity:.85">
      <input id="net-allow" type="checkbox" style="margin:0" /> 允许朋友互动（默认关，仍需每次同意）
    </label>
    <div style="margin-top:2px">
      <button id="net-home" type="button" style="${BTN_CSS};width:100%">回我的房间</button>
    </div>
  `
  document.body.appendChild(panelEl)

  panelEl.querySelector('#net-close').addEventListener('click', () => {
    panelEl.style.display = 'none'
    toggleEl.style.display = 'inline-block'
  })
  codeEl = panelEl.querySelector('#net-code')
  peerListEl = panelEl.querySelector('#net-peers')
  const allowEl = panelEl.querySelector('#net-allow')
  allowEl.checked = allowInteract
  allowEl.addEventListener('change', () => { allowInteract = !!allowEl.checked })
  panelEl.addEventListener('click', (e) => {
    const wave = e.target.closest ? e.target.closest('button[data-wave]') : null
    if (wave) invite(wave.dataset.wave)
  })
  const chatInput = panelEl.querySelector('#net-chat')
  const doChat = () => {
    const v = String(chatInput.value || '').trim()
    if (!v) return
    sendChatMsg(v)
    chatInput.value = ''
  }
  panelEl.querySelector('#net-chat-send').addEventListener('click', doChat)
  chatInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); doChat() }
  })
  publicBtnEl = panelEl.querySelector('#net-public')
  publicBtnEl.addEventListener('click', () => {
    setPublic(!isPublic)
    renderPanel()
  })
  panelEl.querySelector('#net-copy').addEventListener('click', () => {
    try { navigator.clipboard?.writeText(currentRoom) } catch { /* ignore */ }
  })
  joinInputEl = panelEl.querySelector('#net-join-input')
  panelEl.querySelector('#net-join').addEventListener('click', () => {
    let v = ''
    try { v = joinInputEl.value.trim() } catch { /* ignore */ }
    if (!v) return
    enterRoom(v)
    try { joinInputEl.value = '' } catch { /* ignore */ }
    renderPanel()
  })
  panelEl.querySelector('#net-home').addEventListener('click', () => {
    enterRoom(localRoomId())
    renderPanel()
  })
  listEl = panelEl.querySelector('#net-list')
  listEl.addEventListener('click', (e) => {
    const btn = e.target.closest ? e.target.closest('button[data-join]') : null
    if (!btn) return
    enterRoom(btn.dataset.join)
    renderPanel()
  })
}

function renderPanel() {
  if (!panelEl || panelEl.style.display === 'none') return
  codeEl.textContent = currentRoom
  publicBtnEl.textContent = isPublic ? '关闭开放' : '开放房间'
  publicBtnEl.style.background = isPublic ? 'rgba(56,189,248,.28)' : 'rgba(30,41,59,.9)'

  // 房间里的同伴：每个可以发一句打招呼邀请（对方同意才会冒泡）
  const peers = [...remotes.entries()].filter(([, r]) => r.ready && r.actor)
  if (peerListEl) {
    peerListEl.innerHTML = peers.length
      ? peers.map(([pid, r]) =>
          `<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;padding:2px 0">
            <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.name || '同伴')}</span>
            <button type="button" data-wave="${esc(pid)}" style="${BTN_CSS};flex:none">👋</button>
          </div>`).join('')
      : '<div style="opacity:.55">还没有同伴</div>'
  }

  const list = [...directory.values()].filter((v) => v.room !== currentRoom)
  if (!list.length) {
    listEl.innerHTML = '<div style="opacity:.55">还没有人开放房间</div>'
    return
  }
  listEl.innerHTML = list.map((v) =>
    `<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;padding:2px 0">
      <span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(v.name)}</span>
      <button type="button" data-join="${esc(v.room)}" style="${BTN_CSS};flex:none">进入</button>
    </div>`
  ).join('')
}

function updatePanel(now) {
  buildPanelUI()
  if (!panelEl || panelEl.style.display === 'none') return
  if (now - panelAt < 500) return
  panelAt = now
  // 清理超时的大厅条目
  for (const [k, v] of directory) {
    if (now - v.ts > DIRECTORY_TTL) directory.delete(k)
  }
  renderPanel()
}

// ── 调试/状态 ─────────────────────────────────────────────────────────
export function status() {
  const list = [...remotes.values()]
  return {
    room: currentRoom,
    selfId: netSelfId,
    fish: FISH,
    name: NAME,
    isPublic,
    peers: remotes.size,
    rendered: list.filter((r) => r.ready && r.actor).length,
    sent: sentCount,
    recv: recvCount,
    hall: directory.size,
    scene: !!scene
  }
}

window.__roomNetDebug = () => status()
