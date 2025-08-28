/**
 * server.js - realtime SSE versie
 * Lichtgewicht append-only + snapshot, Map-indexed
 * SSE push voor alle betrokken wallets
 */

const express = require("express");
const fs = require("fs");
const fsExtra = require("fs-extra");
const path = require("path");
const cors = require("cors");
const compression = require("compression");

const PORT = process.env.PORT || 3000;
const ADMIN_CODE = process.env.ADMIN_CODE || "change_me_admin_code";
const SNAPSHOT_OPS = parseInt(process.env.SNAPSHOT_OPS || "1000", 10);

const DATA_DIR = path.resolve(__dirname);
const SNAPSHOT_FILE = path.join(DATA_DIR, "snapshot.json");
const OPS_LOG_FILE = path.join(DATA_DIR, "ops.log");

// In-memory map
const users = new Map();

// Write queue
let writeQueue = Promise.resolve();
let pendingOps = [];

// SSE clients: Map user => Set(res)
const sseClients = new Map();

// Enqueue disk write in batch
function enqueueWrite(op) {
  pendingOps.push(op);
  if (pendingOps.length >= 20) flushOps();
}

function flushOps() {
  const opsToWrite = pendingOps.splice(0, pendingOps.length);
  writeQueue = writeQueue.then(() => {
    const data = opsToWrite.map(op => JSON.stringify(op)).join("\n") + "\n";
    return fs.promises.appendFile(OPS_LOG_FILE, data, "utf8");
  }).catch(err => console.error("Write queue error:", err));
  return writeQueue;
}

// Snapshot
async function snapshotToFile() {
  const tmp = SNAPSHOT_FILE + ".tmp";
  const plain = { users: [] };
  for (const [name, v] of users) {
    const entry = { name, balance: v.infinite ? "infinite" : v.balance.toString() };
    if (v.infinite) entry.infinite = true;
    plain.users.push(entry);
  }
  await fsExtra.writeJson(tmp, plain, { spaces: 2 });
  await fsExtra.move(tmp, SNAPSHOT_FILE, { overwrite: true });
  await fs.promises.truncate(OPS_LOG_FILE, 0);
  console.log("Snapshot written and ops.log truncated.");
}

// Apply op in memory and notify SSE
function applyOp(op, persist = true) {
  const { type } = op;
  let affected = [];

  if (type === "create") {
    if (!users.has(op.user)) users.set(op.user, { balance: 0, infinite: !!op.infinite });
    affected.push(op.user);
  } else if (type === "delete") {
    if (op.user && users.has(op.user) && op.user !== "Bank") {
      users.delete(op.user);
      affected.push(op.user);
    }
  } else if (type === "inc") {
    const u = users.get(op.user);
    if (!u) return;
    if (!u.infinite) u.balance += Number(op.amount);
    affected.push(op.user);
  } else if (type === "set") {
    const u = users.get(op.user);
    if (u) u.balance = Number(op.balance);
    affected.push(op.user);
  }

  if (persist) enqueueWrite(op);

  // Notify SSE clients
  for (const user of affected) {
    if (sseClients.has(user)) {
      for (const res of sseClients.get(user)) {
        res.write(`data: ${JSON.stringify({ ...op, newBalance: users.get(user)?.balance || 0 })}\n\n`);
      }
    }
  }
}

// Load data
async function loadData() {
  if (!fs.existsSync(SNAPSHOT_FILE)) await fsExtra.writeJson(SNAPSHOT_FILE, { users: [] });
  if (!fs.existsSync(OPS_LOG_FILE)) fs.writeFileSync(OPS_LOG_FILE, "");
  const snapshot = await fsExtra.readJson(SNAPSHOT_FILE);
  if (snapshot?.users) snapshot.users.forEach(u => users.set(u.name, { balance: Number(u.balance || 0), infinite: !!u.infinite }));
  const lines = fs.readFileSync(OPS_LOG_FILE, "utf8").split("\n");
  for (const line of lines) {
    if (!line.trim()) continue;
    try { applyOp(JSON.parse(line), false); } catch(e) {}
  }
  if (!users.has("Bank")) {
    users.set("Bank", { balance: 0, infinite: true });
    enqueueWrite({ type: "create", user: "Bank", infinite: true });
  } else users.get("Bank").infinite = true;
  console.log(`Data loaded. Users in memory: ${users.size}`);
}

// Express
const app = express();
app.use(cors());
app.use(compression());
app.disable("x-powered-by");

// Endpoints
app.get("/ping", (req,res)=>res.send("pong"));
app.get("/health", (req,res)=>res.json({ ok:true, users: users.size }));
app.get("/exists", (req,res)=>res.json({ exists: users.has(req.query.user) }));

app.get("/createWallet", (req,res)=>{
  const user = req.query.user;
  if (!user) return res.status(400).send("User required");
  if (users.has(user)) return res.status(400).send("Exists");
  applyOp({ type:"create", user });
  res.json({ name:user, balance:users.get(user).balance });
});

app.get("/balance", (req,res)=>{
  const user = req.query.user;
  if (!user) return res.status(400).send("User required");
  const u = users.get(user);
  if (!u) return res.status(404).send("Not found");
  res.json({ balance: u.infinite ? "infinite" : u.balance.toString() });
});

app.get("/deposit", (req,res)=>{
  const user = req.query.user;
  const amt = BigInt(req.query.amount || "0");
  if (!user || amt <= 0n) return res.status(400).send("Invalid");
  if (!users.has(user)) return res.status(404).send("User not found");
  applyOp({ type:"inc", user, amount: amt.toString() });
  res.json({ user, newBalance: users.get(user).balance.toString() });
});

app.get("/transfer", (req,res)=>{
  const { from, to } = req.query;
  const amt = BigInt(req.query.amount || "0");
  if (!from || !to || amt <= 0n) return res.status(400).send("Invalid");
  const s = users.get(from), r = users.get(to);
  if (!s||!r) return res.status(404).send("User not found");
  if (!s.infinite && BigInt(s.balance) < amt) return res.status(400).send("Insufficient funds");
  if (!s.infinite) applyOp({ type:"inc", user:from, amount:(-amt).toString() });
  applyOp({ type:"inc", user:to, amount:amt.toString() });
  res.json({
    from: { name:from, balance: s.infinite?"infinite":s.balance.toString() },
    to: { name:to, balance: r.balance.toString() }
  });
});

app.get("/deleteAccount", (req,res)=>{
  const user = req.query.user;
  if (!user) return res.status(400).send("User required");
  if (user==="Bank") return res.status(400).send("Cannot delete Bank");
  if (!users.has(user)) return res.status(404).send("User not found");
  applyOp({ type:"delete", user });
  res.send(`Deleted ${user}`);
});

app.get("/deleteAll", (req,res)=>{
  const code = req.query.code;
  if (!code) return res.status(400).send("Admin code required");
  if (code!==ADMIN_CODE) return res.status(403).send("Invalid code");
  for(const k of Array.from(users.keys())) if(k!=="Bank") applyOp({type:"delete", user:k});
  snapshotToFile();
  res.send("All user accounts deleted (Bank preserved)");
});

// SSE endpoint
app.get("/events", (req,res)=>{
  const user = req.query.user;
  if (!user) return res.status(400).send("User required");
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
  });
  res.write("\n");

  if(!sseClients.has(user)) sseClients.set(user,new Set());
  sseClients.get(user).add(res);

  req.on("close", ()=>{
    if(sseClients.has(user)) sseClients.get(user).delete(res);
  });
});

// Init
(async()=>{
  await loadData();
  app.listen(PORT, ()=>console.log(`Server listening on ${PORT}, Admin: ${ADMIN_CODE}`));
})();

// Graceful shutdown
process.on("SIGINT", async ()=>{
  console.log("SIGINT: flushing snapshot...");
  try { await writeQueue; await snapshotToFile(); } catch(e){console.error(e);}
  process.exit(0);
});
