/**
 * server.js - geoptimaliseerd, zonder cluster
 * Lichtgewicht wallet API
 * Append-only log + snapshot, Map-indexed, /exists endpoint
 * Batching, compressie, CORS, consistente in-memory data
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

// In-memory users map: key=username, value={balance:number,infinite?:true}
const users = new Map();

// Write queue and batching
let writeQueue = Promise.resolve();
let pendingOps = [];

// Enqueue an operation
function enqueueWrite(op) {
  pendingOps.push(op);
  if (pendingOps.length >= 20) flushOps();
}

// Flush pending operations to disk
function flushOps() {
  const opsToWrite = pendingOps.splice(0, pendingOps.length);
  writeQueue = writeQueue.then(() => {
    const data = opsToWrite.map(op => JSON.stringify(op)).join("\n") + "\n";
    return fs.promises.appendFile(OPS_LOG_FILE, data, "utf8")
      .then(() => {
        if (opsSinceSnapshot() >= SNAPSHOT_OPS) scheduleSnapshot();
      });
  }).catch(err => console.error("Write queue error:", err));
  return writeQueue;
}

// Count ops since last snapshot
function opsSinceSnapshot() {
  return pendingOps.length;
}

// Async snapshot (atomic)
async function snapshotToFile() {
  const tmp = SNAPSHOT_FILE + ".tmp";
  const plain = { users: [] };
  for (const [name, v] of users) {
    const entry = { name, balance: v.infinite ? 0 : v.balance };
    if (v.infinite) entry.infinite = true;
    plain.users.push(entry);
  }
  await fsExtra.writeJson(tmp, plain, { spaces: 2 });
  await fsExtra.move(tmp, SNAPSHOT_FILE, { overwrite: true });
  await fs.promises.truncate(OPS_LOG_FILE, 0);
  console.log("Snapshot written and ops.log truncated.");
}

// Schedule snapshot
function scheduleSnapshot() {
  setImmediate(snapshotToFile).catch(e => console.error(e));
}

// Apply operation in-memory
function applyOp(op, persist = true) {
  const { type } = op;
  if (type === "create") {
    if (!users.has(op.user)) users.set(op.user, { balance: 0, infinite: !!op.infinite });
  } else if (type === "delete") {
    if (op.user && users.has(op.user) && op.user !== "Bank") users.delete(op.user);
  } else if (type === "inc") {
    const u = users.get(op.user);
    if (!u) return;
    if (!u.infinite) u.balance += Number(op.amount);
  } else if (type === "set") {
    const u = users.get(op.user);
    if (u) u.balance = Number(op.balance);
  }
  if (persist) enqueueWrite(op);
}

// Load snapshot + ops.log
async function loadData() {
  if (!fs.existsSync(SNAPSHOT_FILE)) await fsExtra.writeJson(SNAPSHOT_FILE, { users: [] });
  if (!fs.existsSync(OPS_LOG_FILE)) fs.writeFileSync(OPS_LOG_FILE, "");

  // Load snapshot
  const snapshot = await fsExtra.readJson(SNAPSHOT_FILE);
  if (snapshot?.users) {
    snapshot.users.forEach(u => users.set(u.name, { balance: u.balance || 0, infinite: !!u.infinite }));
  }

  // Replay ops.log
  const lines = fs.readFileSync(OPS_LOG_FILE, "utf8").split("\n");
  for (const line of lines) {
    if (!line.trim()) continue;
    try { applyOp(JSON.parse(line), false); } catch(e) {}
  }

  // Ensure Bank exists
  if (!users.has("Bank")) {
    users.set("Bank", { balance: 0, infinite: true });
    enqueueWrite({ type: "create", user: "Bank", infinite: true });
  } else {
    users.get("Bank").infinite = true;
  }

  console.log(`Data loaded. Users in memory: ${users.size}`);
}

// Express app factory
function createApp() {
  const app = express();
  app.use(cors());
  app.use(compression());
  app.disable("x-powered-by");

  app.get("/ping", (req, res) => res.send("pong"));
  app.get("/health", (req,res) => res.json({ ok:true, users: users.size }));

  // Check if user exists
  app.get("/exists", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    res.json({ exists: users.has(user) });
  });

  // Create wallet
  app.get("/createWallet", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (users.has(user)) return res.status(400).send("User exists");
    applyOp({ type:"create", user });
    res.json({ name:user, balance:0 });
  });

  // Get balance
  app.get("/balance", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    const u = users.get(user);
    if (!u) return res.status(404).send("User not found");
    res.json({ balance: u.infinite ? "infinite" : u.balance });
  });

  // Deposit
  app.get("/deposit", (req,res) => {
    const user = req.query.user;
    const amt = Number(req.query.amount);
    if (!user || isNaN(amt) || amt <= 0) return res.status(400).send("Invalid parameters");
    if (!users.has(user)) return res.status(404).send("User not found");
    applyOp({ type:"inc", user, amount: amt });
    res.json({ user, newBalance: getBalance(user) });
  });

  // Transfer
  app.get("/transfer", (req,res) => {
    const { from, to } = req.query;
    const amt = Number(req.query.amount);
    if (!from || !to || isNaN(amt) || amt <= 0) return res.status(400).send("Invalid parameters");
    const sender = users.get(from);
    const receiver = users.get(to);
    if (!sender || !receiver) return res.status(404).send("User not found");
    if (!sender.infinite && sender.balance < amt) return res.status(400).send("Insufficient funds");
    if (!sender.infinite) applyOp({ type:"inc", user:from, amount:-amt });
    applyOp({ type:"inc", user:to, amount:amt });
    res.json({
      from: { name: from, balance: sender.infinite ? "infinite" : sender.balance },
      to: { name: to, balance: receiver.balance }
    });
  });

  // Delete account
  app.get("/deleteAccount", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (user === "Bank") return res.status(400).send("Cannot delete Bank");
    if (!users.has(user)) return res.status(404).send("User not found");
    applyOp({ type:"delete", user });
    res.send(`Deleted ${user}`);
  });

  // Delete all (admin)
  app.get("/deleteAll", (req,res) => {
    const code = req.query.code;
    if (!code) return res.status(400).send("Admin code required");
    if (code !== ADMIN_CODE) return res.status(403).send("Invalid admin code");
    for (const k of Array.from(users.keys())) if (k !== "Bank") applyOp({ type:"delete", user:k });
    scheduleSnapshot();
    res.send("All user accounts deleted (Bank preserved)");
  });

  return app;
}

// Get balance helper
function getBalance(user) {
  const u = users.get(user);
  if (!u) return null;
  return u.infinite ? "infinite" : u.balance;
}

// Start server
(async()=>{
  await loadData();
  const app = createApp();
  app.listen(PORT, ()=>console.log(`Server listening on port ${PORT}, Admin: ${ADMIN_CODE}`));
})();

// Graceful shutdown
process.on("SIGINT", async ()=>{
  console.log("SIGINT: flushing snapshot...");
  try { await writeQueue; await snapshotToFile(); } catch(e){console.error(e);}
  process.exit(0);
});
