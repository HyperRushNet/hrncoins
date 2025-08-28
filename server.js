/**
 * server.js - HRNCoins Wallet
 * Lightweight, append-only log + snapshot, Map-indexed
 * BigInt-safe, polling-based updates, no SSE
 */

const express = require("express");
const fs = require("fs");
const fsExtra = require("fs-extra");
const path = require("path");
const cors = require("cors");
const compression = require("compression");

// Gebruik altijd een schrijfbare directory!
const DATA_DIR = process.env.DATA_DIR || "/tmp";
const SNAPSHOT_FILE = path.join(DATA_DIR, "snapshot.json");
const OPS_LOG_FILE = path.join(DATA_DIR, "ops.log");

const PORT = process.env.PORT || 3000;
const ADMIN_CODE = process.env.ADMIN_CODE || "change_me_admin_code";
const SNAPSHOT_OPS = parseInt(process.env.SNAPSHOT_OPS || "1000", 10);

// In-memory map: key=username, value={ balance: BigInt, infinite?: true, lastOp: timestamp }
const users = new Map();

// In-memory variable to track the last transaction. Does not persist across server restarts.
let lastTransaction = null;

// Queue for disk writes
let writeQueue = Promise.resolve();
let pendingOps = [];

function enqueueWrite(op) {
  pendingOps.push(op);
  if (pendingOps.length >= 20) flushOps();
}

function flushOps() {
  const opsToWrite = pendingOps.splice(0, pendingOps.length);
  writeQueue = writeQueue.then(() => {
    const data = opsToWrite.map(op => JSON.stringify(op)).join("\n") + "\n";
    return fs.promises.appendFile(OPS_LOG_FILE, data, "utf8")
      .then(() => { if (pendingOps.length >= SNAPSHOT_OPS) scheduleSnapshot(); });
  }).catch(err => console.error("Write queue error:", err));
  return writeQueue;
}

async function snapshotToFile() {
  const tmp = SNAPSHOT_FILE + ".tmp";
  const plain = { users: [] };
  for (const [name, v] of users) {
    const entry = { name, balance: v.infinite ? "0" : v.balance.toString() };
    if (v.infinite) entry.infinite = true;
    plain.users.push(entry);
  }
  await fsExtra.writeJson(tmp, plain, { spaces: 2 });
  await fsExtra.move(tmp, SNAPSHOT_FILE, { overwrite: true });
  await fs.promises.truncate(OPS_LOG_FILE, 0);
  console.log("Snapshot written.");
}

function scheduleSnapshot() {
  setImmediate(snapshotToFile).catch(e => console.error(e));
}

function applyOp(op, persist = true) {
  const { type } = op;
  if (type === "create") {
    if (!users.has(op.user)) users.set(op.user, { balance: BigInt(0), infinite: !!op.infinite, lastOp: Date.now() });
  } else if (type === "delete") {
    if (op.user && users.has(op.user) && op.user !== "Bank") users.delete(op.user);
  } else if (type === "inc") {
    const u = users.get(op.user);
    if (!u) return;
    if (!u.infinite) u.balance += BigInt(op.amount);
    u.lastOp = Date.now();
  } else if (type === "set") {
    const u = users.get(op.user);
    if (u) {
      u.balance = BigInt(op.balance);
      u.lastOp = Date.now();
    }
  }
  if (persist) enqueueWrite(op);
}

async function loadData() {
  if (!fs.existsSync(SNAPSHOT_FILE)) await fsExtra.writeJson(SNAPSHOT_FILE, { users: [] });
  if (!fs.existsSync(OPS_LOG_FILE)) fs.writeFileSync(OPS_LOG_FILE, "");

  const snapshot = await fsExtra.readJson(SNAPSHOT_FILE);
  if (snapshot?.users) {
    snapshot.users.forEach(u => users.set(u.name, {
      balance: BigInt(u.balance || "0"),
      infinite: !!u.infinite,
      lastOp: Date.now()
    }));
  }

  const lines = fs.readFileSync(OPS_LOG_FILE, "utf8").split("\n");
  for (const line of lines) {
    if (!line.trim()) continue;
    try { applyOp(JSON.parse(line), false); } catch(e) {}
  }

  if (!users.has("Bank")) {
    users.set("Bank", { balance: BigInt(0), infinite: true, lastOp: Date.now() });
    enqueueWrite({ type: "create", user: "Bank", infinite: true });
  } else users.get("Bank").infinite = true;

  console.log(`Data loaded. Users in memory: ${users.size}`);
}

function createApp() {
  const app = express();
  app.use(cors());
  app.use(compression());
  app.disable("x-powered-by");

  app.get("/ping", (req, res) => res.send("pong"));
  app.get("/health", (req,res) => res.json({ ok:true, users: users.size }));

  // Endpoint to check for wallet existence
  app.get("/exists", (req,res) => {
    const user = req.query.user;
    res.json({ exists: users.has(user) });
  });

  // Endpoint to create a new wallet
  app.get("/createWallet", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (users.has(user)) return res.status(400).send("Exists");
    applyOp({ type:"create", user });
    res.json({ name:user, balance:"0" });
  });

  // Endpoint to get wallet balance
  app.get("/balance", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    const u = users.get(user);
    if (!u) return res.status(404).send("Not found");
    res.json({ balance: u.infinite ? "infinite" : u.balance.toString() });
  });

  // Endpoint to deposit HRNCoins (e.g., from an admin)
  app.get("/deposit", (req,res) => {
    const user = req.query.user;
    const amt = req.query.amount;
    if (!user || !amt) return res.status(400).send("Invalid");
    if (!users.has(user)) return res.status(404).send("User not found");
    applyOp({ type:"inc", user, amount: amt.toString() });
    res.json({ user, newBalance: users.get(user).infinite ? "infinite" : users.get(user).balance.toString() });
  });

  // Main transfer endpoint (used for QR code payments)
  app.get("/transfer", (req,res) => {
    const { from, to } = req.query;
    const amt = req.query.amount;
    if (!from || !to || !amt) return res.status(400).send("Invalid request parameters");
    const s = users.get(from), r = users.get(to);
    if (!s || !r) return res.status(404).send("One or more users not found");
    if (!s.infinite && s.balance < BigInt(amt)) return res.status(400).send("Insufficient funds");

    // Apply operations
    if (!s.infinite) applyOp({ type:"inc", user:from, amount: (-BigInt(amt)).toString() });
    applyOp({ type:"inc", user:to, amount: amt.toString() });

    // Update the last transaction variable
    lastTransaction = {
      from,
      to,
      amount: amt,
      timestamp: new Date().toISOString()
    };
    
    res.json({
      from: { name:from, balance: s.infinite?"infinite":s.balance.toString() },
      to: { name:to, balance: r.balance.toString() }
    });
  });

  // New endpoint to get the last successful transaction
  app.get("/lastTransaction", (req, res) => {
      res.json(lastTransaction);
  });

  // Endpoint to delete a specific user account
  app.get("/deleteAccount", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (user==="Bank") return res.status(400).send("Cannot delete Bank");
    if (!users.has(user)) return res.status(404).send("User not found");
    applyOp({ type:"delete", user });
    res.send(`Deleted ${user}`);
  });

  // Admin endpoint to delete all user accounts except 'Bank'
  app.get("/deleteAll", (req,res) => {
    const code = req.query.code;
    if (!code) return res.status(400).send("Admin code required");
    if (code!==ADMIN_CODE) return res.status(403).send("Invalid code");
    for (const k of Array.from(users.keys())) if(k!=="Bank") applyOp({type:"delete",user:k});
    scheduleSnapshot();
    res.send("All user accounts deleted (Bank preserved)");
  });

  // ---- ALWAYS send CORS headers, also for errors and 404s ----
  app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS");
    res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") {
      return res.sendStatus(200);
    }
    next();
  });

  // fallback 404
  app.use((req,res) => {
    res.status(404).json({ error: "Not found" });
  });

  return app;
}

(async()=>{
  await loadData();
  const app = createApp();
  app.listen(PORT, ()=>console.log(`Server listening on ${PORT}, Admin: ${ADMIN_CODE}`));
})();

// Graceful shutdown
process.on("SIGINT", async ()=>{
  console.log("SIGINT: flushing snapshot...");
  try { await writeQueue; await snapshotToFile(); } catch(e){console.error(e);}
  process.exit(0);
});
