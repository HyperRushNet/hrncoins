/**
 * server.js - HRNCoins Wallet (stabilized)
 * Lightweight, append-only log + snapshot, BigInt-safe
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

// In-memory map: key=username, value={ balance: BigInt, infinite?: boolean, lastOp: timestamp }
const users = new Map();

// Queue for disk writes
let writeQueue = Promise.resolve();
let pendingOps = [];

function enqueueWrite(op) {
  pendingOps.push(op);
  if (pendingOps.length >= 20) flushOps();
}

function flushOps() {
  const opsToWrite = pendingOps.splice(0, pendingOps.length);
  writeQueue = writeQueue
    .then(() => {
      const data = opsToWrite.map(op => JSON.stringify(op)).join("\n") + "\n";
      return fs.promises.appendFile(OPS_LOG_FILE, data, "utf8")
        .then(() => { if (pendingOps.length >= SNAPSHOT_OPS) scheduleSnapshot(); });
    })
    .catch(err => console.error("Write queue error:", err));
  return writeQueue;
}

async function snapshotToFile() {
  const tmp = SNAPSHOT_FILE + ".tmp";
  const plain = { users: [] };
  for (const [name, v] of users) {
    const entry = { name, balance: v.balance.toString() };
    if (v.infinite) entry.infinite = true;
    plain.users.push(entry);
  }
  await fsExtra.writeJson(tmp, plain, { spaces: 2 });
  await fsExtra.move(tmp, SNAPSHOT_FILE, { overwrite: true });
  await fs.promises.truncate(OPS_LOG_FILE, 0);
  console.log("Snapshot written.");
}

function scheduleSnapshot() {
  setImmediate(() => snapshotToFile().catch(e => console.error("Snapshot error:", e)));
}

function applyOp(op, persist = true) {
  try {
    const { type, user } = op;
    if (!user) return;
    if (type === "create") {
      if (!users.has(user)) {
        users.set(user, { balance: BigInt(0), infinite: !!op.infinite, lastOp: Date.now() });
      }
    } else if (type === "delete") {
      if (user !== "Bank" && users.has(user)) users.delete(user);
    } else if (type === "inc") {
      const u = users.get(user);
      if (!u) return;
      if (!u.infinite) u.balance += BigInt(op.amount);
      u.lastOp = Date.now();
    } else if (type === "set") {
      const u = users.get(user);
      if (u) {
        u.balance = BigInt(op.balance);
        u.lastOp = Date.now();
      }
    }
    if (persist) enqueueWrite(op);
  } catch (e) {
    console.error("applyOp error:", e, op);
  }
}

async function loadData() {
  // Ensure files exist
  if (!fs.existsSync(SNAPSHOT_FILE)) await fsExtra.writeJson(SNAPSHOT_FILE, { users: [] });
  if (!fs.existsSync(OPS_LOG_FILE)) fs.writeFileSync(OPS_LOG_FILE, "");

  // Load snapshot
  try {
    const snapshot = await fsExtra.readJson(SNAPSHOT_FILE);
    if (snapshot?.users) {
      snapshot.users.forEach(u => users.set(u.name, {
        balance: BigInt(u.balance || "0"),
        infinite: !!u.infinite,
        lastOp: Date.now()
      }));
    }
  } catch (e) { console.error("Error loading snapshot:", e); }

  // Apply ops log
  try {
    const lines = fs.readFileSync(OPS_LOG_FILE, "utf8").split("\n");
    for (const line of lines) {
      if (!line.trim()) continue;
      try { applyOp(JSON.parse(line), false); } catch(e){ console.error("Invalid op line:", e); }
    }
  } catch(e) { console.error("Error reading ops.log:", e); }

  // Ensure Bank exists
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

  app.get("/exists", (req,res) => {
    const user = req.query.user;
    res.json({ exists: users.has(user) });
  });

  app.get("/createWallet", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (users.has(user)) return res.status(400).send("Exists");
    applyOp({ type:"create", user });
    res.json({ name:user, balance:"0" });
  });

  app.get("/balance", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    const u = users.get(user);
    if (!u) return res.status(404).send("Not found");
    res.json({ balance: u.infinite ? "infinite" : u.balance.toString() });
  });

  app.get("/deposit", (req,res) => {
    const user = req.query.user;
    const amt = req.query.amount;
    if (!user || !amt) return res.status(400).send("Invalid");
    if (!users.has(user)) return res.status(404).send("User not found");
    applyOp({ type:"inc", user, amount: amt.toString() });
    const u = users.get(user);
    res.json({ user, newBalance: u.infinite ? "infinite" : u.balance.toString() });
  });

  app.get("/transfer", (req,res) => {
    const { from, to } = req.query;
    const amt = req.query.amount;
    if (!from || !to || !amt) return res.status(400).send("Invalid");
    const s = users.get(from), r = users.get(to);
    if (!s || !r) return res.status(404).send("User not found");
    const amountBig = BigInt(amt);
    if (!s.infinite && s.balance < amountBig) return res.status(400).send("Insufficient funds");
    if (!s.infinite) applyOp({ type:"inc", user:from, amount: (-amountBig).toString() });
    applyOp({ type:"inc", user:to, amount: amountBig.toString() });
    res.json({
      from: { name:from, balance: s.infinite?"infinite":s.balance.toString() },
      to: { name:to, balance: r.balance.toString() }
    });
  });

  app.get("/deleteAccount", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (user==="Bank") return res.status(400).send("Cannot delete Bank");
    if (!users.has(user)) return res.status(404).send("User not found");
    applyOp({ type:"delete", user });
    res.send(`Deleted ${user}`);
  });

  app.get("/deleteAll", (req,res) => {
    const code = req.query.code;
    if (!code) return res.status(400).send("Admin code required");
    if (code!==ADMIN_CODE) return res.status(403).send("Invalid code");
    for (const k of Array.from(users.keys())) if(k!=="Bank") applyOp({type:"delete",user:k});
    scheduleSnapshot();
    res.send("All user accounts deleted (Bank preserved)");
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
