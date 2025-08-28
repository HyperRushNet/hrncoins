/**
 * server.js - Geoptimaliseerd, realtime wallet server
 * Features:
 * - Append-only log + snapshot
 * - Map-indexed users
 * - /exists endpoint
 * - /createWallet en /balance altijd realtime
 * - Batched writes + snapshots
 * - Compression voor snelle responses
 * - Cluster mode (alle cores)
 * - Bank account altijd infinite
 */

const express = require("express");
const fs = require("fs");
const fsExtra = require("fs-extra");
const path = require("path");
const cors = require("cors");
const compression = require("compression");
const cluster = require("cluster");
const os = require("os");

const PORT = process.env.PORT || 3000;
const ADMIN_CODE = process.env.ADMIN_CODE || "change_me_admin_code";
const SNAPSHOT_OPS = parseInt(process.env.SNAPSHOT_OPS || "1000", 10);
const BATCH_SIZE = 20;

const DATA_DIR = path.resolve(__dirname);
const SNAPSHOT_FILE = path.join(DATA_DIR, "snapshot.json");
const OPS_LOG_FILE = path.join(DATA_DIR, "ops.log");

// In-memory users map
const users = new Map();

// Write queue
let writeQueue = Promise.resolve();
let pendingOps = [];

// ------------------- Helper Functions -------------------
function enqueueWrite(op) {
  pendingOps.push(op);
  if (pendingOps.length >= BATCH_SIZE) flushOps();
}

function flushOps() {
  const opsToWrite = pendingOps.splice(0, pendingOps.length);
  writeQueue = writeQueue.then(() => {
    const data = opsToWrite.map(op => JSON.stringify(op)).join("\n") + "\n";
    return fs.promises.appendFile(OPS_LOG_FILE, data, "utf8")
      .then(() => {
        if (pendingOpsSinceSnapshot() >= SNAPSHOT_OPS) scheduleSnapshot();
      });
  }).catch(err => console.error("Write queue error:", err));
  return writeQueue;
}

function pendingOpsSinceSnapshot() {
  return pendingOps.length;
}

async function snapshotToFile() {
  const tmp = SNAPSHOT_FILE + ".tmp";
  const snapshot = { users: [] };
  for (const [name, u] of users) {
    const entry = { name, balance: u.infinite ? 0 : u.balance };
    if (u.infinite) entry.infinite = true;
    snapshot.users.push(entry);
  }
  await fsExtra.writeJson(tmp, snapshot, { spaces: 2 });
  await fsExtra.move(tmp, SNAPSHOT_FILE, { overwrite: true });
  await fs.promises.truncate(OPS_LOG_FILE, 0);
  console.log("Snapshot saved, ops.log truncated.");
}

function scheduleSnapshot() {
  setImmediate(snapshotToFile).catch(e => console.error(e));
}

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

async function loadData() {
  if (!fs.existsSync(SNAPSHOT_FILE)) await fsExtra.writeJson(SNAPSHOT_FILE, { users: [] });
  if (!fs.existsSync(OPS_LOG_FILE)) fs.writeFileSync(OPS_LOG_FILE, "");

  const snapshot = await fsExtra.readJson(SNAPSHOT_FILE);
  if (snapshot?.users) snapshot.users.forEach(u => users.set(u.name, { balance: u.balance || 0, infinite: !!u.infinite }));

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

function getBalance(user) {
  const u = users.get(user);
  if (!u) return null;
  return u.infinite ? "infinite" : u.balance;
}

// ------------------- Express App -------------------
function createApp() {
  const app = express();
  app.use(cors());
  app.use(compression());
  app.disable("x-powered-by");

  app.get("/ping", (req, res) => res.send("pong"));
  app.get("/health", (req, res) => res.json({ ok:true, users: users.size }));
  app.get("/exists", (req, res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    res.json({ exists: users.has(user) });
  });

  // --- Realtime endpoints ---
  app.get("/createWallet", (req, res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (users.has(user)) return res.status(400).send("Exists");

    users.set(user, { balance: 0 });
    enqueueWrite({ type:"create", user });

    res.json({ name:user, balance:0 });
  });

  app.get("/balance", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    const bal = getBalance(user);
    if (bal === null) return res.status(404).send("Not found");
    res.json({ balance: bal });
  });

  app.get("/deposit", (req,res) => {
    const user = req.query.user;
    const amt = Number(req.query.amount);
    if (!user || isNaN(amt) || amt <= 0) return res.status(400).send("Invalid");
    const u = users.get(user);
    if (!u) return res.status(404).send("User not found");

    if (!u.infinite) u.balance += amt;
    enqueueWrite({ type:"inc", user, amount: amt });

    res.json({ user, newBalance: getBalance(user) });
  });

  app.get("/transfer", (req,res) => {
    const { from, to } = req.query;
    const amt = Number(req.query.amount);
    if (!from||!to||isNaN(amt)||amt<=0) return res.status(400).send("Invalid");
    const s = users.get(from), r = users.get(to);
    if (!s||!r) return res.status(404).send("User not found");
    if (!s.infinite && s.balance < amt) return res.status(400).send("Insufficient funds");

    if (!s.infinite) s.balance -= amt;
    r.balance += amt;

    enqueueWrite({ type:"inc", user:from, amount:-amt });
    enqueueWrite({ type:"inc", user:to, amount:amt });

    res.json({
      from:{name:from,balance:s.infinite?"infinite":s.balance},
      to:{name:to,balance:r.balance}
    });
  });

  app.get("/deleteAccount", (req,res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User required");
    if (user === "Bank") return res.status(400).send("Cannot delete Bank");
    if (!users.has(user)) return res.status(404).send("User not found");

    users.delete(user);
    enqueueWrite({ type:"delete", user });
    res.send(`Deleted ${user}`);
  });

  app.get("/deleteAll", (req,res) => {
    const code = req.query.code;
    if (!code) return res.status(400).send("Admin code required");
    if (code !== ADMIN_CODE) return res.status(403).send("Invalid code");

    for (const k of Array.from(users.keys())) if (k !== "Bank") {
      users.delete(k);
      enqueueWrite({ type:"delete", user:k });
    }
    scheduleSnapshot();
    res.send("All user accounts deleted (Bank preserved)");
  });

  return app;
}

// ------------------- Cluster / Worker -------------------
if (cluster.isMaster) {
  const cpus = os.cpus().length;
  console.log(`Master ${process.pid} running, forking ${cpus} workers`);
  for (let i = 0; i < cpus; i++) cluster.fork();
  cluster.on("exit", (worker, code, signal) => console.log(`Worker ${worker.process.pid} exited`));
} else {
  (async () => {
    await loadData();
    const app = createApp();
    app.listen(PORT, () => console.log(`Worker ${process.pid} listening on ${PORT}, Admin: ${ADMIN_CODE}`));
  })();
}

// ------------------- Graceful Shutdown -------------------
process.on("SIGINT", async () => {
  console.log("SIGINT: flushing snapshot...");
  try { await writeQueue; await snapshotToFile(); } catch(e){console.error(e);}
  process.exit(0);
});
