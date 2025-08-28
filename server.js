/**
 * server.js
 * Lichtgewicht, append-only log + snapshot, Map-indexed, /exists endpoint.
 *
 * Environment:
 *   PORT (default 3000)
 *   ADMIN_CODE (vervang het standaardwachtwoord!)
 *   SNAPSHOT_OPS (optioneel, default 1000) -> maak snapshot na dit aantal ops
 *
 * Opschoning / schaaladviezen:
 * - Stel SNAPSHOT_OPS lager in op lage load; hoger bij veel ops.
 * - Monitor geheugengebruik: ~1M users met alleen naam+number kan groot worden op 512MB.
 */

const express = require("express");
const fs = require("fs");
const fsExtra = require("fs-extra");
const path = require("path");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 3000;
const ADMIN_CODE = process.env.ADMIN_CODE || "change_me_admin_code";
const SNAPSHOT_OPS = parseInt(process.env.SNAPSHOT_OPS || "1000", 10);

const DATA_DIR = path.resolve(__dirname);
const SNAPSHOT_FILE = path.join(DATA_DIR, "snapshot.json");
const OPS_LOG_FILE = path.join(DATA_DIR, "ops.log");

// In-memory map: key = username, value = { balance: Number, infinite?: true }
const users = new Map();

// Simple in-process queue to serialize write operations (append & snapshot)
let writeQueue = Promise.resolve();
let pendingOpsSinceSnapshot = 0;

// Helper: enqueue a function that returns a promise
function enqueueWrite(fn) {
  writeQueue = writeQueue.then(fn).catch(err => {
    console.error("Write queue error:", err);
  });
  return writeQueue;
}

// Append op to ops.log as newline-delimited JSON
function appendOp(op) {
  const line = JSON.stringify(op) + "\n";
  return new Promise((resolve, reject) => {
    fs.appendFile(OPS_LOG_FILE, line, "utf8", err => {
      if (err) return reject(err);
      pendingOpsSinceSnapshot++;
      // trigger snapshot if threshold reached
      if (pendingOpsSinceSnapshot >= SNAPSHOT_OPS) {
        pendingOpsSinceSnapshot = 0;
        // schedule snapshot (non-blocking in API path)
        enqueueWrite(() => snapshotToFile()).catch(e => console.error("Snapshot error:", e));
      }
      resolve();
    });
  });
}

// Create a snapshot.json from current in-memory map (atomic replace)
async function snapshotToFile() {
  const tmp = SNAPSHOT_FILE + ".tmp";
  const plain = { users: [] };
  for (const [name, v] of users) {
    // Don't include non-serializable fields
    const entry = { name, balance: v.infinite ? 0 : v.balance };
    if (v.infinite) entry.infinite = true;
    plain.users.push(entry);
  }
  await fsExtra.writeJson(tmp, plain, { spaces: 2 });
  // atomic rename
  await fsExtra.move(tmp, SNAPSHOT_FILE, { overwrite: true });
  // truncate ops.log (we can safely delete old ops)
  await new Promise((resolve, reject) => fs.truncate(OPS_LOG_FILE, 0, err => err ? reject(err) : resolve()));
  console.log("Snapshot written and ops.log truncated.");
}

// Load snapshot + replay ops log
async function loadData() {
  // ensure files exist
  if (!fs.existsSync(SNAPSHOT_FILE)) {
    await fsExtra.writeJson(SNAPSHOT_FILE, { users: [] });
  }
  if (!fs.existsSync(OPS_LOG_FILE)) {
    fs.writeFileSync(OPS_LOG_FILE, "");
  }

  // load snapshot
  const snapshot = await fsExtra.readJson(SNAPSHOT_FILE);
  if (snapshot && Array.isArray(snapshot.users)) {
    for (const u of snapshot.users) {
      users.set(u.name, { balance: typeof u.balance === "number" ? u.balance : 0, infinite: !!u.infinite });
    }
  }

  // replay ops.log (stream line by line for memory safety)
  await new Promise((resolve, reject) => {
    const rs = fs.createReadStream(OPS_LOG_FILE, { encoding: "utf8" });
    let leftover = "";
    rs.on("data", chunk => {
      const lines = (leftover + chunk).split("\n");
      leftover = lines.pop();
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const op = JSON.parse(line);
          applyOpInMemory(op, false); // don't append again
        } catch (e) {
          console.warn("Skipping malformed op line:", e);
        }
      }
    });
    rs.on("end", () => {
      if (leftover.trim()) {
        try {
          const op = JSON.parse(leftover);
          applyOpInMemory(op, false);
        } catch (e) {
          console.warn("Skipping malformed leftover op:", e);
        }
      }
      resolve();
    });
    rs.on("error", reject);
  });

  // ensure Bank exists and is infinite
  if (!users.has("Bank")) {
    users.set("Bank", { balance: 0, infinite: true });
    // persist creation
    await enqueueWrite(() => appendOp({ op: "create", user: "Bank", infinite: true }));
  } else {
    const b = users.get("Bank");
    b.infinite = true;
  }

  console.log(`Data loaded. Users in memory: ${users.size}`);
}

// Apply op to in-memory map; if persist=true it appends op to log
async function applyOpInMemory(op, persist = true) {
  const { type } = op;
  if (type === "create") {
    if (!users.has(op.user)) {
      users.set(op.user, { balance: op.infinite ? 0 : 0, infinite: !!op.infinite });
      if (persist) await appendOp(op);
    }
  } else if (type === "delete") {
    if (op.user && users.has(op.user) && op.user !== "Bank") {
      users.delete(op.user);
      if (persist) await appendOp(op);
    }
  } else if (type === "set") {
    // set absolute balance
    if (users.has(op.user)) {
      const u = users.get(op.user);
      u.balance = Number(op.balance) || 0;
      if (persist) await appendOp(op);
    }
  } else if (type === "inc") {
    // increment balance by amount (positive or negative), respect Bank infinite
    const u = users.get(op.user);
    if (u) {
      if (!u.infinite) {
        u.balance = Number(u.balance || 0) + Number(op.amount);
      } // if infinite, no change
      if (persist) await appendOp(op);
    }
  } else if (type === "batch") {
    // generic for future; do nothing now
  }
}

// Existence check (fast)
function existsInMemory(user) {
  return users.has(user);
}

// Get balance (fast)
function getBalance(user) {
  const u = users.get(user);
  if (!u) return null;
  if (u.infinite) return "infinite";
  return u.balance;
}

// Server init
(async () => {
  try {
    await loadData();
  } catch (e) {
    console.error("Failed to load data:", e);
    process.exit(1);
  }

  // middleware
  app.use(cors());
  app.disable("x-powered-by");

  // lightweight endpoints

  app.get("/ping", (req, res) => res.send("pong"));

  app.get("/health", (req, res) => {
    res.json({ ok: true, users: users.size });
  });

  // exists endpoint (O(1))
  app.get("/exists", (req, res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User parameter required");
    res.json({ exists: existsInMemory(user) });
  });

  app.get("/createWallet", async (req, res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User parameter required");
    if (users.has(user)) return res.status(400).send("User already exists");

    // add to memory & append op (serialized)
    await enqueueWrite(async () => {
      users.set(user, { balance: 0 });
      await appendOp({ type: "create", user });
    });

    res.json({ name: user, balance: 0 });
  });

  app.get("/balance", (req, res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User parameter required");
    const bal = getBalance(user);
    if (bal === null) return res.status(404).send("User not found");
    res.json({ balance: bal });
  });

  app.get("/deposit", async (req, res) => {
    const user = req.query.user;
    const amount = Number(req.query.amount);
    if (!user || isNaN(amount) || amount <= 0) return res.status(400).send("Invalid parameters");
    if (!users.has(user)) return res.status(404).send("User not found");

    await enqueueWrite(async () => {
      // deposit is inc on user (from Bank)
      await applyOpInMemory({ type: "inc", user, amount }, true);
    });

    res.json({ user, newBalance: getBalance(user) });
  });

  app.get("/transfer", async (req, res) => {
    const from = req.query.from;
    const to = req.query.to;
    const amount = Number(req.query.amount);
    if (!from || !to || isNaN(amount) || amount <= 0) return res.status(400).send("Invalid parameters");
    const sender = users.get(from);
    const receiver = users.get(to);
    if (!sender || !receiver) return res.status(404).send("User not found");

    if (!sender.infinite && sender.balance < amount) return res.status(400).send("Insufficient funds");

    await enqueueWrite(async () => {
      if (!sender.infinite) await applyOpInMemory({ type: "inc", user: from, amount: -amount }, true);
      await applyOpInMemory({ type: "inc", user: to, amount: amount }, true);
    });

    res.json({
      from: { name: from, balance: sender.infinite ? "infinite" : getBalance(from) },
      to: { name: to, balance: getBalance(to) }
    });
  });

  app.get("/deleteAccount", async (req, res) => {
    const user = req.query.user;
    if (!user) return res.status(400).send("User parameter required");
    if (user === "Bank") return res.status(400).send("Cannot delete Bank account");
    if (!users.has(user)) return res.status(404).send("User not found");

    await enqueueWrite(async () => {
      users.delete(user);
      await appendOp({ type: "delete", user });
    });

    res.send(`User ${user} deleted`);
  });

  app.get("/deleteAll", async (req, res) => {
    const code = req.query.code;
    if (!code) return res.status(400).send("Admin code required");
    if (code !== ADMIN_CODE) return res.status(403).send("Invalid admin code");

    await enqueueWrite(async () => {
      // remove all except Bank
      for (const k of Array.from(users.keys())) {
        if (k !== "Bank") users.delete(k);
      }
      await appendOp({ type: "deleteAll", by: "admin" });
      // snapshot immediately to clear log growth
      await snapshotToFile();
    });

    res.send("All user accounts deleted (Bank preserved)");
  });

  // graceful shutdown: flush pending writes
  process.on("SIGINT", async () => {
    console.log("SIGINT received — flushing snapshot...");
    try {
      await writeQueue;
      await snapshotToFile();
    } catch (e) {
      console.error("Error on shutdown:", e);
    }
    process.exit(0);
  });

  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Admin code (set ADMIN_CODE env var to change): ${ADMIN_CODE}`);
  });
})();
