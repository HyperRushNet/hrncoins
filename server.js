// server.js
const express = require("express");
const fs = require("fs-extra");
const cors = require("cors");
const app = express();

const PORT = process.env.PORT || 3000;
const DB_FILE = "./database.json";

// Admin code: zet via env var ADMIN_CODE. Default is insecure => verander.
const ADMIN_CODE = process.env.ADMIN_CODE || "change_me_admin_code";

let db = { users: [] };

// ---------- Helpers ----------
async function loadDB() {
  try {
    db = await fs.readJson(DB_FILE);
    if (!db || !Array.isArray(db.users)) {
      db = { users: [] };
    }
  } catch (err) {
    // Geen database: maak een lege structuur
    db = { users: [] };
  }

  // Zorg dat Bank-wallet altijd bestaat en heeft infinite flag
  if (!db.users.find(u => u.name === "Bank")) {
    db.users.push({ name: "Bank", balance: 0, infinite: true });
    await saveDB();
  } else {
    // Zorg dat Bank altijd infinite = true
    const bank = db.users.find(u => u.name === "Bank");
    bank.infinite = true;
    if (typeof bank.balance !== "number") bank.balance = 0;
    await saveDB();
  }
}

async function saveDB() {
  // Kleine bescherming: nooit serializen van non-JSON waarden
  // (we gebruiken alleen plain objects / numbers / booleans)
  await fs.writeJson(DB_FILE, db, { spaces: 2 });
}

function findUser(name) {
  if (!name) return null;
  return db.users.find(u => u.name === name);
}

function isBank(user) {
  return user && user.name === "Bank";
}

// ---------- Init ----------
loadDB().catch(err => {
  console.error("Fout bij laden DB:", err);
});

// ---------- Middleware ----------
app.use(cors()); // allow all origins (licht & eenvoudig)
app.disable("x-powered-by"); // iets minder fingerprintable

// ---------- Endpoints ----------

// Ping
app.get("/ping", (req, res) => {
  res.send("pong");
});

// Maak wallet aan
app.get("/createWallet", async (req, res) => {
  const user = req.query.user;
  if (!user) return res.status(400).send("User parameter required");

  if (findUser(user)) {
    return res.status(400).send("User already exists");
  }

  const wallet = { name: user, balance: 0 };
  db.users.push(wallet);
  await saveDB();
  res.json(wallet);
});

// Lijst van wallets (light, handig bij debug)
app.get("/list", (req, res) => {
  // Verberg bank.infinite flag niet nodig, maar we sturen alles
  res.json(db.users);
});

// Saldo bekijken
app.get("/balance", (req, res) => {
  const user = req.query.user;
  if (!user) return res.status(400).send("User parameter required");

  const wallet = findUser(user);
  if (!wallet) return res.status(404).send("User not found");

  // Indien bank: toon 'infinite' als true
  if (wallet.infinite) return res.json({ balance: "infinite" });

  res.json({ balance: wallet.balance });
});

// Transfer
app.get("/transfer", async (req, res) => {
  const fromName = req.query.from;
  const toName = req.query.to;
  const amount = Number(req.query.amount);

  if (!fromName || !toName || isNaN(amount) || amount <= 0) {
    return res.status(400).send("Invalid parameters");
  }

  const sender = findUser(fromName);
  const receiver = findUser(toName);

  if (!sender || !receiver) return res.status(404).send("User not found");

  // Check saldo (Bank heeft oneindig geld)
  if (!sender.infinite && sender.balance < amount) {
    return res.status(400).send("Insufficient funds");
  }

  if (!sender.infinite) sender.balance -= amount;
  receiver.balance += amount;

  await saveDB();
  res.json({ from: { name: sender.name, balance: sender.infinite ? "infinite" : sender.balance }, to: { name: receiver.name, balance: receiver.balance } });
});

// Deposit via Bank (mag ook gebruikt worden in plaats van transfer)
app.get("/deposit", async (req, res) => {
  const user = req.query.user;
  const amount = Number(req.query.amount);

  if (!user || isNaN(amount) || amount <= 0) {
    return res.status(400).send("Invalid parameters");
  }

  const wallet = findUser(user);
  if (!wallet) return res.status(404).send("User not found");

  wallet.balance += amount;
  await saveDB();
  res.json({ user: wallet.name, newBalance: wallet.balance });
});

// Verwijder je eigen account
// Voor veiligheid: je mag je account verwijderen, maar niet de Bank.
// Gebruik: /deleteAccount?user=Alice
app.get("/deleteAccount", async (req, res) => {
  const user = req.query.user;
  if (!user) return res.status(400).send("User parameter required");
  if (user === "Bank") return res.status(400).send("Cannot delete Bank account");

  const idx = db.users.findIndex(u => u.name === user);
  if (idx === -1) return res.status(404).send("User not found");

  db.users.splice(idx, 1);
  await saveDB();
  res.send(`User ${user} deleted`);
});

// Admin: verwijder alle accounts (behalve Bank) — vereist admin code
// Gebruik: /deleteAll?code=YOUR_ADMIN_CODE
app.get("/deleteAll", async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send("Admin code required");

  if (code !== ADMIN_CODE) {
    return res.status(403).send("Invalid admin code");
  }

  // Bewaar alleen de Bank-account
  const bank = db.users.find(u => u.name === "Bank") || { name: "Bank", balance: 0, infinite: true };
  db.users = [bank];
  await saveDB();
  res.send("All user accounts deleted (Bank preserved)");
});

// Kleine health endpoint met minimale footprint
app.get("/health", (req, res) => {
  res.json({ ok: true, users: db.users.length });
});

// Start
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Admin code (set ADMIN_CODE env var to change): ${ADMIN_CODE}`);
});
