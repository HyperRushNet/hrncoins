const express = require("express");
const fs = require("fs-extra");
const app = express();
const PORT = process.env.PORT || 3000;

const DB_FILE = "./database.json";
let db = { users: [] };

// 📌 Database inladen bij opstart
async function loadDB() {
  try {
    db = await fs.readJson(DB_FILE);
  } catch (err) {
    console.log("Geen database gevonden, nieuwe maken...");
    await fs.writeJson(DB_FILE, db);
  }

  // Zorg dat Bank-wallet altijd bestaat
  if (!db.users.find(u => u.name === "Bank")) {
    db.users.push({ name: "Bank", balance: Infinity });
    await saveDB();
  }
}

// 📌 Database opslaan
async function saveDB() {
  await fs.writeJson(DB_FILE, db, { spaces: 2 });
}

loadDB();

// ✅ Ping endpoint
app.get("/ping", (req, res) => {
  res.send("pong");
});

// ✅ Wallet aanmaken
app.get("/createWallet", async (req, res) => {
  const { user } = req.query;
  if (!user) return res.status(400).send("User parameter required");

  if (db.users.find(u => u.name === user)) {
    return res.status(400).send("User already exists");
  }

  const wallet = { name: user, balance: 0 };
  db.users.push(wallet);
  await saveDB();

  res.json(wallet);
});

// ✅ Saldo bekijken
app.get("/balance", (req, res) => {
  const { user } = req.query;
  const wallet = db.users.find(u => u.name === user);
  if (!wallet) return res.status(404).send("User not found");

  res.json({ balance: wallet.balance });
});

// ✅ Geld overmaken
app.get("/transfer", async (req, res) => {
  const { from, to, amount } = req.query;
  const amt = Number(amount);

  if (!from || !to || isNaN(amt) || amt <= 0) {
    return res.status(400).send("Invalid parameters");
  }

  const sender = db.users.find(u => u.name === from);
  const receiver = db.users.find(u => u.name === to);

  if (!sender || !receiver) return res.status(404).send("User not found");

  if (sender.name !== "Bank" && sender.balance < amt) {
    return res.status(400).send("Insufficient funds");
  }

  // Bank heeft oneindig geld → saldo blijft Infinity
  if (sender.name !== "Bank") {
    sender.balance -= amt;
  }

  receiver.balance += amt;
  await saveDB();

  res.json({ from: sender, to: receiver });
});

// ✅ Geld toevoegen via Bank (deposit)
app.get("/deposit", async (req, res) => {
  const { user, amount } = req.query;
  const amt = Number(amount);

  if (!user || isNaN(amt) || amt <= 0) {
    return res.status(400).send("Invalid parameters");
  }

  const wallet = db.users.find(u => u.name === user);
  if (!wallet) return res.status(404).send("User not found");

  // Geld komt uit de Bank (oneindig saldo)
  wallet.balance += amt;
  await saveDB();

  res.json({ user: wallet.name, newBalance: wallet.balance });
});

// 🚀 Server starten
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
