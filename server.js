const express = require("express");
const duckdb = require("duckdb");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

const dbPath = path.join(__dirname, "database.duckdb");
console.log("DB PATH:", dbPath);
const db = new duckdb.Database(dbPath);

// Helpers
function query(sql) {
  return new Promise((resolve, reject) => db.all(sql, (err, rows) => err ? reject(err) : resolve(rows)));
}
function exec(sql) {
  return new Promise((resolve, reject) => db.exec(sql, err => err ? reject(err) : resolve()));
}

// Init DB
async function init() {
  console.log("Initializing DB...");
  await exec(`
    CREATE TABLE IF NOT EXISTS stops (
      id INTEGER,
      name VARCHAR,
      lat DOUBLE,
      lon DOUBLE
    )
  `);

  let count = await query(`SELECT COUNT(*) as c FROM stops`);
  console.log("Rows before insert:", count);

  if (count[0].c === 0) {
    await exec(`INSERT INTO stops VALUES (1,'Central Station',45.508,-73.553)`);
    await exec(`INSERT INTO stops VALUES (2,'City Hall',45.509,-73.554)`);
    await exec(`INSERT INTO stops VALUES (3,'Museum',45.507,-73.552)`);
    let verify = await query(`SELECT * FROM stops`);
    console.log("Rows after insert:", verify);
  }
}

// API
app.get("/api/stops", async (req, res) => res.json(await query(`SELECT * FROM stops`)));

app.get("/api/near", async (req, res) => {
  const lat = Number(req.query.lat);
  const lon = Number(req.query.lon);
  if (isNaN(lat) || isNaN(lon)) return res.status(400).json({ error: "Invalid coordinates" });

  const sql = `
    SELECT name,
    6371000 * acos(
      cos(radians(${lat})) * cos(radians(lat)) *
      cos(radians(lon) - radians(${lon})) +
      sin(radians(${lat})) * sin(radians(lat))
    ) AS distance
    FROM stops
    ORDER BY distance
    LIMIT 3
  `;
  res.json(await query(sql));
});

// Static frontend
app.use(express.static(path.join(__dirname, "public")));

// Start server
init().then(() => {
  app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
});
