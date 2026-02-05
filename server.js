const express = require("express");
const duckdb = require("duckdb");


const app = express();
const PORT = 3000;

/* ---------------- DATABASE ---------------- */

const path = require("path");
const dbPath = path.join(__dirname, "database.duckdb");
console.log("DB PATH:", dbPath);

const db = new duckdb.Database(dbPath);

// helper to run SQL and return JSON
// SELECT returning rows
function query(sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

function exec(sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, err => {
      if (err) reject(err);
      else resolve();
    });
  });
}




/* ---------- initialize data once ---------- */

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

  if (count[0].c == 0) {

    await exec(`INSERT INTO stops VALUES (1,'Central Station',45.508,-73.553)`);
    await exec(`INSERT INTO stops VALUES (2,'City Hall',45.509,-73.554)`);
    await exec(`INSERT INTO stops VALUES (3,'Museum',45.507,-73.552)`);

    let verify = await query(`SELECT * FROM stops`);
    console.log("Rows after insert:", verify);
  }
}

/* ---------------- API ---------------- */

// return all stops
app.get("/api/stops", async (req, res) => {
  const rows = await query(`SELECT * FROM stops`);
  res.json(rows);
});

// simple filtered query
app.get("/api/near", async (req, res) => {
  const lat = Number(req.query.lat);
  const lon = Number(req.query.lon);

  if (isNaN(lat) || isNaN(lon)) {
    return res.status(400).json({ error: "Invalid coordinates" });
  }

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

  const rows = await query(sql);
  res.json(rows);
});



/* -------------- static frontend -------------- */

app.use(express.static(path.join(__dirname, "public")));

/* -------------- start server -------------- */

init().then(() => {
  app.listen(PORT, () =>
    console.log(`Server running: http://localhost:${PORT}`)
  );
});
