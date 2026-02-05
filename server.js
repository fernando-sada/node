const express = require("express");
const duckdb = require("duckdb");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

// Load the Parquet file
const parquetFile = path.join(__dirname, "stops.parquet");
console.log("Loading stops from Parquet:", parquetFile);

const db = new duckdb.Database(':memory:'); // in-memory DB

// Helpers
function query(sql) {
  return new Promise((resolve, reject) =>
    db.all(sql, (err, rows) => (err ? reject(err) : resolve(rows)))
  );
}
function exec(sql) {
  return new Promise((resolve, reject) =>
    db.exec(sql, err => (err ? reject(err) : resolve()))
  );
}

// Init DB
async function init() {
  console.log("Initializing DB from Parquet...");
  await exec(`CREATE TABLE stops AS SELECT * FROM read_parquet('${parquetFile}')`);

  let count = await query(`SELECT COUNT(*) AS c FROM stops`);
  console.log("Rows loaded:", count[0].c.toString());
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
