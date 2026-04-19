export default async function handler(req, res) {
  const health = {
    ok: true,
    postgres: {
      configured: false,
      healthy: false,
    },
    timestamp: new Date().toISOString(),
    runtime: {
      source: "vercel-function",
      node: process.version,
      environment: "serverless",
      vercelEnvironment: process.env.VERCEL_ENV || null,
    },
  };

  try {
    if (process.env.DATABASE_URL) {
      health.postgres.configured = true;

      try {
        const { Client } = await import("pg");
        const client = new Client({
          connectionString: process.env.DATABASE_URL,
          ssl: { rejectUnauthorized: false },
        });

        await client.connect();
        await client.query("SELECT 1");
        await client.end();

        health.postgres.healthy = true;
      } catch (dbError) {
        health.postgres.healthy = false;
        health.postgres.error = dbError.message;
      }
    }
  } catch (err) {
    health.ok = false;
    health.error = err.message;
  }

  res.setHeader("Content-Type", "application/json");
  res.statusCode = 200;
  res.end(JSON.stringify(health));
}
