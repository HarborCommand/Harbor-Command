import { Pool } from 'pg';

const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    })
  : null;

export async function getApiHealthStatus({ source = 'unknown' } = {}) {
  let postgres = {
    configured: false,
    healthy: false
  };

  if (process.env.DATABASE_URL && pool) {
    postgres.configured = true;

    try {
      await pool.query('SELECT 1');
      postgres.healthy = true;
    } catch (err) {
      postgres.healthy = false;
    }
  }

  return {
    ok: true,
    postgres,
    timestamp: new Date().toISOString(),
    runtime: {
      source,
      node: process.version,
      environment: 'serverless',
      vercelEnvironment: process.env.VERCEL_ENV || null
    }
  };
}
