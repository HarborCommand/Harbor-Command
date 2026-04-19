function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(body);
}

function buildRuntime() {
  return {
    source: "vercel-function",
    node: process.version,
    environment: process.env.VERCEL ? "vercel" : "node",
    vercelEnvironment: String(process.env.VERCEL_ENV ?? "").trim() || null,
  };
}

function getDatabaseUrl() {
  return String(process.env.DATABASE_URL ?? "").trim();
}

function shouldUseSsl(connectionString) {
  try {
    const parsed = new URL(connectionString);
    const sslMode = String(parsed.searchParams.get("sslmode") ?? "").trim().toLowerCase();
    if (sslMode === "disable") {
      return false;
    }

    return !/^(localhost|127(?:\.\d{1,3}){3})$/u.test(parsed.hostname);
  } catch {
    return true;
  }
}

export default async function handler(_req, res) {
  const timestamp = new Date().toISOString();
  const runtime = buildRuntime();
  const databaseUrl = getDatabaseUrl();

  if (!databaseUrl) {
    sendJson(res, 200, {
      ok: false,
      postgres: {
        configured: false,
        healthy: false,
        error: "DATABASE_URL is not configured.",
      },
      timestamp,
      runtime,
    });
    return;
  }

  let client = null;

  try {
    const pgModule = await import("pg");
    const Client = pgModule.Client || pgModule.default?.Client;

    if (!Client) {
      throw new Error("pg Client export was not found.");
    }

    client = new Client({
      connectionString: databaseUrl,
      ssl: shouldUseSsl(databaseUrl) ? { rejectUnauthorized: false } : false,
    });

    await client.connect();
    await client.query("SELECT 1");

    sendJson(res, 200, {
      ok: true,
      postgres: {
        configured: true,
        healthy: true,
      },
      timestamp,
      runtime,
    });
  } catch (error) {
    sendJson(res, 200, {
      ok: false,
      postgres: {
        configured: true,
        healthy: false,
        error: error instanceof Error ? error.message : "Unable to reach Postgres.",
      },
      timestamp,
      runtime,
    });
  } finally {
    if (client) {
      try {
        await client.end();
      } catch {
      }
    }
  }
}

