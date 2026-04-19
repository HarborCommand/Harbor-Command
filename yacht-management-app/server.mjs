import { createServer } from "node:http";
import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from "node:crypto";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { deflateSync, inflateSync } from "node:zlib";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
loadEnvironmentFiles(__dirname);

const args = process.argv.slice(2);
const portArg = args.find((arg) => arg.startsWith("--port="));
const port = Number.parseInt(portArg ? portArg.split("=")[1] : process.env.PORT ?? "8787", 10);
const resendApiKey = String(process.env.RESEND_API_KEY ?? "").trim();
const resendFromEmail = String(process.env.RESEND_FROM_EMAIL ?? "").trim();
const resendReplyTo = String(process.env.RESEND_REPLY_TO ?? "").trim();
const configuredPublicAppUrl = normalizePublicAppUrl(
  process.env.HARBOR_COMMAND_PUBLIC_URL ?? process.env.PUBLIC_APP_URL ?? ""
);
const localAppUrl = `http://127.0.0.1:${port}`;
const localAppOrigin = new URL(localAppUrl).origin;
const alternateLocalAppOrigin = `http://localhost:${port}`;
const ipv6LocalAppOrigin = `http://[::1]:${port}`;
const configuredPublicAppOrigin = configuredPublicAppUrl ? new URL(configuredPublicAppUrl).origin : "";
const inviteBaseUrl = configuredPublicAppUrl || localAppUrl;
const dataDirectory = path.join(__dirname, "data");
const databasePath = path.join(dataDirectory, "harbor-command.db");
const SESSION_COOKIE_NAME = "harbor_command_session";
const SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 14;
const AUTH_ROLES = new Set(["Captain", "Engineer", "Management", "Owner", "Crew"]);
const USER_ADMIN_ROLES = new Set(["Captain", "Management"]);
const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);
const ALLOWED_API_ORIGINS = new Set([
  localAppOrigin,
  alternateLocalAppOrigin,
  ipv6LocalAppOrigin,
  configuredPublicAppOrigin,
].filter(Boolean));
const MAX_REQUEST_BODY_BYTES = 10 * 1024 * 1024;
const rateLimitStore = new Map();
const APP_CONTENT_SECURITY_POLICY = [
  "default-src 'self'",
  "script-src 'self'",
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data: blob: https:",
  "font-src 'self' data:",
  "connect-src 'self' https://api.open-meteo.com https://geocoding-api.open-meteo.com",
  "object-src 'none'",
  "base-uri 'self'",
  "form-action 'self'",
  "frame-ancestors 'none'",
].join("; ");
const API_CONTENT_SECURITY_POLICY = "default-src 'none'; base-uri 'none'; frame-ancestors 'none'";
const RATE_LIMIT_RULES = [
  {
    name: "auth",
    windowMs: 10 * 60 * 1000,
    limit: 10,
    matches(request, url) {
      return request.method === "POST" && (
        url.pathname === "/api/auth/login"
        || url.pathname === "/api/auth/setup"
        || /^\/api\/invite\/[^/]+\/accept$/u.test(url.pathname)
      );
    },
  },
  {
    name: "access-management",
    windowMs: 10 * 60 * 1000,
    limit: 30,
    matches(request, url) {
      if (!MUTATING_METHODS.has(request.method)) {
        return false;
      }

      return (
        url.pathname === "/api/invites"
        || /^\/api\/invites\/\d+(?:\/send)?$/u.test(url.pathname)
        || url.pathname === "/api/users"
        || /^\/api\/users\/\d+$/u.test(url.pathname)
      );
    },
  },
  {
    name: "state-write",
    windowMs: 60 * 1000,
    limit: 90,
    matches(request, url) {
      return request.method === "PUT" && url.pathname === "/api/bootstrap";
    },
  },
];

mkdirSync(dataDirectory, { recursive: true });

const database = new DatabaseSync(databasePath);
database.exec(`
  PRAGMA foreign_keys = ON;

  CREATE TABLE IF NOT EXISTS app_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS vessels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    builder TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL,
    year_built INTEGER NOT NULL DEFAULT 0,
    vessel_type TEXT NOT NULL DEFAULT '',
    hull_material TEXT NOT NULL DEFAULT '',
    length INTEGER NOT NULL,
    beam REAL NOT NULL DEFAULT 0,
    draft REAL NOT NULL DEFAULT 0,
    guests INTEGER NOT NULL,
    status TEXT NOT NULL,
    berth TEXT NOT NULL,
    captain TEXT NOT NULL,
    location TEXT NOT NULL,
    fuel INTEGER NOT NULL,
    fuel_capacity INTEGER NOT NULL DEFAULT 0,
    water_tank INTEGER NOT NULL,
    water_capacity INTEGER NOT NULL DEFAULT 0,
    grey_tank INTEGER NOT NULL,
    grey_water_capacity INTEGER NOT NULL DEFAULT 0,
    black_tank_level INTEGER NOT NULL DEFAULT 0,
    black_water_capacity INTEGER NOT NULL DEFAULT 0,
    battery_status INTEGER NOT NULL DEFAULT 0,
    utilization INTEGER NOT NULL,
    next_service TEXT,
    engine_info TEXT NOT NULL DEFAULT '',
    generator_info TEXT NOT NULL DEFAULT '',
    photo_data_url TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    display_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS vessel_manufacturers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS vessel_models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    manufacturer_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    vessel_type TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (manufacturer_id, name),
    FOREIGN KEY (manufacturer_id) REFERENCES vessel_manufacturers(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS vessel_model_specs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id INTEGER NOT NULL,
    model_year INTEGER NOT NULL,
    vessel_type TEXT NOT NULL DEFAULT '',
    length REAL NOT NULL DEFAULT 0,
    beam REAL NOT NULL DEFAULT 0,
    draft REAL NOT NULL DEFAULT 0,
    fuel_capacity INTEGER NOT NULL DEFAULT 0,
    water_capacity INTEGER NOT NULL DEFAULT 0,
    black_water_capacity INTEGER NOT NULL DEFAULT 0,
    grey_water_capacity INTEGER NOT NULL DEFAULT 0,
    engine_info TEXT NOT NULL DEFAULT '',
    generator_info TEXT NOT NULL DEFAULT '',
    hull_material TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (model_id, model_year),
    FOREIGN KEY (model_id) REFERENCES vessel_models(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS engine_manufacturers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS generator_manufacturers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS user_vessels (
    vessel_id INTEGER PRIMARY KEY,
    manufacturer_id INTEGER,
    model_id INTEGER,
    model_spec_id INTEGER,
    model_year INTEGER NOT NULL DEFAULT 0,
    vessel_type TEXT NOT NULL DEFAULT '',
    hull_material TEXT NOT NULL DEFAULT '',
    is_custom INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE,
    FOREIGN KEY (manufacturer_id) REFERENCES vessel_manufacturers(id) ON DELETE SET NULL,
    FOREIGN KEY (model_id) REFERENCES vessel_models(id) ON DELETE SET NULL,
    FOREIGN KEY (model_spec_id) REFERENCES vessel_model_specs(id) ON DELETE SET NULL
  );

  CREATE TABLE IF NOT EXISTS maintenance_assets (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    template_id TEXT,
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL DEFAULT '',
    manufacturer TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    serial_number TEXT NOT NULL DEFAULT '',
    location TEXT NOT NULL DEFAULT '',
    meter_source_type TEXT NOT NULL DEFAULT 'none',
    meter_source_id TEXT,
    current_hours INTEGER NOT NULL DEFAULT 0,
    notes TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS maintenance_templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS maintenance_template_tasks (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'General',
    description TEXT NOT NULL DEFAULT '',
    default_priority TEXT NOT NULL DEFAULT 'Medium',
    interval_days INTEGER NOT NULL DEFAULT 0,
    interval_hours INTEGER NOT NULL DEFAULT 0,
    reminder_days INTEGER NOT NULL DEFAULT 0,
    reminder_hours INTEGER NOT NULL DEFAULT 0,
    recurrence_mode TEXT NOT NULL DEFAULT 'days',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (template_id) REFERENCES maintenance_templates(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS maintenance_logs (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    asset_id TEXT,
    template_id TEXT,
    template_task_id TEXT,
    due_date TEXT,
    due_hours INTEGER NOT NULL DEFAULT 0,
    last_completed TEXT,
    last_completed_hours INTEGER NOT NULL DEFAULT 0,
    interval_days INTEGER NOT NULL DEFAULT 7,
    interval_hours INTEGER NOT NULL DEFAULT 0,
    reminder_days INTEGER NOT NULL DEFAULT 3,
    reminder_hours INTEGER NOT NULL DEFAULT 0,
    recurrence_mode TEXT NOT NULL DEFAULT 'days',
    meter_source_type TEXT NOT NULL DEFAULT 'none',
    meter_source_id TEXT,
    is_custom INTEGER NOT NULL DEFAULT 1,
    notes TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS inventory (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    location TEXT NOT NULL DEFAULT '',
    quantity REAL NOT NULL DEFAULT 0,
    unit TEXT NOT NULL DEFAULT '',
    minimum_quantity REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS expenses (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    vendor TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    amount REAL NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USD',
    expense_date TEXT,
    status TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS work_orders (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    due_date TEXT,
    notes TEXT NOT NULL DEFAULT '',
    maintenance_log_id TEXT,
    origin_type TEXT NOT NULL DEFAULT 'manual',
    completed_at TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS charters (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    client TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    berth TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS crew_members (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    certification TEXT NOT NULL DEFAULT '',
    rotation TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    item TEXT NOT NULL,
    report_date TEXT NOT NULL,
    work_done TEXT NOT NULL DEFAULT '',
    systems_checked TEXT NOT NULL DEFAULT '',
    issues TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS weekly_reports (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    week_start TEXT NOT NULL,
    week_end TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    vessel_snapshot_json TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS weekly_report_entries (
    id TEXT PRIMARY KEY,
    report_id TEXT NOT NULL,
    item TEXT NOT NULL,
    report_date TEXT NOT NULL,
    work_done TEXT NOT NULL DEFAULT '',
    systems_checked TEXT NOT NULL DEFAULT '',
    issues TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    source_work_order_id TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (report_id) REFERENCES weekly_reports(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS maintenance_history (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    maintenance_log_id TEXT NOT NULL,
    asset_id TEXT,
    template_task_id TEXT,
    work_order_id TEXT,
    source TEXT NOT NULL DEFAULT 'manual',
    completed_at TEXT NOT NULL,
    completion_date TEXT NOT NULL,
    completed_hours INTEGER NOT NULL DEFAULT 0,
    work_done TEXT NOT NULL DEFAULT '',
    systems_checked TEXT NOT NULL DEFAULT '',
    issues TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE,
    FOREIGN KEY (maintenance_log_id) REFERENCES maintenance_logs(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS vendors (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    contact TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS voyages (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    route TEXT NOT NULL,
    departure TEXT NOT NULL,
    weather TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS engines (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    manufacturer TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    rating TEXT NOT NULL DEFAULT '',
    hours INTEGER NOT NULL DEFAULT 0,
    last_service_hours INTEGER NOT NULL DEFAULT 0,
    service_interval_hours INTEGER NOT NULL DEFAULT 0,
    last_service_date TEXT,
    next_service_date TEXT,
    notes TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS generators (
    id TEXT PRIMARY KEY,
    vessel_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    manufacturer TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    rating TEXT NOT NULL DEFAULT '',
    hours INTEGER NOT NULL DEFAULT 0,
    last_service_hours INTEGER NOT NULL DEFAULT 0,
    service_interval_hours INTEGER NOT NULL DEFAULT 0,
    last_service_date TEXT,
    next_service_date TEXT,
    notes TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS app_preferences (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS invites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    role TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    created_by INTEGER,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at TEXT,
    revoked_at TEXT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
  );

  CREATE TABLE IF NOT EXISTS user_vessel_access (
    user_id INTEGER NOT NULL,
    vessel_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, vessel_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS invite_vessel_access (
    invite_id INTEGER NOT NULL,
    vessel_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (invite_id, vessel_id),
    FOREIGN KEY (invite_id) REFERENCES invites(id) ON DELETE CASCADE,
    FOREIGN KEY (vessel_id) REFERENCES vessels(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_maintenance_logs_vessel_id ON maintenance_logs(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_vessel_models_manufacturer_id ON vessel_models(manufacturer_id);
  CREATE INDEX IF NOT EXISTS idx_vessel_model_specs_model_id ON vessel_model_specs(model_id);
  CREATE INDEX IF NOT EXISTS idx_vessel_model_specs_year ON vessel_model_specs(model_year);
  CREATE INDEX IF NOT EXISTS idx_inventory_vessel_id ON inventory(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_expenses_vessel_id ON expenses(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_work_orders_vessel_id ON work_orders(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_charters_vessel_id ON charters(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_crew_members_vessel_id ON crew_members(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_reports_vessel_id ON reports(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_weekly_reports_vessel_id ON weekly_reports(vessel_id);
  CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_reports_vessel_week ON weekly_reports(vessel_id, week_start, week_end);
  CREATE INDEX IF NOT EXISTS idx_weekly_report_entries_report_id ON weekly_report_entries(report_id);
  CREATE INDEX IF NOT EXISTS idx_vendors_vessel_id ON vendors(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_voyages_vessel_id ON voyages(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_user_vessel_access_user_id ON user_vessel_access(user_id);
  CREATE INDEX IF NOT EXISTS idx_user_vessel_access_vessel_id ON user_vessel_access(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_invite_vessel_access_invite_id ON invite_vessel_access(invite_id);
  CREATE INDEX IF NOT EXISTS idx_invite_vessel_access_vessel_id ON invite_vessel_access(vessel_id);
`);

function ensureUsersColumn(columnName, definition) {
  const columns = database.prepare("PRAGMA table_info(users)").all();
  if (!columns.some((column) => column.name === columnName)) {
    database.exec(`ALTER TABLE users ADD COLUMN ${definition}`);
  }
}

function ensureVesselsColumn(columnName, definition) {
  const columns = database.prepare("PRAGMA table_info(vessels)").all();
  if (!columns.some((column) => column.name === columnName)) {
    database.exec(`ALTER TABLE vessels ADD COLUMN ${definition}`);
  }
}

function ensureTableColumn(tableName, columnName, definition) {
  const columns = database.prepare(`PRAGMA table_info(${tableName})`).all();
  if (!columns.some((column) => column.name === columnName)) {
    database.exec(`ALTER TABLE ${tableName} ADD COLUMN ${definition}`);
  }
}

ensureVesselsColumn("builder", "builder TEXT NOT NULL DEFAULT ''");
ensureVesselsColumn("year_built", "year_built INTEGER NOT NULL DEFAULT 0");
ensureVesselsColumn("vessel_type", "vessel_type TEXT NOT NULL DEFAULT ''");
ensureVesselsColumn("hull_material", "hull_material TEXT NOT NULL DEFAULT ''");
ensureVesselsColumn("beam", "beam REAL NOT NULL DEFAULT 0");
ensureVesselsColumn("draft", "draft REAL NOT NULL DEFAULT 0");
ensureVesselsColumn("fuel_capacity", "fuel_capacity INTEGER NOT NULL DEFAULT 0");
ensureVesselsColumn("water_capacity", "water_capacity INTEGER NOT NULL DEFAULT 0");
ensureVesselsColumn("grey_water_capacity", "grey_water_capacity INTEGER NOT NULL DEFAULT 0");
ensureVesselsColumn("black_tank_level", "black_tank_level INTEGER NOT NULL DEFAULT 0");
ensureVesselsColumn("black_water_capacity", "black_water_capacity INTEGER NOT NULL DEFAULT 0");
ensureVesselsColumn("battery_status", "battery_status INTEGER NOT NULL DEFAULT 0");
ensureVesselsColumn("engine_info", "engine_info TEXT NOT NULL DEFAULT ''");
ensureVesselsColumn("generator_info", "generator_info TEXT NOT NULL DEFAULT ''");
ensureVesselsColumn("display_order", "display_order INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("work_orders", "week_start", "week_start TEXT");
ensureTableColumn("work_orders", "week_end", "week_end TEXT");
ensureTableColumn("work_orders", "entry_date", "entry_date TEXT");
ensureTableColumn("work_orders", "work_done", "work_done TEXT NOT NULL DEFAULT ''");
ensureTableColumn("work_orders", "systems_checked", "systems_checked TEXT NOT NULL DEFAULT ''");
ensureTableColumn("work_orders", "issues", "issues TEXT NOT NULL DEFAULT ''");
ensureTableColumn("work_orders", "maintenance_log_id", "maintenance_log_id TEXT");
ensureTableColumn("work_orders", "origin_type", "origin_type TEXT NOT NULL DEFAULT 'manual'");
ensureTableColumn("work_orders", "completed_at", "completed_at TEXT");
ensureTableColumn("maintenance_logs", "asset_id", "asset_id TEXT");
ensureTableColumn("maintenance_logs", "template_id", "template_id TEXT");
ensureTableColumn("maintenance_logs", "template_task_id", "template_task_id TEXT");
ensureTableColumn("maintenance_logs", "due_hours", "due_hours INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("maintenance_logs", "last_completed_hours", "last_completed_hours INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("maintenance_logs", "interval_hours", "interval_hours INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("maintenance_logs", "reminder_hours", "reminder_hours INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("maintenance_logs", "recurrence_mode", "recurrence_mode TEXT NOT NULL DEFAULT 'days'");
ensureTableColumn("maintenance_logs", "meter_source_type", "meter_source_type TEXT NOT NULL DEFAULT 'none'");
ensureTableColumn("maintenance_logs", "meter_source_id", "meter_source_id TEXT");
ensureTableColumn("maintenance_logs", "is_custom", "is_custom INTEGER NOT NULL DEFAULT 1");
ensureTableColumn("maintenance_history", "asset_id", "asset_id TEXT");
ensureTableColumn("maintenance_history", "template_task_id", "template_task_id TEXT");
ensureTableColumn("maintenance_history", "completed_hours", "completed_hours INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("engines", "last_service_hours", "last_service_hours INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("generators", "last_service_hours", "last_service_hours INTEGER NOT NULL DEFAULT 0");
ensureTableColumn("weekly_reports", "vessel_snapshot_json", "vessel_snapshot_json TEXT NOT NULL DEFAULT ''");
ensureTableColumn("weekly_report_entries", "source_work_order_id", "source_work_order_id TEXT");

ensureUsersColumn("is_active", "is_active INTEGER NOT NULL DEFAULT 1");

database.exec(`
  CREATE INDEX IF NOT EXISTS idx_weekly_report_entries_source_work_order_id ON weekly_report_entries(source_work_order_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_assets_vessel_id ON maintenance_assets(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_logs_asset_id ON maintenance_logs(asset_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_logs_template_id ON maintenance_logs(template_id);
  CREATE INDEX IF NOT EXISTS idx_work_orders_maintenance_log_id ON work_orders(maintenance_log_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_history_vessel_id ON maintenance_history(vessel_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_history_log_id ON maintenance_history(maintenance_log_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_history_asset_id ON maintenance_history(asset_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_history_work_order_id ON maintenance_history(work_order_id);
  CREATE INDEX IF NOT EXISTS idx_maintenance_template_tasks_template_id ON maintenance_template_tasks(template_id);
`);
const seedDirectory = path.join(__dirname, "seeds");
const VESSEL_CATALOG_SEED = readSeedArrayFile("vessel-catalog.json");
const ENGINE_MANUFACTURER_SEED = readSeedArrayFile("engine-manufacturers.json");
const GENERATOR_MANUFACTURER_SEED = readSeedArrayFile("generator-manufacturers.json");
const MAINTENANCE_TEMPLATE_SEED = readSeedArrayFile("maintenance-templates.json");

function readSeedArrayFile(fileName) {
  const filePath = path.join(seedDirectory, fileName);
  if (!existsSync(filePath)) {
    return [];
  }

  try {
    const parsed = JSON.parse(readFileSync(filePath, "utf8"));
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    console.warn(`Unable to read Harbor Command seed file: ${fileName}`, error);
    return [];
  }
}

function slugifyCatalogName(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function seedVesselCatalogIfNeeded() {
  if (!Array.isArray(VESSEL_CATALOG_SEED) || !VESSEL_CATALOG_SEED.length) {
    return;
  }

  const timestamp = nowIso();
  const insertManufacturer = database.prepare(`
    INSERT INTO vessel_manufacturers (name, slug, created_at, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(slug) DO UPDATE SET
      name = excluded.name,
      updated_at = excluded.updated_at
  `);
  const selectManufacturer = database.prepare("SELECT id FROM vessel_manufacturers WHERE slug = ?");
  const insertModel = database.prepare(`
    INSERT INTO vessel_models (manufacturer_id, name, vessel_type, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(manufacturer_id, name) DO UPDATE SET
      vessel_type = excluded.vessel_type,
      updated_at = excluded.updated_at
  `);
  const selectModel = database.prepare("SELECT id FROM vessel_models WHERE manufacturer_id = ? AND name = ?");
  const insertSpec = database.prepare(`
    INSERT INTO vessel_model_specs (
      model_id, model_year, vessel_type, length, beam, draft, fuel_capacity, water_capacity,
      black_water_capacity, grey_water_capacity, engine_info, generator_info, hull_material, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(model_id, model_year) DO UPDATE SET
      vessel_type = excluded.vessel_type,
      length = excluded.length,
      beam = excluded.beam,
      draft = excluded.draft,
      fuel_capacity = excluded.fuel_capacity,
      water_capacity = excluded.water_capacity,
      black_water_capacity = excluded.black_water_capacity,
      grey_water_capacity = excluded.grey_water_capacity,
      engine_info = excluded.engine_info,
      generator_info = excluded.generator_info,
      hull_material = excluded.hull_material,
      updated_at = excluded.updated_at
  `);

  runInTransaction(() => {
    VESSEL_CATALOG_SEED.forEach((entry) => {
      const manufacturerName = String(entry?.manufacturer || "").trim();
      const modelName = String(entry?.model || "").trim();
      const year = Number.parseInt(String(entry?.year || "0"), 10);
      if (!manufacturerName || !modelName || !Number.isFinite(year) || year <= 0) {
        return;
      }

      const manufacturerSlug = slugifyCatalogName(manufacturerName);
      let manufacturerId = Number(selectManufacturer.get(manufacturerSlug)?.id || 0);
      if (!manufacturerId) {
        insertManufacturer.run(manufacturerName, manufacturerSlug, timestamp, timestamp);
        manufacturerId = Number(selectManufacturer.get(manufacturerSlug)?.id || 0);
      } else {
        insertManufacturer.run(manufacturerName, manufacturerSlug, timestamp, timestamp);
      }

      let modelId = Number(selectModel.get(manufacturerId, modelName)?.id || 0);
      if (!modelId) {
        insertModel.run(manufacturerId, modelName, String(entry?.vesselType || "").trim(), timestamp, timestamp);
        modelId = Number(selectModel.get(manufacturerId, modelName)?.id || 0);
      } else {
        insertModel.run(manufacturerId, modelName, String(entry?.vesselType || "").trim(), timestamp, timestamp);
      }

      insertSpec.run(
        modelId,
        year,
        String(entry?.vesselType || "").trim(),
        Number(entry?.length || 0),
        Number(entry?.beam || 0),
        Number(entry?.draft || 0),
        Number(entry?.fuelCapacity || 0),
        Number(entry?.waterCapacity || 0),
        Number(entry?.blackWaterCapacity || 0),
        Number(entry?.greyWaterCapacity || 0),
        String(entry?.engineInfo || "").trim(),
        String(entry?.generatorInfo || "").trim(),
        String(entry?.hullMaterial || "").trim(),
        timestamp,
        timestamp
      );
    });
  });
}

function seedNamedManufacturerCatalog(tableName, seedEntries) {
  if (!Array.isArray(seedEntries) || !seedEntries.length) {
    return;
  }

  const timestamp = nowIso();
  const insertStatement = database.prepare(`
    INSERT INTO ${tableName} (name, slug, created_at, updated_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(slug) DO UPDATE SET
      name = excluded.name,
      updated_at = excluded.updated_at
  `);

  runInTransaction(() => {
    seedEntries.forEach((entry) => {
      const name = String(typeof entry === "string" ? entry : entry?.name || "").trim();
      if (!name) {
        return;
      }

      insertStatement.run(name, slugifyCatalogName(name), timestamp, timestamp);
    });
  });
}

function loadNamedManufacturerCatalog(tableName) {
  return database.prepare(`
    SELECT id, name, slug
    FROM ${tableName}
    ORDER BY name COLLATE NOCASE ASC, id ASC
  `).all().map((row) => ({
    id: String(row.id),
    name: row.name,
    slug: row.slug,
  }));
}

function loadVesselCatalog() {
  const manufacturers = loadNamedManufacturerCatalog("vessel_manufacturers");

  const models = database.prepare(`
    SELECT id, manufacturer_id, name, vessel_type
    FROM vessel_models
    ORDER BY name COLLATE NOCASE ASC, id ASC
  `).all().map((row) => ({
    id: String(row.id),
    manufacturerId: String(row.manufacturer_id),
    name: row.name,
    vesselType: row.vessel_type || "",
  }));

  const specs = database.prepare(`
    SELECT
      specs.id,
      specs.model_id,
      specs.model_year,
      specs.vessel_type,
      specs.length,
      specs.beam,
      specs.draft,
      specs.fuel_capacity,
      specs.water_capacity,
      specs.black_water_capacity,
      specs.grey_water_capacity,
      specs.engine_info,
      specs.generator_info,
      specs.hull_material,
      models.manufacturer_id,
      models.name AS model_name,
      manufacturers.name AS manufacturer_name
    FROM vessel_model_specs specs
    INNER JOIN vessel_models models ON models.id = specs.model_id
    INNER JOIN vessel_manufacturers manufacturers ON manufacturers.id = models.manufacturer_id
    ORDER BY specs.model_year DESC, manufacturers.name COLLATE NOCASE ASC, models.name COLLATE NOCASE ASC, specs.id ASC
  `).all().map((row) => ({
    id: String(row.id),
    modelId: String(row.model_id),
    manufacturerId: String(row.manufacturer_id),
    manufacturerName: row.manufacturer_name,
    modelName: row.model_name,
    year: Number(row.model_year || 0),
    vesselType: row.vessel_type || "",
    length: Number(row.length || 0),
    beam: Number(row.beam || 0),
    draft: Number(row.draft || 0),
    fuelCapacity: Number(row.fuel_capacity || 0),
    waterCapacity: Number(row.water_capacity || 0),
    blackWaterCapacity: Number(row.black_water_capacity || 0),
    greyWaterCapacity: Number(row.grey_water_capacity || 0),
    engineInfo: row.engine_info || "",
    generatorInfo: row.generator_info || "",
    hullMaterial: row.hull_material || "",
  }));

  const years = Array.from(new Set(specs.map((spec) => Number(spec.year || 0)).filter(Boolean))).sort((left, right) => right - left);

  return {
    years,
    manufacturers,
    models,
    specs,
    engineManufacturers: loadNamedManufacturerCatalog("engine_manufacturers"),
    generatorManufacturers: loadNamedManufacturerCatalog("generator_manufacturers"),
    maintenanceTemplates: loadMaintenanceTemplateCatalog(),
  };
}

seedVesselCatalogIfNeeded();
seedNamedManufacturerCatalog("engine_manufacturers", ENGINE_MANUFACTURER_SEED);
seedNamedManufacturerCatalog("generator_manufacturers", GENERATOR_MANUFACTURER_SEED);
seedMaintenanceTemplateCatalogIfNeeded();

function normalizeMaintenanceRecurrenceMode(value) {
  const normalized = normalizeTextValue(value, "days").trim().toLowerCase();
  if (normalized === "hours" || normalized === "days-or-hours") {
    return normalized;
  }
  return "days";
}

function seedMaintenanceTemplateCatalogIfNeeded() {
  if (!Array.isArray(MAINTENANCE_TEMPLATE_SEED) || !MAINTENANCE_TEMPLATE_SEED.length) {
    return;
  }

  const timestamp = nowIso();
  const insertTemplateStatement = database.prepare(`
    INSERT INTO maintenance_templates (id, name, asset_type, description, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name,
      asset_type = excluded.asset_type,
      description = excluded.description,
      updated_at = excluded.updated_at
  `);
  const insertTemplateTaskStatement = database.prepare(`
    INSERT INTO maintenance_template_tasks (
      id, template_id, title, category, description, default_priority,
      interval_days, interval_hours, reminder_days, reminder_hours, recurrence_mode,
      sort_order, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      title = excluded.title,
      category = excluded.category,
      description = excluded.description,
      default_priority = excluded.default_priority,
      interval_days = excluded.interval_days,
      interval_hours = excluded.interval_hours,
      reminder_days = excluded.reminder_days,
      reminder_hours = excluded.reminder_hours,
      recurrence_mode = excluded.recurrence_mode,
      sort_order = excluded.sort_order,
      updated_at = excluded.updated_at
  `);

  runInTransaction(() => {
    MAINTENANCE_TEMPLATE_SEED.forEach((template, templateIndex) => {
      const templateId = normalizeTextValue(template?.id, "").trim();
      const templateName = normalizeTextValue(template?.name, "").trim();
      if (!templateId || !templateName) {
        return;
      }

      insertTemplateStatement.run(
        templateId,
        templateName,
        normalizeTextValue(template?.assetType, ""),
        normalizeTextValue(template?.description, ""),
        timestamp,
        timestamp
      );

      const tasks = Array.isArray(template?.tasks) ? template.tasks : [];
      tasks.forEach((task, taskIndex) => {
        const taskId = normalizeTextValue(task?.id, `${templateId}-task-${taskIndex + 1}`).trim();
        if (!taskId) {
          return;
        }
        insertTemplateTaskStatement.run(
          taskId,
          templateId,
          normalizeTextValue(task?.title, `Template task ${taskIndex + 1}`),
          normalizeTextValue(task?.category, templateName),
          normalizeTextValue(task?.description, ""),
          normalizeTextValue(task?.defaultPriority, "Medium"),
          normalizeIntegerValue(task?.intervalDays, 0),
          normalizeIntegerValue(task?.intervalHours, 0),
          normalizeIntegerValue(task?.reminderDays, 0),
          normalizeIntegerValue(task?.reminderHours, 0),
          normalizeMaintenanceRecurrenceMode(task?.recurrenceMode),
          taskIndex,
          timestamp,
          timestamp
        );
      });
    });
  });
}

function loadMaintenanceTemplateCatalog() {
  const templates = database.prepare(`
    SELECT id, name, asset_type, description
    FROM maintenance_templates
    ORDER BY name COLLATE NOCASE ASC, id ASC
  `).all();

  const tasks = database.prepare(`
    SELECT
      id,
      template_id,
      title,
      category,
      description,
      default_priority,
      interval_days,
      interval_hours,
      reminder_days,
      reminder_hours,
      recurrence_mode,
      sort_order
    FROM maintenance_template_tasks
    ORDER BY template_id ASC, sort_order ASC, id ASC
  `).all();

  return templates.map((template) => ({
    id: String(template.id),
    name: template.name,
    assetType: template.asset_type || "",
    description: template.description || "",
    tasks: tasks
      .filter((task) => String(task.template_id) === String(template.id))
      .map((task) => ({
        id: String(task.id),
        title: task.title,
        category: task.category || "General",
        description: task.description || "",
        defaultPriority: task.default_priority || "Medium",
        intervalDays: Number(task.interval_days || 0),
        intervalHours: Number(task.interval_hours || 0),
        reminderDays: Number(task.reminder_days || 0),
        reminderHours: Number(task.reminder_hours || 0),
        recurrenceMode: normalizeMaintenanceRecurrenceMode(task.recurrence_mode),
        sortOrder: Number(task.sort_order || 0),
      })),
  }));
}

const selectLegacyStateStatement = database.prepare("SELECT state_json, updated_at FROM app_state WHERE id = 1");
const countUsersStatement = database.prepare("SELECT COUNT(*) AS total FROM users");
const insertUserStatement = database.prepare(`
  INSERT INTO users (name, email, password_hash, role, created_at)
  VALUES (?, ?, ?, ?, ?)
`);
const selectUserByEmailStatement = database.prepare(`
  SELECT id, name, email, password_hash, role, is_active, created_at
  FROM users
  WHERE email = ?
`);
const selectUserByIdStatement = database.prepare(`
  SELECT id, name, email, role, is_active, created_at
  FROM users
  WHERE id = ?
`);
const selectManagedUserByIdStatement = database.prepare(`
  SELECT id, name, email, role, is_active, created_at
  FROM users
  WHERE id = ?
`);
const selectUsersStatement = database.prepare(`
  SELECT id, name, email, role, is_active, created_at
  FROM users
  ORDER BY datetime(created_at) DESC, id DESC
`);
const selectManageableVesselsStatement = database.prepare(`
  SELECT id, name, location, berth, status
  FROM vessels
  ORDER BY display_order ASC, id ASC
`);
const selectUserVesselAccessStatement = database.prepare(`
  SELECT vessel_id
  FROM user_vessel_access
  WHERE user_id = ?
  ORDER BY vessel_id ASC
`);
const deleteUserVesselAccessStatement = database.prepare("DELETE FROM user_vessel_access WHERE user_id = ?");
const insertUserVesselAccessStatement = database.prepare(`
  INSERT OR IGNORE INTO user_vessel_access (user_id, vessel_id, created_at)
  VALUES (?, ?, ?)
`);
const selectInviteVesselAccessStatement = database.prepare(`
  SELECT vessel_id
  FROM invite_vessel_access
  WHERE invite_id = ?
  ORDER BY vessel_id ASC
`);
const deleteInviteVesselAccessStatement = database.prepare("DELETE FROM invite_vessel_access WHERE invite_id = ?");
const insertInviteVesselAccessStatement = database.prepare(`
  INSERT OR IGNORE INTO invite_vessel_access (invite_id, vessel_id, created_at)
  VALUES (?, ?, ?)
`);
const selectInviteByTokenStatement = database.prepare(`
  SELECT id, email, role, token, created_by, created_at, expires_at, used_at, revoked_at
  FROM invites
  WHERE token = ?
`);
const selectInviteByIdStatement = database.prepare(`
  SELECT id, email, role, token, created_by, created_at, expires_at, used_at, revoked_at
  FROM invites
  WHERE id = ?
`);
const selectPendingInviteByEmailStatement = database.prepare(`
  SELECT id, email, role, token, created_by, created_at, expires_at, used_at, revoked_at
  FROM invites
  WHERE email = ? AND used_at IS NULL AND revoked_at IS NULL AND expires_at > ?
  ORDER BY datetime(created_at) DESC
  LIMIT 1
`);
const selectPendingInvitesStatement = database.prepare(`
  SELECT id, email, role, token, created_by, created_at, expires_at, used_at, revoked_at
  FROM invites
  WHERE used_at IS NULL AND revoked_at IS NULL AND expires_at > ?
  ORDER BY datetime(created_at) DESC, id DESC
`);
const insertInviteStatement = database.prepare(`
  INSERT INTO invites (email, role, token, created_by, created_at, expires_at)
  VALUES (?, ?, ?, ?, ?, ?)
`);
const markInviteUsedStatement = database.prepare("UPDATE invites SET used_at = ? WHERE id = ?");
const revokeInviteStatement = database.prepare("UPDATE invites SET revoked_at = ? WHERE id = ?");
const insertSessionStatement = database.prepare(`
  INSERT INTO sessions (token, user_id, created_at, expires_at)
  VALUES (?, ?, ?, ?)
`);
const selectSessionUserStatement = database.prepare(`
  SELECT users.id, users.name, users.email, users.role
  FROM sessions
  JOIN users ON users.id = sessions.user_id
  WHERE sessions.token = ? AND sessions.expires_at > ? AND users.is_active = 1
`);
const deleteSessionStatement = database.prepare("DELETE FROM sessions WHERE token = ?");
const deleteUserSessionsStatement = database.prepare("DELETE FROM sessions WHERE user_id = ?");
const updateUserActiveStatement = database.prepare("UPDATE users SET is_active = ? WHERE id = ?");
const deleteUserStatement = database.prepare("DELETE FROM users WHERE id = ?");
const deleteExpiredSessionsStatement = database.prepare("DELETE FROM sessions WHERE expires_at <= ?");
const countActiveAdminUsersStatement = database.prepare(`
  SELECT COUNT(*) AS total
  FROM users
  WHERE is_active = 1 AND role IN ('Captain', 'Management')
`);

const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
};

function loadEnvironmentFiles(baseDirectory) {
  const envValues = {};

  [".env", ".env.local"].forEach((fileName) => {
    const fullPath = path.join(baseDirectory, fileName);
    if (!existsSync(fullPath)) {
      return;
    }

    const fileContents = readFileSync(fullPath, "utf8");
    parseEnvironmentFile(fileContents, envValues);
  });

  Object.entries(envValues).forEach(([key, value]) => {
    if (typeof process.env[key] === "undefined") {
      process.env[key] = value;
    }
  });
}

function parseEnvironmentFile(contents, target) {
  contents
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
    .forEach((line) => {
      const separatorIndex = line.indexOf("=");
      if (separatorIndex === -1) {
        return;
      }

      const key = line.slice(0, separatorIndex).trim();
      let value = line.slice(separatorIndex + 1).trim();

      if (
        (value.startsWith('"') && value.endsWith('"'))
        || (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }

      if (key) {
        target[key] = value;
      }
    });
}

function normalizePublicAppUrl(value) {
  const rawValue = String(value ?? "").trim();
  if (!rawValue) {
    return "";
  }

  try {
    const url = new URL(rawValue);
    url.pathname = url.pathname.replace(/\/+$/u, "");
    url.search = "";
    url.hash = "";
    return url.toString().replace(/\/$/u, "");
  } catch {
    return "";
  }
}

function escapeHtmlForEmail(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDateTimeForEmail(dateString) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(new Date(dateString));
}

function applyCommonSecurityHeaders(response) {
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.setHeader("X-Frame-Options", "DENY");
  response.setHeader("Referrer-Policy", "no-referrer");
  response.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  response.setHeader("Cross-Origin-Resource-Policy", "same-origin");
  response.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=(self)");
  response.setHeader("X-Robots-Tag", "noindex, nofollow");
}

function normalizeOrigin(value) {
  if (!value) {
    return "";
  }

  try {
    return new URL(String(value)).origin;
  } catch {
    return "";
  }
}

function isLoopbackHostname(hostname = "") {
  const normalizedHost = String(hostname ?? "").trim().toLowerCase();
  return normalizedHost === "localhost"
    || normalizedHost === "::1"
    || normalizedHost === "[::1]"
    || /^127(?:\.\d{1,3}){3}$/u.test(normalizedHost);
}

function isLoopbackHostHeader(hostHeader = "") {
  const normalizedHostHeader = String(hostHeader ?? "").trim().toLowerCase();
  if (!normalizedHostHeader) {
    return false;
  }

  const hostOnly = normalizedHostHeader
    .replace(/^\[/u, "")
    .replace(/\](:\d+)?$/u, "")
    .replace(/:\d+$/u, "");

  return isLoopbackHostname(hostOnly);
}

function isLoopbackRemoteAddress(remoteAddress = "") {
  const normalizedAddress = String(remoteAddress ?? "").trim().toLowerCase();
  if (!normalizedAddress) {
    return false;
  }

  return normalizedAddress === "::1"
    || normalizedAddress === "::ffff:127.0.0.1"
    || /^::ffff:127(?:\.\d{1,3}){3}$/u.test(normalizedAddress)
    || /^127(?:\.\d{1,3}){3}$/u.test(normalizedAddress);
}

function isTrustedLocalRequest(request) {
  return isLoopbackHostHeader(request?.headers?.host)
    || isLoopbackRemoteAddress(request?.socket?.remoteAddress);
}

function isAllowedLoopbackRequestUrl(value) {
  if (!value) {
    return false;
  }

  try {
    const parsedUrl = new URL(String(value));
    const parsedPort = parsedUrl.port || (parsedUrl.protocol === "https:" ? "443" : "80");
    return parsedUrl.protocol === "http:"
      && parsedPort === String(port)
      && isLoopbackHostname(parsedUrl.hostname);
  } catch {
    return false;
  }
}

function isAllowedApiOrigin(origin) {
  if (!origin) {
    return false;
  }

  if (ALLOWED_API_ORIGINS.has(origin)) {
    return true;
  }

  try {
    const parsedOrigin = new URL(origin);
    const parsedPort = parsedOrigin.port || (parsedOrigin.protocol === "https:" ? "443" : "80");
    return parsedOrigin.protocol === "http:" && parsedPort === String(port) && isLoopbackHostname(parsedOrigin.hostname);
  } catch {
    return false;
  }
}

function applyApiHeaders(request, response) {
  applyCommonSecurityHeaders(response);
  response.setHeader("Content-Security-Policy", API_CONTENT_SECURITY_POLICY);
  response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
  response.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Requested-With");
  response.setHeader("Cache-Control", "no-store");
  response.setHeader("Vary", "Origin");

  const requestOrigin = normalizeOrigin(request?.headers?.origin);
  if (isAllowedApiOrigin(requestOrigin)) {
    response.setHeader("Access-Control-Allow-Origin", requestOrigin);
    response.setHeader("Access-Control-Allow-Credentials", "true");
  }
}

function applyStaticHeaders(response) {
  applyCommonSecurityHeaders(response);
  response.setHeader("Content-Security-Policy", APP_CONTENT_SECURITY_POLICY);
}

function sendJson(response, statusCode, payload, extraHeaders = {}) {
  const body = JSON.stringify(payload);
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    ...extraHeaders,
  });
  response.end(body);
}

function sendEmpty(response, statusCode, extraHeaders = {}) {
  response.writeHead(statusCode, {
    "Content-Length": 0,
    ...extraHeaders,
  });
  response.end();
}

function sendBuffer(response, statusCode, contentType, buffer, extraHeaders = {}) {
  response.writeHead(statusCode, {
    "Content-Type": contentType,
    "Content-Length": buffer.length,
    ...extraHeaders,
  });
  response.end(buffer);
}

function readRequestBody(request) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let totalBytes = 0;
    let hasRejected = false;

    request.on("data", (chunk) => {
      totalBytes += chunk.length;
      if (totalBytes > MAX_REQUEST_BODY_BYTES) {
        hasRejected = true;
        reject(new Error("Request body is too large."));
        request.destroy();
        return;
      }

      chunks.push(chunk);
    });

    request.on("end", () => {
      if (hasRejected) {
        return;
      }

      resolve(Buffer.concat(chunks).toString("utf8"));
    });

    request.on("error", (error) => {
      reject(error);
    });
  });
}

function parseJsonBody(body) {
  return JSON.parse(body);
}

function nowIso() {
  return new Date().toISOString();
}

function countUsers() {
  return countUsersStatement.get().total;
}

function normalizeEmail(email) {
  return String(email ?? "").trim().toLowerCase();
}

function normalizeVesselIdArray(values) {
  if (!Array.isArray(values)) {
    return [];
  }

  const seen = new Set();
  return values
    .map((value) => normalizeIntegerValue(value, 0))
    .filter((value) => value > 0)
    .filter((value) => {
      if (seen.has(value)) {
        return false;
      }
      seen.add(value);
      return true;
    });
}

function hasFullFleetAccess(subject) {
  const role = typeof subject === "string" ? subject : subject?.role;
  return USER_ADMIN_ROLES.has(String(role ?? "").trim());
}

function listAllVesselIds() {
  return selectManageableVesselsStatement
    .all()
    .map((row) => normalizeIntegerValue(row.id, 0))
    .filter((value) => value > 0);
}

function resolveExistingVesselIds(vesselIds) {
  const knownIds = new Set(listAllVesselIds());
  return normalizeVesselIdArray(vesselIds).filter((vesselId) => knownIds.has(vesselId));
}

function getUserAccessibleVesselIds(userOrUserId) {
  const userId = typeof userOrUserId === "object"
    ? normalizeIntegerValue(userOrUserId?.id, 0)
    : normalizeIntegerValue(userOrUserId, 0);

  if (userId <= 0) {
    return [];
  }

  return selectUserVesselAccessStatement
    .all(userId)
    .map((row) => normalizeIntegerValue(row.vessel_id, 0))
    .filter((value) => value > 0);
}

function getInviteAccessibleVesselIds(inviteOrInviteId) {
  const inviteId = typeof inviteOrInviteId === "object"
    ? normalizeIntegerValue(inviteOrInviteId?.id, 0)
    : normalizeIntegerValue(inviteOrInviteId, 0);

  if (inviteId <= 0) {
    return [];
  }

  return selectInviteVesselAccessStatement
    .all(inviteId)
    .map((row) => normalizeIntegerValue(row.vessel_id, 0))
    .filter((value) => value > 0);
}

function getEffectiveVesselIdsForUser(user) {
  if (!user) {
    return [];
  }

  return hasFullFleetAccess(user) ? listAllVesselIds() : getUserAccessibleVesselIds(user);
}

function listManageableVessels(user) {
  const rows = selectManageableVesselsStatement.all();
  const allowedIds = user && !hasFullFleetAccess(user)
    ? new Set(getUserAccessibleVesselIds(user))
    : null;

  return rows
    .filter((row) => !allowedIds || allowedIds.has(normalizeIntegerValue(row.id, 0)))
    .map((row) => ({
      id: String(row.id),
      name: row.name,
      location: row.location || row.berth || "",
      berth: row.berth || "",
      status: row.status || "",
    }));
}

function grantUserVesselAccess(userId, vesselIds) {
  const normalizedUserId = normalizeIntegerValue(userId, 0);
  if (normalizedUserId <= 0) {
    return;
  }

  const createdAt = nowIso();
  resolveExistingVesselIds(vesselIds).forEach((vesselId) => {
    insertUserVesselAccessStatement.run(normalizedUserId, vesselId, createdAt);
  });
}

function setUserVesselAccess(userId, vesselIds) {
  const normalizedUserId = normalizeIntegerValue(userId, 0);
  if (normalizedUserId <= 0) {
    return [];
  }

  const resolvedIds = resolveExistingVesselIds(vesselIds);
  runInTransaction(() => {
    deleteUserVesselAccessStatement.run(normalizedUserId);
    const createdAt = nowIso();
    resolvedIds.forEach((vesselId) => {
      insertUserVesselAccessStatement.run(normalizedUserId, vesselId, createdAt);
    });
  });
  return resolvedIds;
}

function setInviteVesselAccess(inviteId, vesselIds) {
  const normalizedInviteId = normalizeIntegerValue(inviteId, 0);
  if (normalizedInviteId <= 0) {
    return [];
  }

  const resolvedIds = resolveExistingVesselIds(vesselIds);
  runInTransaction(() => {
    deleteInviteVesselAccessStatement.run(normalizedInviteId);
    const createdAt = nowIso();
    resolvedIds.forEach((vesselId) => {
      insertInviteVesselAccessStatement.run(normalizedInviteId, vesselId, createdAt);
    });
  });
  return resolvedIds;
}

function backfillLegacyUserVesselAccess() {
  const vesselIds = listAllVesselIds();
  if (!vesselIds.length) {
    return;
  }

  selectUsersStatement.all().forEach((user) => {
    if (hasFullFleetAccess(user) || getUserAccessibleVesselIds(user).length) {
      return;
    }

    grantUserVesselAccess(user.id, vesselIds);
  });
}

function isVesselAccessibleToUser(user, vesselId) {
  const normalizedVesselId = normalizeIntegerValue(vesselId, 0);
  if (normalizedVesselId <= 0 || !user) {
    return false;
  }

  if (hasFullFleetAccess(user)) {
    return true;
  }

  return getUserAccessibleVesselIds(user).includes(normalizedVesselId);
}

function sanitizeUser(user) {
  if (!user) {
    return null;
  }

  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    vesselIds: getEffectiveVesselIdsForUser(user).map(String),
    hasFullFleetAccess: hasFullFleetAccess(user),
  };
}

function sanitizeManagedUser(user) {
  if (!user) {
    return null;
  }

  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    isActive: Boolean(user.is_active),
    createdAt: user.created_at,
    vesselIds: getEffectiveVesselIdsForUser(user).map(String),
    hasFullFleetAccess: hasFullFleetAccess(user),
  };
}

function sanitizeInvite(invite) {
  if (!invite) {
    return null;
  }

  return {
    id: invite.id,
    email: invite.email,
    role: invite.role,
    token: invite.token,
    createdAt: invite.created_at,
    expiresAt: invite.expires_at,
    link: buildInviteUrl(invite.token),
    vesselIds: hasFullFleetAccess(invite.role) ? listAllVesselIds().map(String) : getInviteAccessibleVesselIds(invite).map(String),
    hasFullFleetAccess: hasFullFleetAccess(invite.role),
  };
}

function sanitizePublicInvite(invite) {
  if (!invite) {
    return null;
  }

  return {
    email: invite.email,
    role: invite.role,
    expiresAt: invite.expires_at,
  };
}

function getInviteDeliveryStatus() {
  const ready = Boolean(resendApiKey && resendFromEmail && configuredPublicAppUrl);

  if (ready) {
    return {
      ready: true,
      provider: "Resend",
      fromEmail: resendFromEmail,
      publicAppUrl: configuredPublicAppUrl,
      message: `Invite emails send from ${resendFromEmail}.`,
    };
  }

  const missing = [];
  if (!resendApiKey) {
    missing.push("RESEND_API_KEY");
  }
  if (!resendFromEmail) {
    missing.push("RESEND_FROM_EMAIL");
  }
  if (!configuredPublicAppUrl) {
    missing.push("HARBOR_COMMAND_PUBLIC_URL");
  }

  return {
    ready: false,
    provider: "Resend",
    fromEmail: resendFromEmail,
    publicAppUrl: configuredPublicAppUrl || localAppUrl,
    message: missing.length
      ? `Add ${missing.join(", ")} to enable direct invite emails.`
      : "Direct invite email is not configured yet.",
  };
}

function buildInviteUrl(token) {
  if (!inviteBaseUrl) {
    return "";
  }

  const url = new URL(inviteBaseUrl);
  url.search = "";
  url.hash = "";
  url.searchParams.set("invite", token);
  return url.toString();
}

function getClientAddress(request) {
  const forwardedForHeader = String(request.headers["x-forwarded-for"] ?? "").split(",")[0].trim();
  const remoteAddress = forwardedForHeader || request.socket.remoteAddress || "unknown";
  return remoteAddress.replace(/^::ffff:/u, "");
}

function checkRateLimit(request, url) {
  const matchingRule = RATE_LIMIT_RULES.find((rule) => rule.matches(request, url));
  if (!matchingRule) {
    return null;
  }

  const now = Date.now();
  const clientAddress = getClientAddress(request);
  const bucketKey = `${matchingRule.name}:${clientAddress}`;
  const existingEntry = rateLimitStore.get(bucketKey);

  if (!existingEntry || existingEntry.resetAt <= now) {
    rateLimitStore.set(bucketKey, {
      count: 1,
      resetAt: now + matchingRule.windowMs,
    });
    return null;
  }

  if (existingEntry.count >= matchingRule.limit) {
    const retryAfterSeconds = Math.max(1, Math.ceil((existingEntry.resetAt - now) / 1000));
    return {
      retryAfterSeconds,
      message: "Too many requests from this connection. Please wait a moment and try again.",
    };
  }

  existingEntry.count += 1;
  rateLimitStore.set(bucketKey, existingEntry);
  return null;
}

function validateApiOrigin(request) {
  if (
    isTrustedLocalRequest(request)
    || isAllowedLoopbackRequestUrl(request?.headers?.origin)
    || isAllowedLoopbackRequestUrl(request?.headers?.referer)
  ) {
    return "";
  }

  const requestOrigin = normalizeOrigin(request.headers.origin);
  if (requestOrigin && !isAllowedApiOrigin(requestOrigin)) {
    return "This origin is not allowed to access Harbor Command.";
  }

  if (!MUTATING_METHODS.has(request.method)) {
    return "";
  }

  const refererOrigin = normalizeOrigin(request.headers.referer);
  const sourceOrigin = requestOrigin || refererOrigin;
  if (sourceOrigin && !isAllowedApiOrigin(sourceOrigin)) {
    return "This request did not come from an allowed Harbor Command origin.";
  }

  return "";
}

function validatePassword(password) {
  if (typeof password !== "string" || password.length < 8) {
    throw new Error("Password must be at least 8 characters.");
  }
}

function validateSetupPayload(payload) {
  const name = String(payload?.name ?? "").trim();
  const email = normalizeEmail(payload?.email);
  const password = String(payload?.password ?? "");
  const role = String(payload?.role ?? "Captain").trim() || "Captain";

  if (!name) {
    throw new Error("Name is required.");
  }

  if (!email || !email.includes("@")) {
    throw new Error("A valid email is required.");
  }

  if (!AUTH_ROLES.has(role)) {
    throw new Error("Role is invalid.");
  }

  validatePassword(password);

  return {
    name,
    email,
    password,
    role,
  };
}

function validateLoginPayload(payload) {
  const email = normalizeEmail(payload?.email);
  const password = String(payload?.password ?? "");

  if (!email || !email.includes("@")) {
    throw new Error("A valid email is required.");
  }

  if (!password) {
    throw new Error("Password is required.");
  }

  return {
    email,
    password,
  };
}

function validateManagedUserPayload(payload) {
  const nextPayload = validateSetupPayload(payload);
  return {
    ...nextPayload,
    vesselIds: resolveExistingVesselIds(payload?.vesselIds),
  };
}

function validateInvitePayload(payload) {
  const email = normalizeEmail(payload?.email);
  const role = String(payload?.role ?? "Crew").trim() || "Crew";

  if (!email || !email.includes("@")) {
    throw new Error("A valid email is required.");
  }

  if (!AUTH_ROLES.has(role)) {
    throw new Error("Role is invalid.");
  }

  return {
    email,
    role,
    vesselIds: resolveExistingVesselIds(payload?.vesselIds),
  };
}

function validateInviteAcceptPayload(payload) {
  const name = String(payload?.name ?? "").trim();
  const password = String(payload?.password ?? "");

  if (!name) {
    throw new Error("Name is required.");
  }

  validatePassword(password);

  return {
    name,
    password,
  };
}

function validateWeeklyReportPayload(payload, options = {}) {
  const { requireVesselId = false } = options;
  const vesselId = normalizeIntegerValue(payload?.vesselId, 0);
  const weekStartSource = String(payload?.weekStart ?? "").trim();
  const weekStart = formatDateOnly(weekStartSource || payload?.week_end || payload?.weekEnd || nowIso());
  const weekRange = getWorkWeekRange(weekStart);
  const status = normalizeWeeklyReportStatus(payload?.status);

  if (requireVesselId && vesselId <= 0) {
    throw new Error("A vessel is required to create a weekly report.");
  }

  return {
    vesselId,
    weekStart: weekRange.start,
    weekEnd: weekRange.end,
    status,
  };
}

function validateWeeklyReportEntryPayload(payload) {
  const item = String(payload?.item ?? "").trim() || "General update";
  const reportDate = formatDateOnly(payload?.reportDate || payload?.date || nowIso());
  const workDone = String(payload?.workDone ?? "").trim();
  const systemsChecked = String(payload?.systemsChecked ?? "").trim();
  const issues = String(payload?.issues ?? "").trim();
  const notes = String(payload?.notes ?? "").trim();
  const sourceWorkOrderId = String(payload?.sourceWorkOrderId ?? "").trim();

  return {
    item,
    reportDate,
    workDone,
    systemsChecked,
    issues,
    notes,
    sourceWorkOrderId,
  };
}

function hashPassword(password) {
  const salt = randomBytes(16).toString("hex");
  const hash = scryptSync(password, salt, 64).toString("hex");
  return `scrypt$${salt}$${hash}`;
}

function verifyPassword(password, storedHash) {
  const [algorithm, salt, expectedHex] = String(storedHash ?? "").split("$");

  if (algorithm !== "scrypt" || !salt || !expectedHex) {
    return false;
  }

  const expected = Buffer.from(expectedHex, "hex");
  const derived = scryptSync(password, salt, expected.length);

  return timingSafeEqual(derived, expected);
}

function findUserByEmail(email) {
  return selectUserByEmailStatement.get(email);
}

function findManagedUserById(userId) {
  return selectManagedUserByIdStatement.get(userId) || null;
}

function findPendingInviteByEmail(email) {
  return selectPendingInviteByEmailStatement.get(email, nowIso()) || null;
}

function findInviteByToken(token) {
  return selectInviteByTokenStatement.get(token) || null;
}

function findInviteById(inviteId) {
  return selectInviteByIdStatement.get(inviteId) || null;
}

function createUser({ name, email, password, role, vesselIds = [] }) {
  const createdAt = nowIso();
  const passwordHash = hashPassword(password);
  const result = insertUserStatement.run(name, email, passwordHash, role, createdAt);
  const userId = Number(result.lastInsertRowid);

  if (!hasFullFleetAccess(role)) {
    const resolvedVesselIds = vesselIds.length ? vesselIds : listAllVesselIds();
    setUserVesselAccess(userId, resolvedVesselIds);
  }

  return {
    id: userId,
    name,
    email,
    role,
  };
}

function listUsers() {
  return selectUsersStatement.all().map((user) => sanitizeManagedUser(user));
}

function listPendingInvites() {
  return selectPendingInvitesStatement.all(nowIso()).map((invite) => sanitizeInvite(invite));
}

function countActiveAdminUsers() {
  return countActiveAdminUsersStatement.get().total;
}

function canManageUsers(user) {
  return Boolean(user && USER_ADMIN_ROLES.has(user.role));
}

function destroySessionsForUser(userId) {
  deleteUserSessionsStatement.run(userId);
}

function setUserActive(userId, isActive) {
  updateUserActiveStatement.run(isActive ? 1 : 0, userId);
}

function deleteUser(userId) {
  deleteUserStatement.run(userId);
}

function createInvite({ email, role, vesselIds = [] }, createdByUserId) {
  const createdAt = nowIso();
  const expiresAt = new Date(Date.now() + 1000 * 60 * 60 * 24 * 7).toISOString();
  const token = randomBytes(32).toString("hex");
  const result = insertInviteStatement.run(email, role, token, createdByUserId ?? null, createdAt, expiresAt);
  const inviteId = Number(result.lastInsertRowid);

  if (!hasFullFleetAccess(role)) {
    const resolvedVesselIds = vesselIds.length ? vesselIds : listAllVesselIds();
    setInviteVesselAccess(inviteId, resolvedVesselIds);
  }

  return sanitizeInvite(findInviteById(inviteId));
}

function getValidInvite(token) {
  const invite = findInviteByToken(token);
  if (!invite) {
    return { invite: null, error: "This invite link is invalid." };
  }

  if (invite.revoked_at) {
    return { invite: null, error: "This invite link was revoked." };
  }

  if (invite.used_at) {
    return { invite: null, error: "This invite link has already been used." };
  }

  if (invite.expires_at <= nowIso()) {
    return { invite: null, error: "This invite link has expired." };
  }

  if (findUserByEmail(invite.email)) {
    return { invite: null, error: "An account already exists for this invite email." };
  }

  return { invite, error: "" };
}

function markInviteUsed(inviteId) {
  markInviteUsedStatement.run(nowIso(), inviteId);
}

function revokeInvite(inviteId) {
  revokeInviteStatement.run(nowIso(), inviteId);
}

async function sendInviteEmail(invite) {
  const deliveryStatus = getInviteDeliveryStatus();
  if (!deliveryStatus.ready) {
    throw new Error(deliveryStatus.message);
  }

  const inviteLink = buildInviteUrl(invite.token);
  if (!inviteLink) {
    throw new Error("A public invite URL is required before emails can be sent.");
  }

  const subject = `You're invited to Harbor Command`;
  const html = `
    <div style="font-family: Arial, sans-serif; line-height: 1.6; color: #102536;">
      <p style="font-size: 12px; letter-spacing: 0.22em; text-transform: uppercase; color: #2d8cb8; margin: 0 0 16px;">
        Harbor Command
      </p>
      <h1 style="font-size: 24px; margin: 0 0 16px;">You're invited aboard</h1>
      <p style="margin: 0 0 16px;">
        You have been invited to Harbor Command as <strong>${escapeHtmlForEmail(invite.role)}</strong>.
      </p>
      <p style="margin: 0 0 24px;">
        Use the button below to create your password and activate your account.
      </p>
      <p style="margin: 0 0 24px;">
        <a
          href="${escapeHtmlForEmail(inviteLink)}"
          style="display: inline-block; padding: 12px 20px; border-radius: 999px; background: #efb15a; color: #062332; text-decoration: none; font-weight: 700;"
        >
          Accept invite
        </a>
      </p>
      <p style="margin: 0 0 12px;">If the button does not open, copy and paste this link:</p>
      <p style="margin: 0 0 24px; word-break: break-all; color: #40627a;">${escapeHtmlForEmail(inviteLink)}</p>
      <p style="margin: 0; color: #5f7485; font-size: 14px;">
        This invite expires on ${escapeHtmlForEmail(formatDateTimeForEmail(invite.expires_at || invite.expiresAt))}.
      </p>
    </div>
  `;
  const text = [
    "Harbor Command",
    "",
    `You have been invited to Harbor Command as ${invite.role}.`,
    "Open this link to create your password and activate access:",
    inviteLink,
    "",
    `This invite expires on ${formatDateTimeForEmail(invite.expires_at || invite.expiresAt)}.`,
  ].join("\n");

  const payload = {
    from: resendFromEmail,
    to: [invite.email],
    subject,
    html,
    text,
  };

  if (resendReplyTo) {
    payload.reply_to = resendReplyTo;
  }

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${resendApiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(result?.message || result?.error || "Resend could not deliver the invite email.");
  }

  return result;
}

function cleanupExpiredSessions() {
  deleteExpiredSessionsStatement.run(nowIso());
}

function parseCookies(cookieHeader = "") {
  return Object.fromEntries(
    cookieHeader
      .split(";")
      .map((pair) => pair.trim())
      .filter(Boolean)
      .map((pair) => {
        const separatorIndex = pair.indexOf("=");
        if (separatorIndex === -1) {
          return [pair, ""];
        }

        return [pair.slice(0, separatorIndex), decodeURIComponent(pair.slice(separatorIndex + 1))];
      })
  );
}

function getSessionToken(request) {
  const cookies = parseCookies(request.headers.cookie);
  return cookies[SESSION_COOKIE_NAME] || "";
}

function buildSessionCookie(request, token, maxAgeSeconds) {
  const host = String(request.headers.host ?? "").toLowerCase();
  const isLocalHost = /^(?:localhost|127(?:\.\d{1,3}){3}|\[::1\])(?::\d+)?$/u.test(host);
  const parts = [
    `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}`,
    "HttpOnly",
    "Path=/",
    "SameSite=Lax",
    `Max-Age=${maxAgeSeconds}`,
  ];

  if (!isLocalHost && configuredPublicAppUrl.startsWith("https://")) {
    parts.push("Secure");
  }

  return parts.join("; ");
}

function createSession(userId) {
  const token = randomBytes(32).toString("hex");
  const createdAt = nowIso();
  const expiresAt = new Date(Date.now() + SESSION_MAX_AGE_SECONDS * 1000).toISOString();

  insertSessionStatement.run(token, userId, createdAt, expiresAt);

  return token;
}

function destroySession(token) {
  if (!token) {
    return;
  }

  deleteSessionStatement.run(token);
}

function getSessionUser(request) {
  cleanupExpiredSessions();
  const token = getSessionToken(request);

  if (!token) {
    return null;
  }

  return selectSessionUserStatement.get(token, nowIso()) || null;
}

function normalizeTextValue(value, fallback = "") {
  if (typeof value === "string") {
    return value;
  }

  if (value === null || typeof value === "undefined") {
    return fallback;
  }

  return String(value);
}

function normalizeNullableTextValue(value) {
  const normalized = normalizeTextValue(value, "").trim();
  return normalized ? normalized : null;
}

function normalizeIntegerValue(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeNumberValue(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeCollectionItems(items) {
  return Array.isArray(items) ? items : [];
}

function normalizeRowId(value, prefix, index) {
  const normalized = normalizeTextValue(value, "").trim();
  return normalized || `${prefix}-${index + 1}`;
}

function sanitizeLegacyItem(item) {
  if (!item || typeof item !== "object") {
    return {};
  }

  const clone = { ...item };
  delete clone.yachtId;
  return clone;
}

function extractLegacyScopedItems(items, activeYachtId) {
  return normalizeCollectionItems(items)
    .filter((item) => !activeYachtId || !item?.yachtId || item.yachtId === activeYachtId)
    .map((item) => sanitizeLegacyItem(item));
}

function normalizeStateForStructuredStorage(rawState) {
  const source = rawState && typeof rawState === "object" ? { ...rawState } : {};

  if (Array.isArray(source.vessels) && source.vessels.length) {
    return source;
  }

  if (Array.isArray(source.yachts) && source.yachts.length) {
    const activeYacht = source.yachts.find((yacht) => yacht?.id === source.selectedYachtId) || source.yachts[0];
    const activeYachtId = activeYacht?.id;

    return {
      ...source,
      vessel: activeYacht ? sanitizeLegacyItem(activeYacht) : source.vessel,
      maintenanceAssets: [],
      maintenance: extractLegacyScopedItems(source.maintenance, activeYachtId),
      maintenanceHistory: [],
      workOrders: extractLegacyScopedItems(source.workOrders, activeYachtId),
      charters: extractLegacyScopedItems(source.charters, activeYachtId),
      crew: extractLegacyScopedItems(source.crew, activeYachtId),
      reports: extractLegacyScopedItems(source.reports, activeYachtId),
      voyages: extractLegacyScopedItems(source.voyages, activeYachtId),
      vendors: normalizeCollectionItems(source.vendors).map((item) => sanitizeLegacyItem(item)),
      inventory: extractLegacyScopedItems(source.inventory, activeYachtId),
      expenses: extractLegacyScopedItems(source.expenses, activeYachtId),
    };
  }

  return source;
}

function parseDateOnly(value) {
  if (!value) {
    return null;
  }

  if (value instanceof Date) {
    return new Date(value.getFullYear(), value.getMonth(), value.getDate());
  }

  if (typeof value === "string") {
    const dateMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
    if (dateMatch) {
      return new Date(Number(dateMatch[1]), Number(dateMatch[2]) - 1, Number(dateMatch[3]));
    }
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
}

function formatDateOnly(value) {
  const parsed = parseDateOnly(value);
  if (!parsed) {
    return "";
  }

  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getWorkWeekRange(dateValue) {
  const referenceDate = parseDateOnly(dateValue) || parseDateOnly(nowIso());
  const dayOfWeek = referenceDate.getDay();
  const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
  const start = new Date(referenceDate);
  start.setDate(referenceDate.getDate() + mondayOffset);

  const end = new Date(start);
  end.setDate(start.getDate() + 4);

  return {
    start: formatDateOnly(start),
    end: formatDateOnly(end),
  };
}

function normalizeWeeklyReportStatus(status) {
  const normalized = normalizeTextValue(status, "draft").trim().toLowerCase();
  return normalized === "finalized" ? "finalized" : "draft";
}

function compareDateOnlyStrings(left, right) {
  return formatDateOnly(left).localeCompare(formatDateOnly(right));
}

function normalizeMaintenanceAssetRecord(record, index = 0) {
  return {
    id: normalizeTextValue(record?.id, `maintenance-asset-${index + 1}`),
    templateId: normalizeTextValue(record?.templateId ?? record?.template_id, ""),
    name: normalizeTextValue(record?.name, `Installed asset ${index + 1}`),
    assetType: normalizeTextValue(record?.assetType ?? record?.asset_type, ""),
    manufacturer: normalizeTextValue(record?.manufacturer, ""),
    model: normalizeTextValue(record?.model, ""),
    serialNumber: normalizeTextValue(record?.serialNumber ?? record?.serial_number, ""),
    location: normalizeTextValue(record?.location, ""),
    meterSourceType: normalizeTextValue(record?.meterSourceType ?? record?.meter_source_type, "none") || "none",
    meterSourceId: normalizeTextValue(record?.meterSourceId ?? record?.meter_source_id, ""),
    currentHours: normalizeIntegerValue(record?.currentHours ?? record?.current_hours, 0),
    notes: normalizeTextValue(record?.notes, ""),
    createdAt: normalizeTextValue(record?.createdAt ?? record?.created_at, nowIso()),
    updatedAt: normalizeTextValue(record?.updatedAt ?? record?.updated_at, nowIso()),
    sortOrder: Number.isFinite(Number(record?.sortOrder ?? record?.sort_order))
      ? Number(record?.sortOrder ?? record?.sort_order)
      : index,
  };
}

function normalizeMaintenanceTaskRecord(record, index = 0) {
  const intervalDays = normalizeIntegerValue(record?.intervalDays ?? record?.interval_days, 0);
  const intervalHours = normalizeIntegerValue(record?.intervalHours ?? record?.interval_hours, 0);
  let recurrenceMode = normalizeMaintenanceRecurrenceMode(record?.recurrenceMode ?? record?.recurrence_mode);
  if (recurrenceMode === "days" && intervalHours > 0 && intervalDays <= 0) {
    recurrenceMode = "hours";
  }

  return {
    id: normalizeTextValue(record?.id, `maintenance-${index + 1}`),
    title: normalizeTextValue(record?.title, `Maintenance item ${index + 1}`),
    category: normalizeTextValue(record?.category, "General"),
    status: normalizeTextValue(record?.status, "Not Started"),
    priority: normalizeTextValue(record?.priority, "Medium"),
    assetId: normalizeTextValue(record?.assetId ?? record?.asset_id, ""),
    templateId: normalizeTextValue(record?.templateId ?? record?.template_id, ""),
    templateTaskId: normalizeTextValue(record?.templateTaskId ?? record?.template_task_id, ""),
    dueDate: formatDateOnly(record?.dueDate ?? record?.due_date),
    dueHours: normalizeIntegerValue(record?.dueHours ?? record?.due_hours, 0),
    lastCompleted: formatDateOnly(record?.lastCompleted ?? record?.last_completed),
    lastCompletedHours: normalizeIntegerValue(record?.lastCompletedHours ?? record?.last_completed_hours, 0),
    intervalDays,
    intervalHours,
    reminderDays: normalizeIntegerValue(record?.reminderDays ?? record?.reminder_days, 0),
    reminderHours: normalizeIntegerValue(record?.reminderHours ?? record?.reminder_hours, 0),
    recurrenceMode,
    meterSourceType: normalizeTextValue(record?.meterSourceType ?? record?.meter_source_type, "none") || "none",
    meterSourceId: normalizeTextValue(record?.meterSourceId ?? record?.meter_source_id, ""),
    isCustom: normalizeIntegerValue(record?.isCustom ?? record?.is_custom, 1) !== 0,
    notes: normalizeTextValue(record?.notes, ""),
    createdAt: normalizeTextValue(record?.createdAt ?? record?.created_at, nowIso()),
    updatedAt: normalizeTextValue(record?.updatedAt ?? record?.updated_at, nowIso()),
    sortOrder: Number.isFinite(Number(record?.sortOrder ?? record?.sort_order))
      ? Number(record?.sortOrder ?? record?.sort_order)
      : index,
  };
}

function normalizeMaintenanceHistoryRecord(record, index = 0) {
  return {
    id: normalizeTextValue(record?.id, `maintenance-history-${index + 1}`),
    vesselId: normalizeIntegerValue(record?.vesselId ?? record?.vessel_id, 0),
    maintenanceLogId: normalizeTextValue(record?.maintenanceLogId ?? record?.maintenance_log_id, ""),
    assetId: normalizeTextValue(record?.assetId ?? record?.asset_id, ""),
    templateTaskId: normalizeTextValue(record?.templateTaskId ?? record?.template_task_id, ""),
    workOrderId: normalizeTextValue(record?.workOrderId ?? record?.work_order_id, ""),
    source: normalizeTextValue(record?.source, "manual"),
    completedAt: normalizeTextValue(record?.completedAt ?? record?.completed_at, nowIso()),
    completionDate: formatDateOnly(
      (record?.completionDate ?? record?.completion_date ?? record?.completedAt ?? record?.completed_at ?? nowIso())
    ),
    completedHours: normalizeIntegerValue(record?.completedHours ?? record?.completed_hours, 0),
    workDone: normalizeTextValue(record?.workDone ?? record?.work_done, ""),
    systemsChecked: normalizeTextValue(record?.systemsChecked ?? record?.systems_checked, ""),
    issues: normalizeTextValue(record?.issues, ""),
    notes: normalizeTextValue(record?.notes, ""),
    createdAt: normalizeTextValue(record?.createdAt ?? record?.created_at, nowIso()),
    updatedAt: normalizeTextValue(record?.updatedAt ?? record?.updated_at, nowIso()),
  };
}

function usesDateRecurrence(task) {
  const recurrenceMode = normalizeMaintenanceRecurrenceMode(task?.recurrenceMode);
  return recurrenceMode === "days" || recurrenceMode === "days-or-hours";
}

function usesHourRecurrence(task) {
  const recurrenceMode = normalizeMaintenanceRecurrenceMode(task?.recurrenceMode);
  return recurrenceMode === "hours" || recurrenceMode === "days-or-hours";
}

function loadAssetMeterHours(asset, options = {}) {
  if (!asset) {
    return 0;
  }

  const meterSourceType = normalizeTextValue(asset.meterSourceType, "none").trim().toLowerCase();
  const meterSourceId = normalizeTextValue(asset.meterSourceId, "");
  const engines = Array.isArray(options.engines) ? options.engines : [];
  const generators = Array.isArray(options.generators) ? options.generators : [];

  if (meterSourceType === "engine" && meterSourceId) {
    return normalizeIntegerValue(
      engines.find((item) => normalizeTextValue(item?.id, "") === meterSourceId)?.hours,
      normalizeIntegerValue(asset.currentHours, 0)
    );
  }

  if (meterSourceType === "generator" && meterSourceId) {
    return normalizeIntegerValue(
      generators.find((item) => normalizeTextValue(item?.id, "") === meterSourceId)?.hours,
      normalizeIntegerValue(asset.currentHours, 0)
    );
  }

  return normalizeIntegerValue(asset.currentHours, 0);
}

function getMaintenanceCurrentHours(task, assetsById, options = {}) {
  const linkedAsset = assetsById.get(String(task?.assetId || ""));
  if (linkedAsset) {
    return loadAssetMeterHours(linkedAsset, options);
  }

  return normalizeIntegerValue(task?.lastCompletedHours, 0);
}

function getMaintenanceHoursRemaining(task, assetsById, options = {}) {
  const dueHours = normalizeIntegerValue(task?.dueHours, 0);
  if (dueHours <= 0) {
    return null;
  }

  return dueHours - getMaintenanceCurrentHours(task, assetsById, options);
}

const PDF_PAGE_WIDTH = 612;
const PDF_PAGE_HEIGHT = 792;
const PDF_PAGE_MARGIN = 42;
const PDF_CONTENT_WIDTH = PDF_PAGE_WIDTH - (PDF_PAGE_MARGIN * 2);
const PDF_COLORS = {
  navy: [0.035, 0.196, 0.302],
  sea: [0.055, 0.478, 0.647],
  gold: [0.937, 0.694, 0.353],
  ink: [0.051, 0.141, 0.22],
  slate: [0.298, 0.38, 0.443],
  border: [0.772, 0.847, 0.886],
  sky: [0.914, 0.957, 0.98],
  paper: [0.996, 0.99, 0.974],
  white: [1, 1, 1],
  coral: [0.75, 0.42, 0.33],
  mint: [0.298, 0.718, 0.647],
};

function normalizePdfText(value, fallback = "") {
  return normalizeTextValue(value, fallback)
    .replace(/[–—]/gu, "-")
    .replace(/[“”]/gu, "\"")
    .replace(/[‘’]/gu, "'")
    .replace(/\u2026/gu, "...")
    .normalize("NFKD")
    .replace(/[^\x20-\x7E\n]/gu, " ")
    .replace(/\r\n?/gu, "\n");
}

function escapePdfText(value) {
  return normalizePdfText(value)
    .replace(/\\/gu, "\\\\")
    .replace(/\(/gu, "\\(")
    .replace(/\)/gu, "\\)");
}

function formatPdfNumber(value) {
  return Number(value).toFixed(2).replace(/\.00$/u, "").replace(/(\.\d)0$/u, "$1");
}

function colorToPdf(color) {
  return color.map((channel) => formatPdfNumber(channel)).join(" ");
}

function formatDateOnlyForPdf(dateValue) {
  const parsed = parseDateOnly(dateValue);
  if (!parsed) {
    return "-";
  }

  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${monthNames[parsed.getMonth()]} ${parsed.getDate()}, ${parsed.getFullYear()}`;
}

function formatDateTimeForPdf(dateValue) {
  try {
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit",
    }).format(new Date(dateValue));
  } catch {
    return formatDateOnlyForPdf(dateValue);
  }
}

function formatWeeklyReportPeriodForPdf(report) {
  return `${formatDateOnlyForPdf(report.weekStart)} - ${formatDateOnlyForPdf(report.weekEnd)}`;
}

function formatPdfStatusLabel(status) {
  return normalizeWeeklyReportStatus(status) === "finalized" ? "Finalized" : "Draft";
}

function formatPdfPercentWithCapacity(level, capacity, unit = "gal") {
  const normalizedLevel = normalizeIntegerValue(level, 0);
  const normalizedCapacity = normalizeIntegerValue(capacity, 0);
  if (normalizedCapacity > 0) {
    return `${normalizedLevel}% | ${normalizedCapacity} ${unit} cap`;
  }

  return `${normalizedLevel}%`;
}

function formatPdfDimensionValue(value, unit = "ft") {
  const normalized = normalizeNumberValue(value, 0);
  if (!normalized) {
    return `- ${unit}`;
  }

  return `${formatPdfNumber(normalized)} ${unit}`;
}

function paethPredictor(left, up, upLeft) {
  const prediction = left + up - upLeft;
  const leftDistance = Math.abs(prediction - left);
  const upDistance = Math.abs(prediction - up);
  const upLeftDistance = Math.abs(prediction - upLeft);

  if (leftDistance <= upDistance && leftDistance <= upLeftDistance) {
    return left;
  }

  if (upDistance <= upLeftDistance) {
    return up;
  }

  return upLeft;
}

function parseJpegDimensions(buffer) {
  if (!buffer || buffer.length < 4 || buffer[0] !== 0xff || buffer[1] !== 0xd8) {
    return null;
  }

  let offset = 2;
  while (offset < buffer.length) {
    while (offset < buffer.length && buffer[offset] !== 0xff) {
      offset += 1;
    }

    while (offset < buffer.length && buffer[offset] === 0xff) {
      offset += 1;
    }

    if (offset >= buffer.length) {
      break;
    }

    const marker = buffer[offset];
    offset += 1;

    if (marker === 0xd9 || marker === 0xda) {
      break;
    }

    if (offset + 1 >= buffer.length) {
      break;
    }

    const segmentLength = buffer.readUInt16BE(offset);
    if (segmentLength < 2 || offset + segmentLength > buffer.length) {
      break;
    }

    if (
      (marker >= 0xc0 && marker <= 0xc3)
      || (marker >= 0xc5 && marker <= 0xc7)
      || (marker >= 0xc9 && marker <= 0xcb)
      || (marker >= 0xcd && marker <= 0xcf)
    ) {
      const height = buffer.readUInt16BE(offset + 3);
      const width = buffer.readUInt16BE(offset + 5);
      return { width, height };
    }

    offset += segmentLength;
  }

  return null;
}

function buildJpegPdfImage(buffer) {
  const dimensions = parseJpegDimensions(buffer);
  if (!dimensions) {
    return null;
  }

  return {
    width: dimensions.width,
    height: dimensions.height,
    colorSpace: "/DeviceRGB",
    bitsPerComponent: 8,
    filter: "/DCTDecode",
    data: buffer,
  };
}

function buildPngPdfImage(buffer) {
  const pngSignature = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  if (!buffer || buffer.length < pngSignature.length || !buffer.subarray(0, 8).equals(pngSignature)) {
    return null;
  }

  let offset = 8;
  let width = 0;
  let height = 0;
  let bitDepth = 0;
  let colorType = 0;
  let interlace = 0;
  const idatChunks = [];

  while (offset + 8 <= buffer.length) {
    const chunkLength = buffer.readUInt32BE(offset);
    const chunkType = buffer.toString("ascii", offset + 4, offset + 8);
    const dataStart = offset + 8;
    const dataEnd = dataStart + chunkLength;
    if (dataEnd + 4 > buffer.length) {
      return null;
    }

    const chunkData = buffer.subarray(dataStart, dataEnd);
    offset = dataEnd + 4;

    if (chunkType === "IHDR") {
      width = chunkData.readUInt32BE(0);
      height = chunkData.readUInt32BE(4);
      bitDepth = chunkData[8];
      colorType = chunkData[9];
      interlace = chunkData[12];
    } else if (chunkType === "IDAT") {
      idatChunks.push(chunkData);
    } else if (chunkType === "IEND") {
      break;
    }
  }

  if (!width || !height || bitDepth !== 8 || interlace !== 0 || !idatChunks.length) {
    return null;
  }

  let channels = 0;
  if (colorType === 0) {
    channels = 1;
  } else if (colorType === 2) {
    channels = 3;
  } else if (colorType === 4) {
    channels = 2;
  } else if (colorType === 6) {
    channels = 4;
  } else {
    return null;
  }

  const bytesPerPixel = channels;
  const stride = width * bytesPerPixel;
  const inflated = inflateSync(Buffer.concat(idatChunks));
  const expectedLength = height * (stride + 1);
  if (inflated.length < expectedLength) {
    return null;
  }

  const reconstructedRows = [];
  let inputOffset = 0;
  for (let rowIndex = 0; rowIndex < height; rowIndex += 1) {
    const filterType = inflated[inputOffset];
    inputOffset += 1;
    const rowData = inflated.subarray(inputOffset, inputOffset + stride);
    inputOffset += stride;
    const reconstructed = Buffer.allocUnsafe(stride);
    const previousRow = reconstructedRows[rowIndex - 1] || null;

    for (let column = 0; column < stride; column += 1) {
      const left = column >= bytesPerPixel ? reconstructed[column - bytesPerPixel] : 0;
      const up = previousRow ? previousRow[column] : 0;
      const upLeft = previousRow && column >= bytesPerPixel ? previousRow[column - bytesPerPixel] : 0;
      let value = rowData[column];

      if (filterType === 1) {
        value = (value + left) & 0xff;
      } else if (filterType === 2) {
        value = (value + up) & 0xff;
      } else if (filterType === 3) {
        value = (value + Math.floor((left + up) / 2)) & 0xff;
      } else if (filterType === 4) {
        value = (value + paethPredictor(left, up, upLeft)) & 0xff;
      }

      reconstructed[column] = value;
    }

    reconstructedRows.push(reconstructed);
  }

  const rgbData = Buffer.allocUnsafe(width * height * 3);
  let rgbOffset = 0;
  reconstructedRows.forEach((row) => {
    if (colorType === 0) {
      for (let index = 0; index < row.length; index += 1) {
        const gray = row[index];
        rgbData[rgbOffset] = gray;
        rgbData[rgbOffset + 1] = gray;
        rgbData[rgbOffset + 2] = gray;
        rgbOffset += 3;
      }
      return;
    }

    if (colorType === 2) {
      row.copy(rgbData, rgbOffset, 0, row.length);
      rgbOffset += row.length;
      return;
    }

    if (colorType === 4) {
      for (let index = 0; index < row.length; index += 2) {
        const gray = row[index];
        rgbData[rgbOffset] = gray;
        rgbData[rgbOffset + 1] = gray;
        rgbData[rgbOffset + 2] = gray;
        rgbOffset += 3;
      }
      return;
    }

    for (let index = 0; index < row.length; index += 4) {
      rgbData[rgbOffset] = row[index];
      rgbData[rgbOffset + 1] = row[index + 1];
      rgbData[rgbOffset + 2] = row[index + 2];
      rgbOffset += 3;
    }
  });

  return {
    width,
    height,
    colorSpace: "/DeviceRGB",
    bitsPerComponent: 8,
    filter: "/FlateDecode",
    data: deflateSync(rgbData),
  };
}

function parsePdfImageFromDataUrl(dataUrl) {
  const normalized = normalizeTextValue(dataUrl, "");
  const match = /^data:(image\/[a-z0-9.+-]+);base64,([a-z0-9+/=\r\n]+)$/iu.exec(normalized);
  if (!match) {
    return null;
  }

  const mimeType = match[1].toLowerCase();
  const imageBuffer = Buffer.from(match[2].replace(/\s+/gu, ""), "base64");

  if (mimeType === "image/jpeg" || mimeType === "image/jpg") {
    return buildJpegPdfImage(imageBuffer);
  }

  if (mimeType === "image/png") {
    return buildPngPdfImage(imageBuffer);
  }

  return null;
}

function estimatePdfTextWidth(text, fontSize) {
  return normalizePdfText(text).length * fontSize * 0.52;
}

function wrapPdfText(text, maxWidth, fontSize) {
  const normalized = normalizePdfText(text, "").trim();
  if (!normalized) {
    return ["-"];
  }

  const maxChars = Math.max(10, Math.floor(maxWidth / Math.max(fontSize * 0.52, 1)));
  const paragraphs = normalized.split("\n");
  const lines = [];

  paragraphs.forEach((paragraph) => {
    const words = paragraph.trim().split(/\s+/u).filter(Boolean);
    if (!words.length) {
      lines.push("");
      return;
    }

    let currentLine = "";
    words.forEach((word) => {
      if (estimatePdfTextWidth(word, fontSize) > maxWidth) {
        if (currentLine) {
          lines.push(currentLine);
          currentLine = "";
        }

        for (let index = 0; index < word.length; index += maxChars) {
          lines.push(word.slice(index, index + maxChars));
        }
        return;
      }

      const candidate = currentLine ? `${currentLine} ${word}` : word;
      if (estimatePdfTextWidth(candidate, fontSize) <= maxWidth) {
        currentLine = candidate;
      } else {
        lines.push(currentLine);
        currentLine = word;
      }
    });

    if (currentLine) {
      lines.push(currentLine);
    }
  });

  return lines.length ? lines : ["-"];
}

function pushPdfRectangle(commands, x, y, width, height, options = {}) {
  const { fillColor = null, strokeColor = null, lineWidth = 1 } = options;
  if (fillColor) {
    commands.push(`${colorToPdf(fillColor)} rg`);
  }
  if (strokeColor) {
    commands.push(`${colorToPdf(strokeColor)} RG`);
    commands.push(`${formatPdfNumber(lineWidth)} w`);
  }

  const operator = fillColor && strokeColor ? "B" : fillColor ? "f" : "S";
  commands.push(`${formatPdfNumber(x)} ${formatPdfNumber(y)} ${formatPdfNumber(width)} ${formatPdfNumber(height)} re ${operator}`);
}

function pushPdfText(commands, text, x, y, options = {}) {
  const {
    font = "F1",
    size = 12,
    color = PDF_COLORS.ink,
  } = options;

  commands.push("BT");
  commands.push(`/${font} ${formatPdfNumber(size)} Tf`);
  commands.push(`${colorToPdf(color)} rg`);
  commands.push(`1 0 0 1 ${formatPdfNumber(x)} ${formatPdfNumber(y)} Tm`);
  commands.push(`(${escapePdfText(text)}) Tj`);
  commands.push("ET");
}

function pushPdfImage(commands, imageName, x, y, width, height) {
  commands.push("q");
  commands.push(`${formatPdfNumber(width)} 0 0 ${formatPdfNumber(height)} ${formatPdfNumber(x)} ${formatPdfNumber(y)} cm`);
  commands.push(`/${imageName} Do`);
  commands.push("Q");
}

function pushPdfTextBlock(commands, text, x, yTop, maxWidth, options = {}) {
  const {
    font = "F1",
    size = 12,
    color = PDF_COLORS.ink,
    lineHeight = size + 3,
  } = options;
  const lines = Array.isArray(text) ? text : wrapPdfText(text, maxWidth, size);
  let baseline = yTop - size;

  lines.forEach((line) => {
    pushPdfText(commands, line, x, baseline, { font, size, color });
    baseline -= lineHeight;
  });

  return baseline;
}

function buildWeeklyReportPdfFilename(report, vessel) {
  const vesselSlug = normalizePdfText(vessel?.name || "vessel")
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "") || "vessel";
  const weekSlug = formatDateOnly(report?.weekStart || "report").replace(/[^0-9-]/gu, "") || "week";
  return `harbor-command-${vesselSlug}-weekly-report-${weekSlug}.pdf`;
}

function buildPdfDocument(pageDefinitions) {
  const normalizedPages = pageDefinitions.map((pageDefinition) => {
    if (Array.isArray(pageDefinition)) {
      return {
        commands: pageDefinition,
        images: [],
      };
    }

    return {
      commands: Array.isArray(pageDefinition?.commands) ? pageDefinition.commands : [],
      images: Array.isArray(pageDefinition?.images) ? pageDefinition.images : [],
    };
  });

  const objects = [null];
  const addObject = (body) => {
    objects.push(body);
    return objects.length - 1;
  };
  const toAsciiBuffer = (value) => Buffer.isBuffer(value) ? value : Buffer.from(String(value), "ascii");

  const fontRegularId = addObject("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>");
  const fontBoldId = addObject("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>");
  const pageIds = [];

  normalizedPages.forEach((pageDefinition) => {
    const imageRefs = [];
    pageDefinition.images.forEach((imageDefinition, imageIndex) => {
      if (!imageDefinition?.image?.data) {
        return;
      }

      const imageName = imageDefinition.name || `Im${imageIndex + 1}`;
      const image = imageDefinition.image;
      const imageHeader = `<< /Type /XObject /Subtype /Image /Width ${image.width} /Height ${image.height} /ColorSpace ${image.colorSpace} /BitsPerComponent ${image.bitsPerComponent} /Filter ${image.filter} /Length ${image.data.length} >>\nstream\n`;
      const imageBody = Buffer.concat([
        Buffer.from(imageHeader, "ascii"),
        image.data,
        Buffer.from("\nendstream", "ascii"),
      ]);
      const imageId = addObject(imageBody);
      imageRefs.push({ name: imageName, id: imageId });
    });

    const content = pageDefinition.commands.join("\n");
    const contentBuffer = Buffer.from(content, "ascii");
    const contentId = addObject(Buffer.concat([
      Buffer.from(`<< /Length ${contentBuffer.length} >>\nstream\n`, "ascii"),
      contentBuffer,
      Buffer.from("\nendstream", "ascii"),
    ]));
    const xObjectResource = imageRefs.length
      ? ` /XObject << ${imageRefs.map((imageRef) => `/${imageRef.name} ${imageRef.id} 0 R`).join(" ")} >>`
      : "";
    const pageId = addObject(`<< /Type /Page /Parent __PAGES__ 0 R /MediaBox [0 0 ${PDF_PAGE_WIDTH} ${PDF_PAGE_HEIGHT}] /Resources << /Font << /F1 ${fontRegularId} 0 R /F2 ${fontBoldId} 0 R >>${xObjectResource} >> /Contents ${contentId} 0 R >>`);
    pageIds.push(pageId);
  });

  const pagesId = addObject("");
  const catalogId = addObject(`<< /Type /Catalog /Pages ${pagesId} 0 R >>`);
  objects[pagesId] = `<< /Type /Pages /Count ${pageIds.length} /Kids [${pageIds.map((pageId) => `${pageId} 0 R`).join(" ")}] >>`;
  pageIds.forEach((pageId) => {
    objects[pageId] = objects[pageId].replace("__PAGES__", String(pagesId));
  });

  const documentParts = [Buffer.from("%PDF-1.4\n%HARBOR\n", "ascii")];
  const offsets = [0];
  let documentLength = documentParts[0].length;
  for (let index = 1; index < objects.length; index += 1) {
    const objectBuffer = toAsciiBuffer(objects[index]);
    const objectHeader = Buffer.from(`${index} 0 obj\n`, "ascii");
    const objectFooter = Buffer.from("\nendobj\n", "ascii");
    offsets[index] = documentLength;
    documentParts.push(objectHeader, objectBuffer, objectFooter);
    documentLength += objectHeader.length + objectBuffer.length + objectFooter.length;
  }

  const xrefOffset = documentLength;
  let trailer = `xref\n0 ${objects.length}\n0000000000 65535 f \n`;
  for (let index = 1; index < objects.length; index += 1) {
    trailer += `${String(offsets[index]).padStart(10, "0")} 00000 n \n`;
  }

  trailer += `trailer\n<< /Size ${objects.length} /Root ${catalogId} 0 R >>\nstartxref\n${xrefOffset}\n%%EOF`;
  documentParts.push(Buffer.from(trailer, "ascii"));
  return Buffer.concat(documentParts);
}

function buildWeeklyReportPdfBuffer(report, vessel) {
  const pages = [];
  const entryWidth = PDF_CONTENT_WIDTH;
  const entryInnerWidth = entryWidth - 24;
  const entryColumnGap = 16;
  const entryColumnWidth = (entryInnerWidth - entryColumnGap) / 2;
  const generatedAt = nowIso();
  const vesselImage = parsePdfImageFromDataUrl(vessel?.photo_data_url || vessel?.photoDataUrl || "");
  const snapshot = parseWeeklyReportVesselSnapshot(
    report?.vesselSnapshot,
    vessel,
    report?.updatedAt || report?.createdAt || generatedAt
  ) || buildWeeklyReportVesselSnapshot(vessel, report?.updatedAt || report?.createdAt || generatedAt);
  const vesselTitle = normalizePdfText(snapshot.name || vessel?.name || "Vessel");
  const vesselMeta = [snapshot.builder, snapshot.model].filter(Boolean).join(" | ");
  const periodText = formatWeeklyReportPeriodForPdf(report);
  let currentPage = null;
  let commands = [];
  let cursorY = 0;

  const renderMetaPill = (x, y, width, height, label, value, accent = false) => {
    pushPdfRectangle(commands, x, y, width, height, {
      fillColor: accent ? PDF_COLORS.sky : PDF_COLORS.paper,
      strokeColor: PDF_COLORS.border,
    });
    pushPdfText(commands, label, x + 10, y + height - 13, {
      font: "F2",
      size: 7.6,
      color: PDF_COLORS.slate,
    });
    const valueLines = wrapPdfText(value, width - 20, 9.2).slice(0, 2);
    pushPdfTextBlock(commands, valueLines, x + 10, y + height - 24, width - 20, {
      font: "F1",
      size: 9.2,
      lineHeight: 10.2,
      color: PDF_COLORS.ink,
    });
  };

  const clampGaugePercent = (value) => Math.max(0, Math.min(100, normalizeIntegerValue(value, 0)));

  const renderGaugeCard = (x, y, width, height, options) => {
    const {
      label,
      percent,
      headline,
      fillColor = PDF_COLORS.sea,
      accent = false,
    } = options;
    const safePercent = clampGaugePercent(percent);
    const trackX = x + 10;
    const trackY = y + 10;
    const trackWidth = width - 20;
    const fillWidth = safePercent > 0 ? Math.max(8, trackWidth * (safePercent / 100)) : 0;

    pushPdfRectangle(commands, x, y, width, height, {
      fillColor: accent ? PDF_COLORS.sky : PDF_COLORS.white,
      strokeColor: PDF_COLORS.border,
    });
    pushPdfText(commands, label, x + 10, y + height - 14, {
      font: "F2",
      size: 7.4,
      color: PDF_COLORS.slate,
    });
    pushPdfText(commands, headline, x + 10, y + height - 34, {
      font: "F2",
      size: 16.5,
      color: PDF_COLORS.ink,
    });
    pushPdfRectangle(commands, trackX, trackY, trackWidth, 7, {
      fillColor: PDF_COLORS.paper,
      strokeColor: PDF_COLORS.border,
      lineWidth: 0.6,
    });
    if (fillWidth > 0) {
      pushPdfRectangle(commands, trackX, trackY, fillWidth, 7, {
        fillColor,
      });
    }
  };

  const renderInfoCard = (x, y, width, height, label, primary, secondary = "", accent = false) => {
    pushPdfRectangle(commands, x, y, width, height, {
      fillColor: accent ? PDF_COLORS.sky : PDF_COLORS.white,
      strokeColor: PDF_COLORS.border,
    });
    pushPdfText(commands, label, x + 10, y + height - 14, {
      font: "F2",
      size: 7.4,
      color: PDF_COLORS.slate,
    });
    pushPdfTextBlock(commands, primary, x + 10, y + height - 26, width - 20, {
      font: "F2",
      size: 10.2,
      lineHeight: 11.2,
      color: PDF_COLORS.ink,
    });
    if (secondary) {
      pushPdfTextBlock(commands, secondary, x + 10, y + 24, width - 20, {
        font: "F1",
        size: 8.5,
        lineHeight: 9.4,
        color: PDF_COLORS.slate,
      });
    }
  };

  const renderPhotoPanel = (x, y, width, height) => {
    pushPdfRectangle(commands, x, y, width, height, {
      fillColor: PDF_COLORS.white,
      strokeColor: PDF_COLORS.border,
    });

    if (!vesselImage) {
      pushPdfText(commands, "Vessel Image", x + 12, y + height - 18, {
        font: "F2",
        size: 8.2,
        color: PDF_COLORS.slate,
      });
      pushPdfTextBlock(commands, "Add a profile image in the Vessel tab and it will appear in this weekly report.", x + 12, y + height - 30, width - 24, {
        font: "F1",
        size: 9.6,
        lineHeight: 11.2,
        color: PDF_COLORS.ink,
      });
      return;
    }

    const availableWidth = width - 12;
    const availableHeight = height - 12;
    const scale = Math.min(availableWidth / vesselImage.width, availableHeight / vesselImage.height);
    const drawWidth = vesselImage.width * scale;
    const drawHeight = vesselImage.height * scale;
    const drawX = x + ((width - drawWidth) / 2);
    const drawY = y + ((height - drawHeight) / 2);
    pushPdfImage(commands, "ImHero", drawX, drawY, drawWidth, drawHeight);
  };

  const startPage = (isFirstPage = pages.length === 0) => {
    currentPage = {
      commands: [],
      images: [],
    };
    commands = currentPage.commands;
    pages.push(currentPage);
    const pageTop = PDF_PAGE_HEIGHT - PDF_PAGE_MARGIN;

    if (isFirstPage) {
      if (vesselImage) {
        currentPage.images.push({ name: "ImHero", image: vesselImage });
      }

      pushPdfRectangle(commands, PDF_PAGE_MARGIN, pageTop - 44, PDF_CONTENT_WIDTH, 44, {
        fillColor: PDF_COLORS.navy,
      });
      pushPdfText(commands, "Harbor Command", PDF_PAGE_MARGIN + 18, pageTop - 28, {
        font: "F2",
        size: 17,
        color: PDF_COLORS.white,
      });
      pushPdfText(commands, "Owner Weekly Vessel Brief", PDF_PAGE_MARGIN + 18, pageTop - 40, {
        font: "F1",
        size: 9.4,
        color: PDF_COLORS.gold,
      });

      const photoWidth = 176;
      const photoHeight = 124;
      const photoX = PDF_PAGE_MARGIN + PDF_CONTENT_WIDTH - photoWidth;
      const photoY = pageTop - 168;
      const heroTextWidth = PDF_CONTENT_WIDTH - photoWidth - 20;

      renderPhotoPanel(photoX, photoY, photoWidth, photoHeight);

      pushPdfText(commands, vesselTitle, PDF_PAGE_MARGIN, pageTop - 82, {
        font: "F2",
        size: 21,
        color: PDF_COLORS.ink,
      });
      if (vesselMeta) {
        pushPdfText(commands, vesselMeta, PDF_PAGE_MARGIN, pageTop - 100, {
          font: "F1",
          size: 10.2,
          color: PDF_COLORS.slate,
        });
      }
      pushPdfText(commands, periodText, PDF_PAGE_MARGIN, pageTop - 120, {
        font: "F2",
        size: 11.8,
        color: PDF_COLORS.sea,
      });
      pushPdfTextBlock(
        commands,
        `Status ${formatPdfStatusLabel(report?.status)} | Generated ${formatDateTimeForPdf(generatedAt)}`,
        PDF_PAGE_MARGIN,
        pageTop - 132,
        heroTextWidth,
        {
          font: "F1",
          size: 9.4,
          lineHeight: 11.2,
          color: PDF_COLORS.slate,
        }
      );
      pushPdfTextBlock(
        commands,
        `Captain ${snapshot.captain || "-"} | ${snapshot.location || "Location pending"}${snapshot.berth ? ` | Berth ${snapshot.berth}` : ""}`,
        PDF_PAGE_MARGIN,
        pageTop - 148,
        heroTextWidth,
        {
          font: "F1",
          size: 9.4,
          lineHeight: 11.2,
          color: PDF_COLORS.slate,
        }
      );

      const metaCardY = pageTop - 224;
      const metaCardGap = 10;
      const metaCardWidth = (PDF_CONTENT_WIDTH - (metaCardGap * 3)) / 4;
      const metaCards = [
        { label: "Report Status", value: formatPdfStatusLabel(report?.status), accent: true },
        { label: "Entries", value: `${report.entries.length} saved row${report.entries.length === 1 ? "" : "s"}` },
        { label: "Issues Logged", value: `${report.entries.filter((entry) => normalizeTextValue(entry.issues, "").trim()).length}` },
        { label: "Snapshot Captured", value: formatDateTimeForPdf(snapshot.capturedAt || generatedAt) },
      ];

      metaCards.forEach((card, index) => {
        renderMetaPill(
          PDF_PAGE_MARGIN + (index * (metaCardWidth + metaCardGap)),
          metaCardY,
          metaCardWidth,
          52,
          card.label,
          card.value,
          card.accent
        );
      });

      pushPdfText(commands, "Vessel Systems Snapshot", PDF_PAGE_MARGIN, metaCardY - 26, {
        font: "F2",
        size: 13,
        color: PDF_COLORS.ink,
      });
      pushPdfText(commands, "Saved with this report snapshot for quick owner review.", PDF_PAGE_MARGIN, metaCardY - 39, {
        font: "F1",
        size: 9,
        color: PDF_COLORS.slate,
      });

      const snapshotCardGap = 10;
      const snapshotCardWidth = (PDF_CONTENT_WIDTH - (snapshotCardGap * 2)) / 3;
      const snapshotCardHeight = 62;
      const snapshotTopY = metaCardY - 108;
      const snapshotSecondRowY = snapshotTopY - (snapshotCardHeight + 10);

      renderGaugeCard(
        PDF_PAGE_MARGIN,
        snapshotTopY,
        snapshotCardWidth,
        snapshotCardHeight,
        {
          label: "Fuel Reserve",
          percent: snapshot.fuel,
          headline: `${snapshot.fuel}%`,
          detail: snapshot.fuelCapacity ? `${snapshot.fuelCapacity} gal capacity on file` : "Fuel reserve from vessel profile",
          fillColor: PDF_COLORS.gold,
          accent: true,
        }
      );
      renderGaugeCard(
        PDF_PAGE_MARGIN + snapshotCardWidth + snapshotCardGap,
        snapshotTopY,
        snapshotCardWidth,
        snapshotCardHeight,
        {
          label: "Fresh Water",
          percent: snapshot.waterTank,
          headline: `${snapshot.waterTank}%`,
          detail: snapshot.waterCapacity ? `${snapshot.waterCapacity} gal freshwater capacity` : "Water level from vessel profile",
          fillColor: PDF_COLORS.sea,
        }
      );
      renderGaugeCard(
        PDF_PAGE_MARGIN + ((snapshotCardWidth + snapshotCardGap) * 2),
        snapshotTopY,
        snapshotCardWidth,
        snapshotCardHeight,
        {
          label: "Battery / Utilization",
          percent: snapshot.batteryStatus,
          headline: `${snapshot.batteryStatus}% battery`,
          detail: `${snapshot.utilization}% live utilization`,
          fillColor: PDF_COLORS.mint,
        }
      );
      renderGaugeCard(
        PDF_PAGE_MARGIN,
        snapshotSecondRowY,
        snapshotCardWidth,
        snapshotCardHeight,
        {
          label: "Holding Tanks",
          percent: Math.max(snapshot.blackTankLevel, snapshot.greyTank),
          headline: `${Math.max(snapshot.blackTankLevel, snapshot.greyTank)}% full`,
          detail: `Black ${snapshot.blackTankLevel}%${snapshot.blackWaterCapacity ? ` of ${snapshot.blackWaterCapacity} gal` : ""} | Grey ${snapshot.greyTank}%${snapshot.greyWaterCapacity ? ` of ${snapshot.greyWaterCapacity} gal` : ""}`,
          fillColor: PDF_COLORS.coral,
        }
      );
      renderInfoCard(
        PDF_PAGE_MARGIN + snapshotCardWidth + snapshotCardGap,
        snapshotSecondRowY,
        snapshotCardWidth,
        snapshotCardHeight,
        "Bridge / Berth",
        snapshot.location || "Location pending",
        `${snapshot.berth ? `Berth ${snapshot.berth}` : "Berth pending"}${snapshot.captain ? ` | Capt. ${snapshot.captain}` : ""}`
      );
      renderInfoCard(
        PDF_PAGE_MARGIN + ((snapshotCardWidth + snapshotCardGap) * 2),
        snapshotSecondRowY,
        snapshotCardWidth,
        snapshotCardHeight,
        "Hull / Service",
        `L ${formatPdfDimensionValue(snapshot.length)} | B ${formatPdfDimensionValue(snapshot.beam)} | D ${formatPdfDimensionValue(snapshot.draft)}`,
        snapshot.nextService ? `Next service ${formatDateOnlyForPdf(snapshot.nextService)}` : "No next service date saved"
      );

      pushPdfText(commands, "Weekly Report Entries", PDF_PAGE_MARGIN, snapshotSecondRowY - 26, {
        font: "F2",
        size: 13.5,
        color: PDF_COLORS.ink,
      });
      pushPdfText(commands, `${report.entries.length} saved entry${report.entries.length === 1 ? "" : "ies"} for ${vesselTitle}.`, PDF_PAGE_MARGIN, snapshotSecondRowY - 40, {
        font: "F1",
        size: 9.4,
        color: PDF_COLORS.slate,
      });

      cursorY = snapshotSecondRowY - 56;
      return;
    }

    pushPdfRectangle(commands, PDF_PAGE_MARGIN, pageTop - 28, PDF_CONTENT_WIDTH, 28, {
      fillColor: PDF_COLORS.navy,
    });
    pushPdfText(commands, "Harbor Command", PDF_PAGE_MARGIN + 12, pageTop - 18, {
      font: "F2",
      size: 11,
      color: PDF_COLORS.white,
    });
    pushPdfText(commands, `${vesselTitle} | ${periodText}`, PDF_PAGE_MARGIN + 116, pageTop - 18, {
      font: "F1",
      size: 9,
      color: PDF_COLORS.gold,
    });
    pushPdfText(commands, "Weekly Report Entries (continued)", PDF_PAGE_MARGIN, pageTop - 48, {
      font: "F2",
      size: 11.5,
      color: PDF_COLORS.ink,
    });
    cursorY = pageTop - 62;
  };

  const estimateFieldBlockHeight = (label, value) => {
    const valueLines = wrapPdfText(value, entryColumnWidth, 9.2);
    return 12 + (valueLines.length * 10.8) + 6;
  };

  const drawFieldBlock = (x, yTop, label, value, accentColor = PDF_COLORS.sea) => {
    pushPdfText(commands, label, x, yTop - 2, {
      font: "F2",
      size: 7.6,
      color: accentColor,
    });
    const bottom = pushPdfTextBlock(commands, value, x, yTop - 10, entryColumnWidth, {
      font: "F1",
      size: 9.2,
      lineHeight: 10.8,
      color: PDF_COLORS.ink,
    });
    return bottom - 2;
  };

  const estimateEntryHeight = (entry) => {
    const titleLines = wrapPdfText(entry.item || "General update", entryInnerWidth - 86, 11.8);
    const leftHeight = estimateFieldBlockHeight("Work Done", entry.workDone || "No work logged.")
      + estimateFieldBlockHeight("Systems Checked", entry.systemsChecked || "None noted.");
    const rightHeight = estimateFieldBlockHeight("Issues", entry.issues || "None noted.")
      + estimateFieldBlockHeight("Notes", entry.notes || "No additional notes.");
    const contentHeight = Math.max(leftHeight, rightHeight);
    return 24 + (titleLines.length * 12.4) + 10 + contentHeight + 10;
  };

  const ensureSpace = (requiredHeight) => {
    if (!pages.length) {
      startPage(true);
      return;
    }

    if ((cursorY - requiredHeight) < 56) {
      startPage(false);
    }
  };

  const drawEntryBlock = (entry, index) => {
    const blockHeight = estimateEntryHeight(entry);
    ensureSpace(blockHeight);
    const blockBottom = cursorY - blockHeight;

    pushPdfRectangle(commands, PDF_PAGE_MARGIN, blockBottom, entryWidth, blockHeight, {
      fillColor: PDF_COLORS.white,
      strokeColor: PDF_COLORS.border,
    });
    pushPdfRectangle(commands, PDF_PAGE_MARGIN, cursorY - 20, entryWidth, 20, {
      fillColor: PDF_COLORS.sky,
    });
    pushPdfText(commands, `#${String(index + 1).padStart(2, "0")}`, PDF_PAGE_MARGIN + 10, cursorY - 13, {
      font: "F2",
      size: 7.8,
      color: PDF_COLORS.sea,
    });
    pushPdfText(commands, formatDateOnlyForPdf(entry.reportDate), PDF_PAGE_MARGIN + entryWidth - 88, cursorY - 13, {
      font: "F1",
      size: 8.8,
      color: PDF_COLORS.slate,
    });

    const titleLines = wrapPdfText(entry.item || "General update", entryInnerWidth - 86, 11.8);
    let contentTop = cursorY - 28;
    const titleBottom = pushPdfTextBlock(commands, titleLines, PDF_PAGE_MARGIN + 12, contentTop, entryInnerWidth - 86, {
      font: "F2",
      size: 11.8,
      lineHeight: 12.4,
      color: PDF_COLORS.ink,
    });
    contentTop = titleBottom - 4;

    const leftX = PDF_PAGE_MARGIN + 12;
    const rightX = leftX + entryColumnWidth + entryColumnGap;
    const workValue = entry.workDone || "No work logged.";
    const systemsValue = entry.systemsChecked || "None noted.";
    const issuesValue = entry.issues || "None noted.";
    const notesValue = entry.notes || "No additional notes.";

    let leftBottom = drawFieldBlock(leftX, contentTop, "Work Done", workValue);
    leftBottom = drawFieldBlock(leftX, leftBottom, "Systems Checked", systemsValue);

    let rightBottom = drawFieldBlock(
      rightX,
      contentTop,
      "Issues",
      issuesValue,
      issuesValue !== "None noted." ? PDF_COLORS.coral : PDF_COLORS.sea
    );
    rightBottom = drawFieldBlock(rightX, rightBottom, "Notes", notesValue);

    cursorY = blockBottom - 10;
  };

  startPage(true);

  if (!Array.isArray(report.entries) || !report.entries.length) {
    ensureSpace(42);
    pushPdfRectangle(commands, PDF_PAGE_MARGIN, cursorY - 42, entryWidth, 32, {
      fillColor: PDF_COLORS.paper,
      strokeColor: PDF_COLORS.border,
    });
    pushPdfText(commands, "No entries were saved for this weekly report.", PDF_PAGE_MARGIN + 12, cursorY - 28, {
      font: "F1",
      size: 10,
      color: PDF_COLORS.slate,
    });
    cursorY -= 50;
  } else {
    report.entries
      .slice()
      .sort((left, right) => compareDateOnlyStrings(left.reportDate, right.reportDate))
      .forEach((entry, index) => {
        drawEntryBlock(entry, index);
      });
  }

  pages.forEach((pageCommands, pageIndex) => {
    pushPdfText(pageCommands.commands, `Page ${pageIndex + 1} of ${pages.length}`, PDF_PAGE_WIDTH - PDF_PAGE_MARGIN - 60, 22, {
      font: "F1",
      size: 8.4,
      color: PDF_COLORS.slate,
    });
    pushPdfText(pageCommands.commands, "Generated from Harbor Command saved weekly report data", PDF_PAGE_MARGIN, 22, {
      font: "F1",
      size: 8.4,
      color: PDF_COLORS.slate,
    });
  });

  return buildPdfDocument(pages);
}

function normalizeWeeklyReportEntry(item, index = 0, persistedAt = nowIso()) {
  return {
    id: normalizeRowId(item?.id, "report-entry", index),
    item: normalizeTextValue(item?.item, "General update"),
    reportDate: formatDateOnly(item?.reportDate || item?.date || nowIso()),
    workDone: normalizeTextValue(item?.workDone, item?.summary || ""),
    systemsChecked: normalizeTextValue(item?.systemsChecked, ""),
    issues: normalizeTextValue(item?.issues, ""),
    notes: normalizeTextValue(item?.notes, ""),
    sourceWorkOrderId: normalizeTextValue(item?.sourceWorkOrderId ?? item?.source_work_order_id, ""),
    sortOrder: normalizeIntegerValue(item?.sortOrder, index),
    createdAt: normalizeTextValue(item?.createdAt, persistedAt),
    updatedAt: normalizeTextValue(item?.updatedAt, persistedAt),
  };
}

function normalizeWeeklyReportVesselSnapshot(snapshot, vessel = null, capturedAt = nowIso()) {
  const source = snapshot && typeof snapshot === "object" ? snapshot : {};
  const vesselSource = vessel && typeof vessel === "object" ? vessel : {};

  return {
    capturedAt: normalizeTextValue(source.capturedAt, capturedAt),
    name: normalizeTextValue(source.name, vesselSource.name || "Vessel"),
    builder: normalizeTextValue(source.builder, vesselSource.builder || ""),
    model: normalizeTextValue(source.model, vesselSource.model || ""),
    status: normalizeTextValue(source.status, vesselSource.status || ""),
    captain: normalizeTextValue(source.captain, vesselSource.captain || ""),
    location: normalizeTextValue(source.location, vesselSource.location || ""),
    berth: normalizeTextValue(source.berth, vesselSource.berth || ""),
    length: normalizeNumberValue(source.length, vesselSource.length || 0),
    beam: normalizeNumberValue(source.beam, vesselSource.beam || 0),
    draft: normalizeNumberValue(source.draft, vesselSource.draft || 0),
    fuel: normalizeIntegerValue(source.fuel, vesselSource.fuel || 0),
    fuelCapacity: normalizeIntegerValue(source.fuelCapacity, vesselSource.fuelCapacity || vesselSource.fuel_capacity || 0),
    waterTank: normalizeIntegerValue(source.waterTank, vesselSource.waterTank || vesselSource.water_tank || 0),
    waterCapacity: normalizeIntegerValue(source.waterCapacity, vesselSource.waterCapacity || vesselSource.water_capacity || 0),
    greyTank: normalizeIntegerValue(source.greyTank, vesselSource.greyTank || vesselSource.grey_tank || 0),
    greyWaterCapacity: normalizeIntegerValue(source.greyWaterCapacity, vesselSource.greyWaterCapacity || vesselSource.grey_water_capacity || 0),
    blackTankLevel: normalizeIntegerValue(source.blackTankLevel, vesselSource.blackTankLevel || vesselSource.black_tank_level || 0),
    blackWaterCapacity: normalizeIntegerValue(source.blackWaterCapacity, vesselSource.blackWaterCapacity || vesselSource.black_water_capacity || 0),
    batteryStatus: normalizeIntegerValue(source.batteryStatus, vesselSource.batteryStatus || vesselSource.battery_status || 0),
    utilization: normalizeIntegerValue(source.utilization, vesselSource.utilization || 0),
    nextService: normalizeNullableTextValue(source.nextService || vesselSource.nextService || vesselSource.next_service),
  };
}

function parseWeeklyReportVesselSnapshot(rawValue, vessel = null, capturedAt = nowIso()) {
  if (rawValue == null || rawValue === "") {
    return vessel ? normalizeWeeklyReportVesselSnapshot({}, vessel, capturedAt) : null;
  }

  if (typeof rawValue === "object") {
    if (Array.isArray(rawValue) || !Object.keys(rawValue).length) {
      return vessel ? normalizeWeeklyReportVesselSnapshot({}, vessel, capturedAt) : null;
    }
    return normalizeWeeklyReportVesselSnapshot(rawValue, vessel, capturedAt);
  }

  try {
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return vessel ? normalizeWeeklyReportVesselSnapshot({}, vessel, capturedAt) : null;
    }
    return normalizeWeeklyReportVesselSnapshot(parsed, vessel, capturedAt);
  } catch {
    return vessel ? normalizeWeeklyReportVesselSnapshot({}, vessel, capturedAt) : null;
  }
}

function buildWeeklyReportVesselSnapshot(vessel, capturedAt = nowIso()) {
  return normalizeWeeklyReportVesselSnapshot(
    {
      capturedAt,
      name: vessel?.name,
      builder: vessel?.builder,
      model: vessel?.model,
      status: vessel?.status,
      captain: vessel?.captain,
      location: vessel?.location,
      berth: vessel?.berth,
      length: vessel?.length,
      beam: vessel?.beam,
      draft: vessel?.draft,
      fuel: vessel?.fuel,
      fuelCapacity: vessel?.fuelCapacity ?? vessel?.fuel_capacity,
      waterTank: vessel?.waterTank ?? vessel?.water_tank,
      waterCapacity: vessel?.waterCapacity ?? vessel?.water_capacity,
      greyTank: vessel?.greyTank ?? vessel?.grey_tank,
      greyWaterCapacity: vessel?.greyWaterCapacity ?? vessel?.grey_water_capacity,
      blackTankLevel: vessel?.blackTankLevel ?? vessel?.black_tank_level,
      blackWaterCapacity: vessel?.blackWaterCapacity ?? vessel?.black_water_capacity,
      batteryStatus: vessel?.batteryStatus ?? vessel?.battery_status,
      utilization: vessel?.utilization,
      nextService: vessel?.nextService ?? vessel?.next_service,
    },
    vessel,
    capturedAt
  );
}

function normalizeWeeklyReportRecord(item, index = 0, persistedAt = nowIso()) {
  const entriesSource = Array.isArray(item?.entries) ? item.entries : [];
  const entries = entriesSource
    .map((entry, entryIndex) => normalizeWeeklyReportEntry(entry, entryIndex, persistedAt))
    .sort((left, right) => {
      const dateComparison = compareDateOnlyStrings(left.reportDate, right.reportDate);
      if (dateComparison !== 0) {
        return dateComparison;
      }
      return left.sortOrder - right.sortOrder;
    });
  const weekSourceDate = normalizeTextValue(item?.weekStart, "")
    || normalizeTextValue(item?.reportDate, "")
    || entries[0]?.reportDate
    || nowIso();
  const workWeek = getWorkWeekRange(weekSourceDate);
  const weekStart = formatDateOnly(item?.weekStart) || workWeek.start;
  const weekEnd = formatDateOnly(item?.weekEnd) || workWeek.end;

  return {
    id: normalizeRowId(item?.id, "weekly-report", index),
    weekStart,
    weekEnd,
    status: normalizeWeeklyReportStatus(item?.status),
    sortOrder: normalizeIntegerValue(item?.sortOrder, index),
    createdAt: normalizeTextValue(item?.createdAt, persistedAt),
    updatedAt: normalizeTextValue(item?.updatedAt, persistedAt),
    vesselSnapshot: parseWeeklyReportVesselSnapshot(item?.vesselSnapshot ?? item?.vessel_snapshot_json, null, persistedAt),
    entries,
  };
}

function buildWeeklyReportsFromLegacyEntries(entries, persistedAt = nowIso()) {
  const groupedReports = new Map();

  normalizeCollectionItems(entries).forEach((item, index) => {
    const entry = normalizeWeeklyReportEntry(item, index, persistedAt);
    const weekRange = getWorkWeekRange(entry.reportDate);
    const groupKey = `${weekRange.start}:${weekRange.end}`;
    const existingGroup = groupedReports.get(groupKey);

    if (existingGroup) {
      existingGroup.entries.push({
        ...entry,
        sortOrder: existingGroup.entries.length,
      });
      existingGroup.updatedAt = entry.updatedAt || existingGroup.updatedAt;
      return;
    }

    groupedReports.set(groupKey, {
      id: `weekly-report-${weekRange.start}`,
      weekStart: weekRange.start,
      weekEnd: weekRange.end,
      status: compareDateOnlyStrings(weekRange.end, formatDateOnly(nowIso())) < 0 ? "finalized" : "draft",
      sortOrder: groupedReports.size,
      createdAt: entry.createdAt || persistedAt,
      updatedAt: entry.updatedAt || persistedAt,
      entries: [
        {
          ...entry,
          sortOrder: 0,
        },
      ],
    });
  });

  return Array.from(groupedReports.values())
    .sort((left, right) => compareDateOnlyStrings(right.weekStart, left.weekStart))
    .map((report, reportIndex) => ({
      ...report,
      sortOrder: reportIndex,
      entries: report.entries
        .slice()
        .sort((left, right) => {
          const dateComparison = compareDateOnlyStrings(left.reportDate, right.reportDate);
          if (dateComparison !== 0) {
            return dateComparison;
          }
          return left.sortOrder - right.sortOrder;
        })
        .map((entry, entryIndex) => ({
          ...entry,
          sortOrder: entryIndex,
        })),
    }));
}

function normalizeWeeklyReportsCollection(items, persistedAt = nowIso()) {
  const source = normalizeCollectionItems(items);
  if (!source.length) {
    return [];
  }

  const looksLikeWeeklyReports = source.some((item) =>
    Array.isArray(item?.entries)
    || Object.prototype.hasOwnProperty.call(item || {}, "weekStart")
    || Object.prototype.hasOwnProperty.call(item || {}, "weekEnd")
    || Object.prototype.hasOwnProperty.call(item || {}, "status")
  );

  if (!looksLikeWeeklyReports) {
    return buildWeeklyReportsFromLegacyEntries(source, persistedAt);
  }

  return source
    .map((item, index) => normalizeWeeklyReportRecord(item, index, persistedAt))
    .sort((left, right) => compareDateOnlyStrings(right.weekStart, left.weekStart))
    .map((report, reportIndex) => ({
      ...report,
      sortOrder: reportIndex,
      entries: report.entries.map((entry, entryIndex) => ({
        ...entry,
        sortOrder: entryIndex,
      })),
    }));
}

function runInTransaction(callback) {
  database.exec("BEGIN IMMEDIATE");

  try {
    const result = callback();
    database.exec("COMMIT");
    return result;
  } catch (error) {
    try {
      database.exec("ROLLBACK");
    } catch {
    }

    throw error;
  }
}

function hasStructuredState() {
  const row = database.prepare("SELECT COUNT(*) AS total FROM vessels").get();
  return Number(row?.total || 0) > 0;
}

function loadWeeklyReportsForVessel(vesselId) {
  const reports = database.prepare(`
    SELECT id, week_start, week_end, status, vessel_snapshot_json, sort_order, created_at, updated_at
    FROM weekly_reports
    WHERE vessel_id = ?
    ORDER BY week_start DESC, sort_order ASC, id ASC
  `).all(vesselId).map((row) => ({
    id: row.id,
    weekStart: row.week_start,
    weekEnd: row.week_end,
    status: row.status,
    vesselSnapshot: parseWeeklyReportVesselSnapshot(row.vessel_snapshot_json, null, row.updated_at || row.created_at || nowIso()),
    sortOrder: row.sort_order,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    entries: [],
  }));

  if (!reports.length) {
    return [];
  }

  const entries = database.prepare(`
    SELECT e.id, e.report_id, e.item, e.report_date, e.work_done, e.systems_checked, e.issues, e.notes, e.source_work_order_id, e.sort_order, e.created_at, e.updated_at
    FROM weekly_report_entries e
    INNER JOIN weekly_reports r ON r.id = e.report_id
    WHERE r.vessel_id = ?
    ORDER BY r.week_start DESC, e.report_date ASC, e.sort_order ASC, e.id ASC
  `).all(vesselId);

  const entriesByReportId = new Map();
  entries.forEach((row) => {
    const bucket = entriesByReportId.get(row.report_id) || [];
    bucket.push({
      id: row.id,
      item: row.item,
      reportDate: row.report_date,
      workDone: row.work_done || "",
      systemsChecked: row.systems_checked || "",
      issues: row.issues || "",
      notes: row.notes || "",
      sourceWorkOrderId: row.source_work_order_id || "",
      sortOrder: row.sort_order,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    });
    entriesByReportId.set(row.report_id, bucket);
  });

  return reports.map((report) => ({
    ...report,
    entries: (entriesByReportId.get(report.id) || []).map((entry, index) => ({
      ...entry,
      sortOrder: index,
    })),
  }));
}

function migrateLegacyReportsIfNeeded() {
  const legacyCount = Number(database.prepare("SELECT COUNT(*) AS total FROM reports").get()?.total || 0);
  const weeklyCount = Number(database.prepare("SELECT COUNT(*) AS total FROM weekly_reports").get()?.total || 0);

  if (!legacyCount || weeklyCount) {
    return;
  }

  const legacyRows = database.prepare(`
    SELECT id, vessel_id, item, report_date, work_done, systems_checked, issues, sort_order, created_at, updated_at
    FROM reports
    ORDER BY vessel_id ASC, report_date ASC, sort_order ASC, id ASC
  `).all();

  const rowsByVessel = new Map();
  legacyRows.forEach((row) => {
    const bucket = rowsByVessel.get(row.vessel_id) || [];
    bucket.push({
      id: row.id,
      item: row.item,
      reportDate: row.report_date,
      workDone: row.work_done || "",
      systemsChecked: row.systems_checked || "",
      issues: row.issues || "",
      notes: "",
      sortOrder: row.sort_order,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    });
    rowsByVessel.set(row.vessel_id, bucket);
  });

  runInTransaction(() => {
    const insertWeeklyReportStatement = database.prepare(`
      INSERT INTO weekly_reports (
        id, vessel_id, week_start, week_end, status, vessel_snapshot_json, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertWeeklyReportEntryStatement = database.prepare(`
      INSERT INTO weekly_report_entries (
        id, report_id, item, report_date, work_done, systems_checked, issues, notes, source_work_order_id, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    rowsByVessel.forEach((rows, vesselId) => {
      const weeklyReports = normalizeWeeklyReportsCollection(rows);
      weeklyReports.forEach((report, reportIndex) => {
        const reportId = normalizeRowId(report.id, `weekly-report-v${vesselId}`, reportIndex);
        insertWeeklyReportStatement.run(
          reportId,
          vesselId,
          report.weekStart,
          report.weekEnd,
          normalizeWeeklyReportStatus(report.status),
          JSON.stringify(parseWeeklyReportVesselSnapshot(report.vesselSnapshot, null, report.updatedAt || report.createdAt || nowIso())),
          reportIndex,
          normalizeTextValue(report.createdAt, nowIso()),
          normalizeTextValue(report.updatedAt, nowIso())
        );

        report.entries.forEach((entry, entryIndex) => {
          insertWeeklyReportEntryStatement.run(
            normalizeRowId(entry.id, `${reportId}-entry`, entryIndex),
            reportId,
            normalizeTextValue(entry.item, "General update"),
            formatDateOnly(entry.reportDate || report.weekStart),
            normalizeTextValue(entry.workDone, ""),
            normalizeTextValue(entry.systemsChecked, ""),
            normalizeTextValue(entry.issues, ""),
            normalizeTextValue(entry.notes, ""),
            normalizeTextValue(entry.sourceWorkOrderId, ""),
            entryIndex,
            normalizeTextValue(entry.createdAt, nowIso()),
            normalizeTextValue(entry.updatedAt, nowIso())
          );
        });
      });
    });
  });
}

function normalizeWorkOrderWorkspaceRecord(row, index = 0) {
  const reportDate = formatDateOnly(
    row?.reportDate
    || row?.entryDate
    || row?.entry_date
    || row?.report_date
    || row?.dueDate
    || row?.due_date
    || nowIso()
  );
  const weekRange = getWorkWeekRange(row?.weekStart || row?.week_start || row?.weekEnd || row?.week_end || reportDate);
  const item = normalizeTextValue(row?.item ?? row?.title, `Weekly entry ${index + 1}`);

  return {
    id: row?.id || randomUUID(),
    item,
    title: item,
    reportDate,
    dueDate: reportDate,
    workDone: normalizeTextValue(row?.work_done ?? row?.workDone, ""),
    systemsChecked: normalizeTextValue(row?.systems_checked ?? row?.systemsChecked, ""),
    issues: normalizeTextValue(row?.issues, ""),
    notes: normalizeTextValue(row?.notes, ""),
    weekStart: formatDateOnly(row?.weekStart || row?.week_start || weekRange.start),
    weekEnd: formatDateOnly(row?.weekEnd || row?.week_end || weekRange.end),
    priority: normalizeTextValue(row?.priority, ""),
    status: normalizeTextValue(row?.status, ""),
    maintenanceLogId: normalizeTextValue(row?.maintenance_log_id ?? row?.maintenanceLogId, ""),
    originType: normalizeWorkOrderOriginType(row?.origin_type ?? row?.originType),
    completedAt: normalizeNullableTextValue(row?.completed_at ?? row?.completedAt),
    sortOrder: Number.isFinite(Number(row?.sort_order)) ? Number(row.sort_order) : index,
    createdAt: normalizeTextValue(row?.createdAt ?? row?.created_at, nowIso()),
    updatedAt: normalizeTextValue(row?.updatedAt ?? row?.updated_at, nowIso()),
  };
}

function normalizeWorkOrderOriginType(value) {
  const normalized = normalizeTextValue(value, "manual").trim().toLowerCase();
  return normalized === "maintenance-suggestion" ? "maintenance-suggestion" : "manual";
}

function isCompletedWorkOrderStatus(status) {
  return normalizeTextValue(status, "").trim().toLowerCase() === "completed";
}

function hasWorkOrderNarrativeContent(item) {
  return Boolean(
    normalizeTextValue(item?.workDone, "").trim()
    || normalizeTextValue(item?.systemsChecked, "").trim()
    || normalizeTextValue(item?.issues, "").trim()
    || normalizeTextValue(item?.notes, "").trim()
  );
}

function shouldIncludeWorkOrderInWeeklyReport(item) {
  const normalized = normalizeWorkOrderWorkspaceRecord(item);
  if (normalized.originType !== "maintenance-suggestion") {
    return true;
  }

  return isCompletedWorkOrderStatus(normalized.status) || hasWorkOrderNarrativeContent(normalized);
}

function filterWorkOrdersForWeeklyReport(entries) {
  return (Array.isArray(entries) ? entries : []).filter((entry) => shouldIncludeWorkOrderInWeeklyReport(entry));
}

function addDaysToDateOnly(value, days) {
  const parsed = parseDateOnly(value) || parseDateOnly(nowIso());
  const nextDate = new Date(parsed);
  nextDate.setDate(nextDate.getDate() + normalizeIntegerValue(days, 0));
  return formatDateOnly(nextDate);
}

function clampDateToWeekRange(value, weekStart, weekEnd) {
  const normalizedValue = formatDateOnly(value || weekStart);
  if (compareDateOnlyStrings(normalizedValue, weekStart) < 0) {
    return weekStart;
  }
  if (compareDateOnlyStrings(normalizedValue, weekEnd) > 0) {
    return weekEnd;
  }
  return normalizedValue;
}

function isMaintenanceTaskSuggestionActive(task, weekRange, assetsById = new Map(), options = {}) {
  if (usesDateRecurrence(task)) {
    const dueDate = formatDateOnly(task?.dueDate);
    if (dueDate) {
      const reminderDays = Math.max(0, normalizeIntegerValue(task?.reminderDays, 0));
      const reminderStart = addDaysToDateOnly(dueDate, -reminderDays);
      if (compareDateOnlyStrings(reminderStart, weekRange.end) <= 0) {
        return true;
      }
    }
  }

  if (usesHourRecurrence(task)) {
    const hoursRemaining = getMaintenanceHoursRemaining(task, assetsById, options);
    if (hoursRemaining !== null) {
      const reminderHours = Math.max(0, normalizeIntegerValue(task?.reminderHours, 0));
      if (hoursRemaining <= reminderHours) {
        return true;
      }
    }
  }

  return false;
}

function buildSuggestedMaintenanceWorkOrder(task, weekRange, timestamp) {
  const preferredDate = clampDateToWeekRange(task?.dueDate || weekRange.start, weekRange.start, weekRange.end);
  return normalizeWorkOrderWorkspaceRecord(
    {
      id: `maintenance-suggestion-${normalizeTextValue(task?.id, "task")}-${weekRange.start}`,
      title: normalizeTextValue(task?.title, "Recurring maintenance"),
      priority: normalizeTextValue(task?.priority, "Medium"),
      status: "Open",
      notes: normalizeTextValue(
        task?.notes,
        `Suggested from recurring maintenance for the ${task?.category || "General"} system.`
      ),
      week_start: weekRange.start,
      week_end: weekRange.end,
      entry_date: preferredDate,
      work_done: "",
      systems_checked: "",
      issues: "",
      maintenance_log_id: normalizeTextValue(task?.id, ""),
      origin_type: "maintenance-suggestion",
      completed_at: "",
      created_at: timestamp,
      updated_at: timestamp,
    }
  );
}

function loadMaintenanceHistoryRecords() {
  const rows = database.prepare(`
    SELECT id, vessel_id, maintenance_log_id, asset_id, template_task_id, work_order_id, source, completed_at, completion_date,
           completed_hours, work_done, systems_checked, issues, notes, created_at, updated_at
    FROM maintenance_history
    ORDER BY completion_date DESC, created_at DESC, id DESC
  `).all().map((row, index) => normalizeMaintenanceHistoryRecord(row, index));

  const byWorkOrderId = new Map();
  const byMaintenanceDate = new Map();

  rows.forEach((row) => {
    if (row.workOrderId) {
      byWorkOrderId.set(String(row.workOrderId), row);
    }
    byMaintenanceDate.set(
      [String(row.vesselId), String(row.maintenanceLogId), formatDateOnly(row.completionDate), String(row.source || "manual")].join("::"),
      row
    );
  });

  return {
    rows,
    byWorkOrderId,
    byMaintenanceDate,
  };
}

function mergeMaintenanceHistoryRecords(existingHistory, nextEntries) {
  const merged = existingHistory.rows.map((row) => ({ ...row }));
  const byWorkOrderId = new Map(existingHistory.byWorkOrderId);
  const byMaintenanceDate = new Map(existingHistory.byMaintenanceDate);

  nextEntries.forEach((entry) => {
    const nextRow = normalizeMaintenanceHistoryRecord(entry);
    const workOrderKey = nextRow.workOrderId ? String(nextRow.workOrderId) : "";
    const maintenanceDateKey = [
      String(nextRow.vesselId),
      String(nextRow.maintenanceLogId),
      nextRow.completionDate,
      nextRow.source,
    ].join("::");

    const existingByWorkOrder = workOrderKey ? byWorkOrderId.get(workOrderKey) : null;
    const existingByDate = byMaintenanceDate.get(maintenanceDateKey);
    const existing = existingByWorkOrder || existingByDate;

    if (existing) {
      Object.assign(existing, nextRow, {
        id: existing.id,
        createdAt: existing.createdAt || nextRow.createdAt,
      });
      return;
    }

    merged.push(nextRow);
    if (workOrderKey) {
      byWorkOrderId.set(workOrderKey, nextRow);
    }
    byMaintenanceDate.set(maintenanceDateKey, nextRow);
  });

  return merged;
}

function applyConnectedAutomationToBundle(bundle, vesselId, options = {}) {
  const persistedAt = options.persistedAt || nowIso();
  const activeWeek = getWorkWeekRange(options.activeWorkWeekStart || nowIso());
  const maintenanceAssets = normalizeCollectionItems(bundle?.maintenanceAssets).map((item, index) =>
    normalizeMaintenanceAssetRecord(item, index)
  );
  const maintenance = normalizeCollectionItems(bundle?.maintenance).map((item, index) =>
    normalizeMaintenanceTaskRecord(item, index)
  );
  let workOrders = normalizeCollectionItems(bundle?.workOrders).map((item, index) =>
    normalizeWorkOrderWorkspaceRecord(item, index)
  );
  const incomingHistory = normalizeCollectionItems(bundle?.maintenanceHistory).map((item, index) =>
    normalizeMaintenanceHistoryRecord(item, index)
  );
  const maintenanceById = new Map(maintenance.map((item) => [String(item.id), item]));
  const assetsById = new Map(maintenanceAssets.map((item) => [String(item.id), item]));
  const historyEntries = [];
  const automationOptions = {
    engines: Array.isArray(options.engines) ? options.engines : [],
    generators: Array.isArray(options.generators) ? options.generators : [],
  };

  workOrders = workOrders.map((order, index) => ({
    ...order,
    sortOrder: Number.isFinite(Number(order.sortOrder)) ? Number(order.sortOrder) : index,
  }));

  workOrders.forEach((order) => {
    if (!order.maintenanceLogId || !isCompletedWorkOrderStatus(order.status)) {
      return;
    }

    const linkedTask = maintenanceById.get(String(order.maintenanceLogId));
    if (!linkedTask) {
      return;
    }

    const completionDate = formatDateOnly(order.reportDate || order.completedAt || persistedAt) || formatDateOnly(persistedAt);
    linkedTask.status = "Completed";
    order.completedAt = normalizeTextValue(order.completedAt, persistedAt);
    if (usesDateRecurrence(linkedTask)) {
      linkedTask.lastCompleted = completionDate;
      linkedTask.dueDate = linkedTask.intervalDays > 0
        ? addDaysToDateOnly(completionDate, normalizeIntegerValue(linkedTask.intervalDays, 0))
        : "";
    }
    const completedHours = usesHourRecurrence(linkedTask)
      ? getMaintenanceCurrentHours(linkedTask, assetsById, automationOptions)
      : 0;
    if (usesHourRecurrence(linkedTask)) {
      linkedTask.lastCompletedHours = completedHours;
      linkedTask.dueHours = linkedTask.intervalHours > 0
        ? completedHours + normalizeIntegerValue(linkedTask.intervalHours, 0)
        : 0;
    }

    historyEntries.push({
      id: `maintenance-history-${order.id}`,
      vesselId,
      maintenanceLogId: linkedTask.id,
      assetId: linkedTask.assetId,
      templateTaskId: linkedTask.templateTaskId,
      workOrderId: order.id,
      source: "work-order",
      completedAt: order.completedAt,
      completionDate,
      completedHours,
      workDone: order.workDone,
      systemsChecked: order.systemsChecked,
      issues: order.issues,
      notes: order.notes,
      createdAt: order.createdAt || persistedAt,
      updatedAt: order.updatedAt || persistedAt,
    });
  });

  maintenance.forEach((task) => {
    if (!task.lastCompleted && !task.lastCompletedHours) {
      return;
    }
    historyEntries.push({
      id: `maintenance-manual-${task.id}-${task.lastCompleted || task.lastCompletedHours || "baseline"}`,
      vesselId,
      maintenanceLogId: task.id,
      assetId: task.assetId,
      templateTaskId: task.templateTaskId,
      workOrderId: "",
      source: "manual",
      completedAt: task.lastCompleted || persistedAt,
      completionDate: task.lastCompleted || persistedAt,
      completedHours: normalizeIntegerValue(task.lastCompletedHours, 0),
      workDone: "",
      systemsChecked: "",
      issues: "",
      notes: task.notes,
      createdAt: persistedAt,
      updatedAt: persistedAt,
    });
  });

  workOrders = workOrders.filter((order) => {
    if (order.originType !== "maintenance-suggestion" || isCompletedWorkOrderStatus(order.status)) {
      return true;
    }

    const linkedTask = maintenanceById.get(String(order.maintenanceLogId));
    if (!linkedTask) {
      return hasWorkOrderNarrativeContent(order);
    }

    if (isMaintenanceTaskSuggestionActive(linkedTask, activeWeek, assetsById, automationOptions)) {
      return true;
    }

    return hasWorkOrderNarrativeContent(order);
  });

  maintenance.forEach((task) => {
    if (!isMaintenanceTaskSuggestionActive(task, activeWeek, assetsById, automationOptions)) {
      return;
    }

    const existingLinkedOrder = workOrders.find((order) =>
      String(order.maintenanceLogId) === String(task.id)
      && order.weekStart === activeWeek.start
      && order.weekEnd === activeWeek.end
      && !isCompletedWorkOrderStatus(order.status)
    );

    if (existingLinkedOrder) {
      return;
    }

    workOrders.unshift(buildSuggestedMaintenanceWorkOrder(task, activeWeek, persistedAt));
  });

  workOrders = workOrders
    .sort((left, right) =>
      compareDateOnlyStrings(left.reportDate, right.reportDate)
      || normalizeIntegerValue(left.sortOrder, 0) - normalizeIntegerValue(right.sortOrder, 0)
      || String(left.item).localeCompare(String(right.item))
    )
    .map((order, index) => ({
      ...order,
      sortOrder: index,
    }));

  return {
    maintenanceAssets,
    maintenance,
    workOrders,
    historyEntries: mergeMaintenanceHistoryRecords(
      {
        rows: incomingHistory,
        byWorkOrderId: new Map(incomingHistory.filter((row) => row.workOrderId).map((row) => [String(row.workOrderId), row])),
        byMaintenanceDate: new Map(
          incomingHistory.map((row) => ([
            [String(row.vesselId), String(row.maintenanceLogId), formatDateOnly(row.completionDate), String(row.source || "manual")].join("::"),
            row,
          ]))
        ),
      },
      historyEntries
    ),
  };
}

function normalizeWorkOrderFieldSet(record) {
  return {
    workDone: normalizeTextValue(record?.work_done ?? record?.workDone, "").trim(),
    systemsChecked: normalizeTextValue(record?.systems_checked ?? record?.systemsChecked, "").trim(),
    issues: normalizeTextValue(record?.issues, "").trim(),
    notes: normalizeTextValue(record?.notes, "").trim(),
  };
}

function hasWorkOrderFieldContent(fieldSet) {
  return Boolean(
    normalizeTextValue(fieldSet?.workDone, "").trim()
    || normalizeTextValue(fieldSet?.systemsChecked, "").trim()
    || normalizeTextValue(fieldSet?.issues, "").trim()
    || normalizeTextValue(fieldSet?.notes, "").trim()
  );
}

function buildWorkOrderPreservationKey(vesselId, weekStart, weekEnd, reportDate, title) {
  return [
    String(vesselId ?? "").trim(),
    formatDateOnly(weekStart || reportDate || nowIso()),
    formatDateOnly(weekEnd || weekStart || reportDate || nowIso()),
    formatDateOnly(reportDate || weekStart || nowIso()),
    normalizeTextValue(title, "").trim().toLowerCase(),
  ].join("::");
}

function buildWorkOrderWeekTitleKey(vesselId, weekStart, weekEnd, title) {
  return [
    String(vesselId ?? "").trim(),
    formatDateOnly(weekStart || nowIso()),
    formatDateOnly(weekEnd || weekStart || nowIso()),
    normalizeTextValue(title, "").trim().toLowerCase(),
  ].join("::");
}

function mergeWorkOrderFieldSets(primary, secondary) {
  const normalizedPrimary = normalizeWorkOrderFieldSet(primary || {});
  const normalizedSecondary = normalizeWorkOrderFieldSet(secondary || {});
  return {
    workDone: normalizedPrimary.workDone || normalizedSecondary.workDone || "",
    systemsChecked: normalizedPrimary.systemsChecked || normalizedSecondary.systemsChecked || "",
    issues: normalizedPrimary.issues || normalizedSecondary.issues || "",
    notes: normalizedPrimary.notes || normalizedSecondary.notes || "",
  };
}

function createWorkOrderPreservationIndex() {
  const index = {
    byId: new Map(),
    byKey: new Map(),
    byWeekTitle: new Map(),
  };

  const existingWorkOrders = database.prepare(`
    SELECT id, vessel_id, title, week_start, week_end, entry_date, work_done, systems_checked, issues, notes
    FROM work_orders
  `).all();

  existingWorkOrders.forEach((row) => {
    const fields = normalizeWorkOrderFieldSet(row);
    if (!hasWorkOrderFieldContent(fields)) {
      return;
    }

    index.byId.set(String(row.id), fields);
    index.byKey.set(
      buildWorkOrderPreservationKey(row.vessel_id, row.week_start, row.week_end, row.entry_date, row.title),
      fields
    );
    index.byWeekTitle.set(
      buildWorkOrderWeekTitleKey(row.vessel_id, row.week_start, row.week_end, row.title),
      mergeWorkOrderFieldSets(index.byWeekTitle.get(buildWorkOrderWeekTitleKey(row.vessel_id, row.week_start, row.week_end, row.title)) || {}, fields)
    );
  });

  const existingReportEntries = database.prepare(`
    SELECT r.vessel_id, r.week_start, r.week_end, e.item, e.report_date, e.work_done, e.systems_checked, e.issues, e.notes
    FROM weekly_report_entries e
    INNER JOIN weekly_reports r ON r.id = e.report_id
  `).all();

  existingReportEntries.forEach((row) => {
    const fields = normalizeWorkOrderFieldSet(row);
    if (!hasWorkOrderFieldContent(fields)) {
      return;
    }

    const key = buildWorkOrderPreservationKey(row.vessel_id, row.week_start, row.week_end, row.report_date, row.item);
    const current = index.byKey.get(key);
    index.byKey.set(key, mergeWorkOrderFieldSets(current || {}, fields));
    const weekTitleKey = buildWorkOrderWeekTitleKey(row.vessel_id, row.week_start, row.week_end, row.item);
    index.byWeekTitle.set(weekTitleKey, mergeWorkOrderFieldSets(index.byWeekTitle.get(weekTitleKey) || {}, fields));
  });

  return index;
}

function resolvePreservedWorkOrderFields(index, descriptor) {
  if (!index) {
    return normalizeWorkOrderFieldSet({});
  }

  const preservedById = descriptor?.id ? index.byId.get(String(descriptor.id)) : null;
  const preservedByKey = index.byKey.get(
    buildWorkOrderPreservationKey(
      descriptor?.vesselId,
      descriptor?.weekStart,
      descriptor?.weekEnd,
      descriptor?.reportDate,
      descriptor?.title
    )
  );
  const preservedByWeekTitle = index.byWeekTitle.get(
    buildWorkOrderWeekTitleKey(
      descriptor?.vesselId,
      descriptor?.weekStart,
      descriptor?.weekEnd,
      descriptor?.title
    )
  );

  return mergeWorkOrderFieldSets(
    mergeWorkOrderFieldSets(preservedById || {}, preservedByKey || {}),
    preservedByWeekTitle || {}
  );
}

function loadWorkOrdersForWeek(vesselId, weekStart, weekEnd) {
  return database.prepare(`
    SELECT id, title, priority, status, due_date, notes, sort_order, created_at, updated_at,
           week_start, week_end, entry_date, work_done, systems_checked, issues, maintenance_log_id, origin_type, completed_at
    FROM work_orders
    WHERE vessel_id = ?
      AND (
        (week_start = ? AND week_end = ?)
        OR (entry_date >= ? AND entry_date <= ?)
        OR ((entry_date IS NULL OR entry_date = '') AND due_date >= ? AND due_date <= ?)
      )
    ORDER BY entry_date ASC, sort_order ASC, id ASC
  `)
    .all(vesselId, weekStart, weekEnd, weekStart, weekEnd, weekStart, weekEnd)
    .map((row, index) => normalizeWorkOrderWorkspaceRecord(row, index));
}

function migrateCurrentWeekWorkspaceIfNeeded() {
  const vessels = database.prepare("SELECT id FROM vessels ORDER BY display_order ASC, id ASC").all();
  if (!vessels.length) {
    return;
  }

  const currentWeek = getWorkWeekRange(nowIso());
  const existingWorkOrders = database.prepare(`
    SELECT id, vessel_id, title, priority, status, due_date, notes, sort_order, created_at, updated_at,
           week_start, week_end, entry_date, work_done, systems_checked, issues
    FROM work_orders
    ORDER BY vessel_id ASC, sort_order ASC, id ASC
  `).all();
  const updateWorkOrderStatement = database.prepare(`
    UPDATE work_orders
    SET week_start = ?, week_end = ?, entry_date = ?, notes = ?, work_done = ?, systems_checked = ?, issues = ?, updated_at = ?
    WHERE id = ?
  `);
  const insertWorkOrderStatement = database.prepare(`
    INSERT INTO work_orders (
      id, vessel_id, title, priority, status, due_date, notes, sort_order, created_at, updated_at,
      week_start, week_end, entry_date, work_done, systems_checked, issues
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const currentWeekReportIdStatement = database.prepare(`
    SELECT id
    FROM weekly_reports
    WHERE vessel_id = ? AND week_start = ? AND week_end = ?
    ORDER BY datetime(updated_at) DESC, id DESC
    LIMIT 1
  `);
  const currentWeekReportEntriesStatement = database.prepare(`
    SELECT id, item, report_date, work_done, systems_checked, issues, notes, source_work_order_id, sort_order, created_at, updated_at
    FROM weekly_report_entries
    WHERE report_id = ?
    ORDER BY report_date ASC, sort_order ASC, id ASC
  `);
  const currentWeekReportEntryLookup = new Map();

  runInTransaction(() => {
    vessels.forEach((vessel) => {
      const currentWeekReport = currentWeekReportIdStatement.get(vessel.id, currentWeek.start, currentWeek.end);
      if (!currentWeekReport?.id) {
        return;
      }

      const reportEntries = currentWeekReportEntriesStatement.all(currentWeekReport.id);
      reportEntries.forEach((entry) => {
        currentWeekReportEntryLookup.set(
          buildWorkOrderPreservationKey(
            vessel.id,
            currentWeek.start,
            currentWeek.end,
            formatDateOnly(entry.report_date || currentWeek.start),
            normalizeTextValue(entry.item, "")
          ),
          normalizeWorkOrderFieldSet(entry)
        );
      });
    });

    existingWorkOrders.forEach((row, index) => {
      const fallbackFields = currentWeekReportEntryLookup.get(
        buildWorkOrderPreservationKey(
          row.vessel_id,
          row.week_start || currentWeek.start,
          row.week_end || currentWeek.end,
          row.entry_date || row.due_date || currentWeek.start,
          row.title
        )
      );
      const mergedRow = {
        ...row,
        ...mergeWorkOrderFieldSets(normalizeWorkOrderFieldSet(row), fallbackFields || {}),
      };
      const normalized = normalizeWorkOrderWorkspaceRecord(mergedRow, index);
      const needsUpdate =
        row.week_start !== normalized.weekStart
        || row.week_end !== normalized.weekEnd
        || row.entry_date !== normalized.reportDate
        || normalizeTextValue(row.notes, "") !== normalized.notes
        || normalizeTextValue(row.work_done, "") !== normalized.workDone
        || normalizeTextValue(row.systems_checked, "") !== normalized.systemsChecked
        || normalizeTextValue(row.issues, "") !== normalized.issues;

      if (!needsUpdate) {
        return;
      }

      updateWorkOrderStatement.run(
        normalized.weekStart,
        normalized.weekEnd,
        normalized.reportDate,
        normalized.notes,
        normalized.workDone,
        normalized.systemsChecked,
        normalized.issues,
        nowIso(),
        row.id
      );
    });

    vessels.forEach((vessel) => {
      const currentWorkspaceEntries = loadWorkOrdersForWeek(vessel.id, currentWeek.start, currentWeek.end);
      if (currentWorkspaceEntries.length) {
        return;
      }

      const currentWeekReport = currentWeekReportIdStatement.get(vessel.id, currentWeek.start, currentWeek.end);
      if (!currentWeekReport?.id) {
        return;
      }

      const reportEntries = currentWeekReportEntriesStatement.all(currentWeekReport.id);
      reportEntries.forEach((entry, index) => {
        insertWorkOrderStatement.run(
          randomUUID(),
          vessel.id,
          normalizeTextValue(entry.item, `Weekly entry ${index + 1}`),
          "Medium",
          "Open",
          formatDateOnly(entry.report_date || currentWeek.start),
          normalizeTextValue(entry.notes, ""),
          index,
          normalizeTextValue(entry.created_at, nowIso()),
          normalizeTextValue(entry.updated_at, nowIso()),
          currentWeek.start,
          currentWeek.end,
          formatDateOnly(entry.report_date || currentWeek.start),
          normalizeTextValue(entry.work_done, ""),
          normalizeTextValue(entry.systems_checked, ""),
          normalizeTextValue(entry.issues, "")
        );
      });
    });
  });
}

function repairWorkOrderDatesFromReportsIfNeeded() {
  const invalidRows = database.prepare(`
    SELECT id, vessel_id, title, week_start, week_end, entry_date, due_date
    FROM work_orders
    WHERE week_start IS NOT NULL
      AND week_start != ''
      AND week_end IS NOT NULL
      AND week_end != ''
      AND (
        entry_date IS NULL
        OR entry_date = ''
        OR entry_date < week_start
        OR entry_date > week_end
      )
  `).all();

  if (!invalidRows.length) {
    return;
  }

  const reportEntryLookup = new Map();
  database.prepare(`
    SELECT r.vessel_id, r.week_start, r.week_end, e.item, e.report_date
    FROM weekly_report_entries e
    INNER JOIN weekly_reports r ON r.id = e.report_id
    ORDER BY r.week_start DESC, e.sort_order ASC, e.id ASC
  `).all().forEach((row) => {
    const key = [
      String(row.vessel_id),
      formatDateOnly(row.week_start || nowIso()),
      formatDateOnly(row.week_end || nowIso()),
      normalizeTextValue(row.item, "").trim().toLowerCase(),
    ].join("::");

    if (!reportEntryLookup.has(key)) {
      reportEntryLookup.set(key, formatDateOnly(row.report_date || row.week_start || nowIso()));
    }
  });

  const updateStatement = database.prepare(`
    UPDATE work_orders
    SET entry_date = ?, due_date = ?, updated_at = ?
    WHERE id = ?
  `);

  runInTransaction(() => {
    invalidRows.forEach((row) => {
      const lookupKey = [
        String(row.vessel_id),
        formatDateOnly(row.week_start || nowIso()),
        formatDateOnly(row.week_end || nowIso()),
        normalizeTextValue(row.title, "").trim().toLowerCase(),
      ].join("::");
      const weekRange = getWorkWeekRange(row.week_start || row.week_end || nowIso());
      const repairedDate = reportEntryLookup.get(lookupKey)
        || clampDateToWeekRange(row.due_date || row.entry_date || weekRange.start, weekRange.start, weekRange.end);

      updateStatement.run(
        repairedDate,
        repairedDate,
        nowIso(),
        row.id
      );
    });
  });
}

function loadStructuredBundleForVessel(vesselId) {
  return {
    maintenanceAssets: database.prepare(`
      SELECT id, template_id, name, asset_type, manufacturer, model, serial_number, location,
             meter_source_type, meter_source_id, current_hours, notes, sort_order, created_at, updated_at
      FROM maintenance_assets
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row, index) => normalizeMaintenanceAssetRecord(row, index)),
    maintenance: database.prepare(`
      SELECT id, title, category, status, priority, asset_id, template_id, template_task_id,
             due_date, due_hours, last_completed, last_completed_hours, interval_days, interval_hours,
             reminder_days, reminder_hours, recurrence_mode, meter_source_type, meter_source_id, is_custom,
             notes, sort_order, created_at, updated_at
      FROM maintenance_logs
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row, index) => normalizeMaintenanceTaskRecord(row, index)),
    maintenanceHistory: database.prepare(`
      SELECT id, vessel_id, maintenance_log_id, asset_id, template_task_id, work_order_id, source, completed_at,
             completion_date, completed_hours, work_done, systems_checked, issues, notes, created_at, updated_at
      FROM maintenance_history
      WHERE vessel_id = ?
      ORDER BY completion_date DESC, created_at DESC, id DESC
    `).all(vesselId).map((row, index) => normalizeMaintenanceHistoryRecord(row, index)),
    workOrders: database.prepare(`
      SELECT id, title, priority, status, due_date, notes, sort_order, created_at, updated_at,
             week_start, week_end, entry_date, work_done, systems_checked, issues, maintenance_log_id, origin_type, completed_at
      FROM work_orders
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row, index) => normalizeWorkOrderWorkspaceRecord(row, index)),
    inventory: database.prepare(`
      SELECT id, name, location, quantity, unit, minimum_quantity, status, notes
      FROM inventory
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row) => ({
      id: row.id,
      name: row.name,
      location: row.location || "",
      quantity: row.quantity,
      unit: row.unit || "",
      minimumQuantity: row.minimum_quantity,
      status: row.status || "",
      notes: row.notes || "",
    })),
    expenses: database.prepare(`
      SELECT id, title, vendor, category, amount, currency, expense_date, status, notes
      FROM expenses
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row) => ({
      id: row.id,
      title: row.title,
      vendor: row.vendor || "",
      category: row.category || "",
      amount: row.amount,
      currency: row.currency || "USD",
      expenseDate: row.expense_date || "",
      status: row.status || "",
      notes: row.notes || "",
    })),
    charters: database.prepare(`
      SELECT id, client, start_date, end_date, berth, status
      FROM charters
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row) => ({
      id: row.id,
      client: row.client,
      start: row.start_date,
      end: row.end_date,
      berth: row.berth || "",
      status: row.status,
    })),
    crew: database.prepare(`
      SELECT id, name, role, certification, rotation
      FROM crew_members
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row) => ({
      id: row.id,
      name: row.name,
      role: row.role,
      certification: row.certification || "",
      rotation: row.rotation || "",
    })),
    reports: loadWeeklyReportsForVessel(vesselId),
    vendors: database.prepare(`
      SELECT id, name, contact, email, phone, status, category
      FROM vendors
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row) => ({
      id: row.id,
      name: row.name,
      contact: row.contact || "",
      email: row.email || "",
      phone: row.phone || "",
      status: row.status || "",
      category: row.category || "",
    })),
    voyages: database.prepare(`
      SELECT id, route, departure, weather, status
      FROM voyages
      WHERE vessel_id = ?
      ORDER BY sort_order ASC, id ASC
    `).all(vesselId).map((row) => ({
      id: row.id,
      route: row.route,
      departure: row.departure,
      weather: row.weather || "",
      status: row.status || "",
    })),
  };
}

function loadMachineryCollectionForVessel(vesselId, tableName) {
  return database.prepare(`
    SELECT id, label, manufacturer, model, rating, hours, last_service_hours, service_interval_hours, last_service_date, next_service_date, notes
    FROM ${tableName}
    WHERE vessel_id = ?
    ORDER BY sort_order ASC, id ASC
  `).all(vesselId).map((row) => ({
    id: row.id,
    label: row.label,
    manufacturer: row.manufacturer || "",
    model: row.model || "",
    rating: row.rating || "",
    hours: row.hours || 0,
    lastServiceHours: row.last_service_hours || 0,
    serviceIntervalHours: row.service_interval_hours || 0,
    lastServiceDate: row.last_service_date || "",
    nextServiceDate: row.next_service_date || "",
    notes: row.notes || "",
  }));
}

function buildMachinerySummary(items, fallback = "") {
  if (!Array.isArray(items) || !items.length) {
    return normalizeTextValue(fallback, "");
  }

  return items
    .map((item) => [item.label, item.manufacturer, item.model, item.rating].filter(Boolean).join(" | "))
    .filter(Boolean)
    .join(" | ");
}

function getStoredState(sessionUser = null) {
  if (!hasStructuredState()) {
    return null;
  }

  const allowedVesselIds = sessionUser && !hasFullFleetAccess(sessionUser)
    ? new Set(getUserAccessibleVesselIds(sessionUser).map((value) => String(value)))
    : null;
  const vesselRows = database.prepare(
`
    SELECT
      v.id,
      v.name,
      v.builder,
      v.model,
      v.year_built,
      v.vessel_type,
      v.hull_material,
      v.length,
      v.beam,
      v.draft,
      v.guests,
      v.status,
      v.berth,
      v.captain,
      v.location,
      v.fuel,
      v.fuel_capacity,
      v.water_tank,
      v.water_capacity,
      v.grey_tank,
      v.grey_water_capacity,
      v.black_tank_level,
      v.black_water_capacity,
      v.battery_status,
      v.utilization,
      v.next_service,
      v.engine_info,
      v.generator_info,
      v.photo_data_url,
      v.notes,
      v.updated_at,
      v.display_order,
      uv.manufacturer_id,
      uv.model_id,
      uv.model_spec_id,
      uv.model_year,
      uv.is_custom
    FROM vessels v
    LEFT JOIN user_vessels uv ON uv.vessel_id = v.id
    ORDER BY v.display_order ASC, v.id ASC
  `).all().filter((row) => !allowedVesselIds || allowedVesselIds.has(String(row.id)));

  if (!vesselRows.length) {
    return null;
  }

  const preferences = Object.fromEntries(
    database.prepare("SELECT key, value FROM app_preferences").all().map((row) => [row.key, row.value])
  );
  const vessels = vesselRows.map((row) => {
    const engines = loadMachineryCollectionForVessel(row.id, "engines");
    const generators = loadMachineryCollectionForVessel(row.id, "generators");

    return {
      id: String(row.id),
      name: row.name,
      builder: row.builder || "",
      model: row.model,
      yearBuilt: row.year_built || row.model_year || 0,
      vesselType: row.vessel_type || "",
      hullMaterial: row.hull_material || "",
      length: row.length,
      beam: row.beam,
      draft: row.draft,
      guests: row.guests,
      status: row.status,
      berth: row.berth,
      captain: row.captain,
      location: row.location,
      fuel: row.fuel,
      fuelCapacity: row.fuel_capacity,
      waterTank: row.water_tank,
      waterCapacity: row.water_capacity,
      greyTank: row.grey_tank,
      greyWaterCapacity: row.grey_water_capacity,
      blackTankLevel: row.black_tank_level,
      blackWaterCapacity: row.black_water_capacity,
      batteryStatus: row.battery_status,
      utilization: row.utilization,
      nextService: row.next_service || "",
      engineInfo: buildMachinerySummary(engines, row.engine_info || ""),
      generatorInfo: buildMachinerySummary(generators, row.generator_info || ""),
      engines,
      generators,
      catalogManufacturerId: row.manufacturer_id ? String(row.manufacturer_id) : "",
      catalogModelId: row.model_id ? String(row.model_id) : "",
      catalogSpecId: row.model_spec_id ? String(row.model_spec_id) : "",
      isCustom: Number(row.is_custom ?? 1) !== 0,
      photoDataUrl: row.photo_data_url || "",
      notes: row.notes || "",
    };
  });
  const vesselBundles = Object.fromEntries(
    vesselRows.map((row) => [String(row.id), loadStructuredBundleForVessel(row.id)])
  );
  const activeVesselId = vessels.some((vessel) => vessel.id === String(preferences.activeVesselId))
    ? String(preferences.activeVesselId)
    : vessels[0].id;
  const activeBundle = vesselBundles[activeVesselId] || loadStructuredBundleForVessel(Number(activeVesselId));
  const activeVessel = vessels.find((vessel) => vessel.id === activeVesselId) || vessels[0];

  const state = {
    vessels,
    activeVesselId,
    vesselBundles,
    vessel: activeVessel,
    maintenanceAssets: activeBundle.maintenanceAssets,
    maintenance: activeBundle.maintenance,
    maintenanceHistory: activeBundle.maintenanceHistory,
    workOrders: activeBundle.workOrders,
    inventory: activeBundle.inventory,
    expenses: activeBundle.expenses,
    charters: activeBundle.charters,
    crew: activeBundle.crew,
    reports: activeBundle.reports,
    vendors: activeBundle.vendors,
    voyages: activeBundle.voyages,
    activeView: preferences.activeView || "overview",
    activeWorkWeekStart: preferences.activeWorkWeekStart || getWorkWeekRange(nowIso()).start,
    activeMaintenanceFilter: preferences.activeMaintenanceFilter || "all",
    activeMaintenanceSort: preferences.activeMaintenanceSort || "category",
    activeMaintenanceCategory: preferences.activeMaintenanceCategory || "all",
    activeMaintenanceQuery: preferences.activeMaintenanceQuery || "",
    activeReportId: preferences.activeReportId || activeBundle.reports[0]?.id || "",
    activeWorkOrderSort: preferences.activeWorkOrderSort || "date-asc",
    activeVendorFilter: preferences.activeVendorFilter || "all",
    activeVendorSort: preferences.activeVendorSort || "name-asc",
    activeInventorySort: preferences.activeInventorySort || "name-asc",
    activeExpenseSort: preferences.activeExpenseSort || "date-desc",
    activeCharterSort: preferences.activeCharterSort || "start-asc",
    activeVoyageSort: preferences.activeVoyageSort || "departure-asc",
  };

  const latestUpdatedAt = vesselRows
    .map((row) => row.updated_at)
    .filter(Boolean)
    .sort()
    .at(-1) || nowIso();

  return {
    state,
    updatedAt: latestUpdatedAt,
  };
}

function findVesselById(vesselId) {
  return database.prepare(`
    SELECT id, name, builder, model, year_built, vessel_type, hull_material, status, captain, location, berth,
           length, beam, draft, fuel, fuel_capacity, water_tank, water_capacity,
           grey_tank, grey_water_capacity, black_tank_level, black_water_capacity,
           battery_status, utilization, next_service, photo_data_url
    FROM vessels
    WHERE id = ?
  `).get(vesselId) || null;
}

function getWeeklyReportEntryReportId(entryId) {
  return normalizeTextValue((database.prepare("SELECT report_id FROM weekly_report_entries WHERE id = ?").get(entryId) || {}).report_id, "");
}

function getWeeklyReportRecord(reportId) {
  const report = database.prepare(`
    SELECT id, vessel_id, week_start, week_end, status, vessel_snapshot_json, sort_order, created_at, updated_at
    FROM weekly_reports
    WHERE id = ?
  `).get(reportId);

  if (!report) {
    return null;
  }

  const entries = database.prepare(`
    SELECT id, item, report_date, work_done, systems_checked, issues, notes, sort_order, created_at, updated_at
    FROM weekly_report_entries
    WHERE report_id = ?
    ORDER BY report_date ASC, sort_order ASC, id ASC
  `).all(reportId).map((entry, index) => ({
    id: entry.id,
    item: entry.item,
    reportDate: entry.report_date,
    workDone: entry.work_done || "",
    systemsChecked: entry.systems_checked || "",
    issues: entry.issues || "",
    notes: entry.notes || "",
    sortOrder: index,
    createdAt: entry.created_at,
    updatedAt: entry.updated_at,
  }));

  return {
    id: report.id,
    vesselId: String(report.vessel_id),
    weekStart: report.week_start,
    weekEnd: report.week_end,
    status: report.status,
    vesselSnapshot: parseWeeklyReportVesselSnapshot(report.vessel_snapshot_json, null, report.updated_at || report.created_at || nowIso()),
    sortOrder: report.sort_order,
    createdAt: report.created_at,
    updatedAt: report.updated_at,
    entries,
  };
}

function assertDateInWorkWeek(dateValue, weekStart, weekEnd) {
  const normalizedDate = formatDateOnly(dateValue);
  if (
    compareDateOnlyStrings(normalizedDate, weekStart) < 0
    || compareDateOnlyStrings(normalizedDate, weekEnd) > 0
  ) {
    throw new Error("Entry date must stay within the selected Monday through Friday report week.");
  }
}

function createWeeklyReport(payload) {
  const normalized = validateWeeklyReportPayload(payload, { requireVesselId: true });
  const vessel = findVesselById(normalized.vesselId);

  if (!vessel) {
    throw new Error("Vessel not found.");
  }

  const existing = database.prepare(`
    SELECT id
    FROM weekly_reports
    WHERE vessel_id = ? AND week_start = ? AND week_end = ?
  `).get(normalized.vesselId, normalized.weekStart, normalized.weekEnd);

  if (existing?.id) {
    return getWeeklyReportRecord(existing.id);
  }

  const reportId = randomUUID();
  const timestamp = nowIso();
  const reportCount = Number(database.prepare("SELECT COUNT(*) AS total FROM weekly_reports WHERE vessel_id = ?").get(normalized.vesselId)?.total || 0);
  const vesselSnapshot = JSON.stringify(
    parseWeeklyReportVesselSnapshot(payload?.vesselSnapshot, vessel, timestamp)
  );

  database.prepare(`
    INSERT INTO weekly_reports (
      id, vessel_id, week_start, week_end, status, vessel_snapshot_json, sort_order, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    reportId,
    normalized.vesselId,
    normalized.weekStart,
    normalized.weekEnd,
    normalized.status,
    vesselSnapshot,
    reportCount,
    timestamp,
    timestamp
  );

  return getWeeklyReportRecord(reportId);
}

function replaceWeeklyReportEntriesFromWorkspace(reportId, entries, options = {}) {
  const report = getWeeklyReportRecord(reportId);
  if (!report) {
    throw new Error("Weekly report not found.");
  }

  const timestamp = nowIso();
  const nextSnapshot = JSON.stringify(
    parseWeeklyReportVesselSnapshot(options?.vesselSnapshot, null, timestamp)
  );
  const insertWeeklyReportEntryStatement = database.prepare(`
    INSERT INTO weekly_report_entries (
      id, report_id, item, report_date, work_done, systems_checked, issues, notes, source_work_order_id, sort_order, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  runInTransaction(() => {
    database.prepare("DELETE FROM weekly_report_entries WHERE report_id = ?").run(reportId);
    entries.forEach((entry, index) => {
      insertWeeklyReportEntryStatement.run(
        randomUUID(),
        reportId,
        normalizeTextValue(entry.item, `Weekly entry ${index + 1}`),
        formatDateOnly(entry.reportDate || report.weekStart),
        normalizeTextValue(entry.workDone, ""),
        normalizeTextValue(entry.systemsChecked, ""),
        normalizeTextValue(entry.issues, ""),
        normalizeTextValue(entry.notes, ""),
        normalizeTextValue(entry.sourceWorkOrderId ?? entry.id, ""),
        index,
        normalizeTextValue(entry.createdAt, timestamp),
        timestamp
      );
    });
    database.prepare("UPDATE weekly_reports SET vessel_snapshot_json = ?, updated_at = ? WHERE id = ?").run(
      nextSnapshot,
      timestamp,
      reportId
    );
  });

  return getWeeklyReportRecord(reportId);
}

function generateWeeklyReportFromWorkspace(payload) {
  const normalized = validateWeeklyReportPayload(payload, { requireVesselId: true });
  const vessel = findVesselById(normalized.vesselId);
  if (!vessel) {
    throw new Error("Vessel not found.");
  }

  const workspaceEntries = filterWorkOrdersForWeeklyReport(
    loadWorkOrdersForWeek(normalized.vesselId, normalized.weekStart, normalized.weekEnd)
  );
  if (!workspaceEntries.length) {
    throw new Error("No completed or logged weekly entries were found for this vessel and work week.");
  }

  const existingReport = database.prepare(`
    SELECT id
    FROM weekly_reports
    WHERE vessel_id = ? AND week_start = ? AND week_end = ?
  `).get(normalized.vesselId, normalized.weekStart, normalized.weekEnd);

  if (existingReport?.id) {
    const currentReport = getWeeklyReportRecord(existingReport.id);
    if (!currentReport) {
      throw new Error("Weekly report not found.");
    }

    if (currentReport.status === "finalized") {
      throw new Error("This weekly report is finalized. Reopen it in Reports before generating a new snapshot.");
    }

    return replaceWeeklyReportEntriesFromWorkspace(
      currentReport.id,
      workspaceEntries,
      { vesselSnapshot: buildWeeklyReportVesselSnapshot(vessel, nowIso()) }
    );
  }

  const report = createWeeklyReport({
    vesselId: normalized.vesselId,
    weekStart: normalized.weekStart,
    status: "draft",
    vesselSnapshot: buildWeeklyReportVesselSnapshot(vessel, nowIso()),
  });
  return replaceWeeklyReportEntriesFromWorkspace(
    report.id,
    workspaceEntries,
    { vesselSnapshot: buildWeeklyReportVesselSnapshot(vessel, nowIso()) }
  );
}

function updateWeeklyReport(reportId, payload) {
  const existing = getWeeklyReportRecord(reportId);
  if (!existing) {
    throw new Error("Weekly report not found.");
  }

  const nextStatus = payload?.status ? normalizeWeeklyReportStatus(payload.status) : existing.status;
  const timestamp = nowIso();

  if (nextStatus === "finalized" && existing.status !== "finalized") {
    const vessel = findVesselById(existing.vesselId);
    if (vessel) {
      const workspaceEntries = filterWorkOrdersForWeeklyReport(
        loadWorkOrdersForWeek(existing.vesselId, existing.weekStart, existing.weekEnd)
      );
      if (workspaceEntries.length) {
        replaceWeeklyReportEntriesFromWorkspace(existing.id, workspaceEntries, {
          vesselSnapshot: buildWeeklyReportVesselSnapshot(vessel, timestamp),
        });
      }
    }
  }

  database.prepare(`
    UPDATE weekly_reports
    SET status = ?, updated_at = ?
    WHERE id = ?
  `).run(nextStatus, timestamp, reportId);

  return getWeeklyReportRecord(reportId);
}

function createWeeklyReportEntry(reportId, payload) {
  const report = getWeeklyReportRecord(reportId);
  if (!report) {
    throw new Error("Weekly report not found.");
  }

  if (report.status === "finalized") {
    throw new Error("Finalize status is locked. Reopen the report before editing entries.");
  }

  const entry = validateWeeklyReportEntryPayload(payload);
  assertDateInWorkWeek(entry.reportDate, report.weekStart, report.weekEnd);
  const entryId = randomUUID();
  const timestamp = nowIso();

  database.prepare(`
    INSERT INTO weekly_report_entries (
      id, report_id, item, report_date, work_done, systems_checked, issues, notes, source_work_order_id, sort_order, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    entryId,
    reportId,
    entry.item,
    entry.reportDate,
    entry.workDone,
    entry.systemsChecked,
    entry.issues,
    entry.notes,
    entry.sourceWorkOrderId,
    report.entries.length,
    timestamp,
    timestamp
  );

  database.prepare("UPDATE weekly_reports SET updated_at = ? WHERE id = ?").run(timestamp, reportId);
  return getWeeklyReportRecord(reportId);
}

function updateWeeklyReportEntry(entryId, payload) {
  const existing = database.prepare(`
    SELECT e.id, e.report_id
    FROM weekly_report_entries e
    WHERE e.id = ?
  `).get(entryId);

  if (!existing) {
    throw new Error("Report entry not found.");
  }

  const report = getWeeklyReportRecord(existing.report_id);
  if (!report) {
    throw new Error("Weekly report not found.");
  }

  if (report.status === "finalized") {
    throw new Error("Finalize status is locked. Reopen the report before editing entries.");
  }

  const entry = validateWeeklyReportEntryPayload(payload);
  assertDateInWorkWeek(entry.reportDate, report.weekStart, report.weekEnd);
  const timestamp = nowIso();

  database.prepare(`
    UPDATE weekly_report_entries
    SET item = ?, report_date = ?, work_done = ?, systems_checked = ?, issues = ?, notes = ?, source_work_order_id = ?, updated_at = ?
    WHERE id = ?
  `).run(
    entry.item,
    entry.reportDate,
    entry.workDone,
    entry.systemsChecked,
    entry.issues,
    entry.notes,
    entry.sourceWorkOrderId,
    timestamp,
    entryId
  );

  database.prepare("UPDATE weekly_reports SET updated_at = ? WHERE id = ?").run(timestamp, report.id);
  return getWeeklyReportRecord(report.id);
}

function deleteWeeklyReportEntry(entryId) {
  const existing = database.prepare(`
    SELECT e.id, e.report_id
    FROM weekly_report_entries e
    WHERE e.id = ?
  `).get(entryId);

  if (!existing) {
    throw new Error("Report entry not found.");
  }

  const report = getWeeklyReportRecord(existing.report_id);
  if (!report) {
    throw new Error("Weekly report not found.");
  }

  if (report.status === "finalized") {
    throw new Error("Finalize status is locked. Reopen the report before editing entries.");
  }

  runInTransaction(() => {
    database.prepare("DELETE FROM weekly_report_entries WHERE id = ?").run(entryId);
    database.prepare("UPDATE weekly_reports SET updated_at = ? WHERE id = ?").run(nowIso(), report.id);
  });

  return getWeeklyReportRecord(report.id);
}

function saveStructuredState(rawState, options = {}) {
  const persistedAt = options.updatedAt || nowIso();
  const grantAccessClientIds = new Set(Array.isArray(options.grantAccessClientIds) ? options.grantAccessClientIds.map((value) => String(value)) : []);
  const state = normalizeStateForStructuredStorage(rawState);
  const workOrderPreservationIndex = createWorkOrderPreservationIndex();
  const existingMaintenanceHistory = loadMaintenanceHistoryRecords();
  const fallbackVessel = state?.vessel && typeof state.vessel === "object" ? state.vessel : {};
  const vesselsSource = Array.isArray(state.vessels) && state.vessels.length
    ? state.vessels
    : [fallbackVessel];
  const fallbackBundle = {
    maintenanceAssets: normalizeCollectionItems(state.maintenanceAssets),
    maintenance: normalizeCollectionItems(state.maintenance),
    maintenanceHistory: normalizeCollectionItems(state.maintenanceHistory),
    workOrders: normalizeCollectionItems(state.workOrders),
    inventory: normalizeCollectionItems(state.inventory),
    expenses: normalizeCollectionItems(state.expenses),
    charters: normalizeCollectionItems(state.charters),
    crew: normalizeCollectionItems(state.crew),
    reports: normalizeWeeklyReportsCollection(state.reports, persistedAt),
    vendors: normalizeCollectionItems(state.vendors),
    voyages: normalizeCollectionItems(state.voyages),
  };
  const incomingBundles = state?.vesselBundles && typeof state.vesselBundles === "object"
    ? state.vesselBundles
    : {};
  const usedClientIds = new Set();
  const normalizedVessels = vesselsSource.map((vessel, index) => {
    let clientId = normalizeTextValue(vessel?.id, "").trim() || `vessel-${index + 1}`;
    while (usedClientIds.has(clientId)) {
      clientId = `${clientId}-${index + 1}`;
    }
    usedClientIds.add(clientId);

    return {
      clientId,
      name: normalizeTextValue(vessel?.name, `Vessel ${index + 1}`),
      builder: normalizeTextValue(vessel?.builder, ""),
      model: normalizeTextValue(vessel?.model, ""),
      yearBuilt: normalizeIntegerValue(vessel?.yearBuilt ?? vessel?.year_built, 0),
      vesselType: normalizeTextValue(vessel?.vesselType ?? vessel?.vessel_type, ""),
      hullMaterial: normalizeTextValue(vessel?.hullMaterial ?? vessel?.hull_material, ""),
      catalogManufacturerId: normalizeIntegerValue(vessel?.catalogManufacturerId ?? vessel?.manufacturerId ?? vessel?.manufacturer_id, 0),
      catalogModelId: normalizeIntegerValue(vessel?.catalogModelId ?? vessel?.modelId ?? vessel?.model_id, 0),
      catalogSpecId: normalizeIntegerValue(vessel?.catalogSpecId ?? vessel?.modelSpecId ?? vessel?.model_spec_id, 0),
      isCustom: vessel?.isCustom === false ? false : normalizeIntegerValue(vessel?.catalogSpecId ?? vessel?.modelSpecId ?? vessel?.model_spec_id, 0) <= 0,
      length: normalizeIntegerValue(vessel?.length, 0),
      beam: normalizeNumberValue(vessel?.beam, 0),
      draft: normalizeNumberValue(vessel?.draft, 0),
      guests: normalizeIntegerValue(vessel?.guests, 0),
      status: normalizeTextValue(vessel?.status, "Ready"),
      berth: normalizeTextValue(vessel?.berth, ""),
      captain: normalizeTextValue(vessel?.captain, ""),
      location: normalizeTextValue(vessel?.location, ""),
      fuel: normalizeIntegerValue(vessel?.fuel, 0),
      fuelCapacity: normalizeIntegerValue(vessel?.fuelCapacity, 0),
      waterTank: normalizeIntegerValue(vessel?.waterTank, 0),
      waterCapacity: normalizeIntegerValue(vessel?.waterCapacity, 0),
      greyTank: normalizeIntegerValue(vessel?.greyTank, 0),
      greyWaterCapacity: normalizeIntegerValue(vessel?.greyWaterCapacity, 0),
      blackTankLevel: normalizeIntegerValue(vessel?.blackTankLevel, 0),
      blackWaterCapacity: normalizeIntegerValue(vessel?.blackWaterCapacity, 0),
      batteryStatus: normalizeIntegerValue(vessel?.batteryStatus, 0),
      utilization: normalizeIntegerValue(vessel?.utilization, 0),
      nextService: normalizeNullableTextValue(vessel?.nextService),
      engines: Array.isArray(vessel?.engines) ? vessel.engines : [],
      generators: Array.isArray(vessel?.generators) ? vessel.generators : [],
      engineInfo: buildMachinerySummary(
        Array.isArray(vessel?.engines) ? vessel.engines : [],
        normalizeTextValue(vessel?.engineInfo, "")
      ),
      generatorInfo: buildMachinerySummary(
        Array.isArray(vessel?.generators) ? vessel.generators : [],
        normalizeTextValue(vessel?.generatorInfo, "")
      ),
      photoDataUrl: normalizeTextValue(vessel?.photoDataUrl, ""),
      notes: normalizeTextValue(vessel?.notes, ""),
      displayOrder: index,
    };
  });
  const activeClientVesselId = normalizeTextValue(
    state.activeVesselId,
    normalizedVessels[0]?.clientId || ""
  ).trim();
  const vesselBundleLookup = new Map();

  normalizedVessels.forEach((vessel, index) => {
    const bundleSource = incomingBundles[vessel.clientId]
      || incomingBundles[String(vessel.clientId)]
      || (index === 0 ? fallbackBundle : {});
    vesselBundleLookup.set(vessel.clientId, {
      maintenanceAssets: normalizeCollectionItems(bundleSource.maintenanceAssets),
      maintenance: normalizeCollectionItems(bundleSource.maintenance),
      maintenanceHistory: normalizeCollectionItems(bundleSource.maintenanceHistory),
      workOrders: normalizeCollectionItems(bundleSource.workOrders),
      inventory: normalizeCollectionItems(bundleSource.inventory),
      expenses: normalizeCollectionItems(bundleSource.expenses),
      charters: normalizeCollectionItems(bundleSource.charters),
      crew: normalizeCollectionItems(bundleSource.crew),
      reports: normalizeWeeklyReportsCollection(bundleSource.reports, persistedAt),
      vendors: normalizeCollectionItems(bundleSource.vendors),
      voyages: normalizeCollectionItems(bundleSource.voyages),
    });
  });

  const basePreferences = [
    ["activeView", normalizeTextValue(state.activeView, "overview")],
    ["activeWorkWeekStart", normalizeTextValue(state.activeWorkWeekStart, getWorkWeekRange(nowIso()).start)],
    ["activeMaintenanceFilter", normalizeTextValue(state.activeMaintenanceFilter, "all")],
    ["activeMaintenanceSort", normalizeTextValue(state.activeMaintenanceSort, "category")],
    ["activeMaintenanceCategory", normalizeTextValue(state.activeMaintenanceCategory, "all")],
    ["activeMaintenanceQuery", normalizeTextValue(state.activeMaintenanceQuery, "")],
    ["activeReportId", normalizeTextValue(state.activeReportId, "")],
    ["activeWorkOrderSort", normalizeTextValue(state.activeWorkOrderSort, "date-asc")],
    ["activeVendorFilter", normalizeTextValue(state.activeVendorFilter, "all")],
    ["activeVendorSort", normalizeTextValue(state.activeVendorSort, "name-asc")],
    ["activeInventorySort", normalizeTextValue(state.activeInventorySort, "name-asc")],
    ["activeExpenseSort", normalizeTextValue(state.activeExpenseSort, "date-desc")],
    ["activeCharterSort", normalizeTextValue(state.activeCharterSort, "start-asc")],
    ["activeVoyageSort", normalizeTextValue(state.activeVoyageSort, "departure-asc")],
  ];
  const activeWorkWeekStart = normalizeTextValue(state.activeWorkWeekStart, getWorkWeekRange(nowIso()).start);
  const generatedMaintenanceHistoryEntries = [];

  runInTransaction(() => {
    database.exec(`
      DELETE FROM maintenance_history;
      DELETE FROM maintenance_logs;
      DELETE FROM maintenance_assets;
      DELETE FROM inventory;
      DELETE FROM expenses;
      DELETE FROM work_orders;
      DELETE FROM charters;
      DELETE FROM crew_members;
      DELETE FROM weekly_report_entries;
      DELETE FROM weekly_reports;
      DELETE FROM vendors;
      DELETE FROM voyages;
      DELETE FROM engines;
      DELETE FROM generators;
      DELETE FROM user_vessels;
      DELETE FROM app_preferences;
      DELETE FROM vessels;
    `);

    const insertVesselStatement = database.prepare(`
      INSERT INTO vessels (
        name,
        builder,
        model,
        year_built,
        vessel_type,
        hull_material,
        length,
        beam,
        draft,
        guests,
        status,
        berth,
        captain,
        location,
        fuel,
        fuel_capacity,
        water_tank,
        water_capacity,
        grey_tank,
        grey_water_capacity,
        black_tank_level,
        black_water_capacity,
        battery_status,
        utilization,
        next_service,
        engine_info,
        generator_info,
        photo_data_url,
        notes,
        display_order,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertVesselWithIdStatement = database.prepare(`
      INSERT INTO vessels (
        id,
        name,
        builder,
        model,
        year_built,
        vessel_type,
        hull_material,
        length,
        beam,
        draft,
        guests,
        status,
        berth,
        captain,
        location,
        fuel,
        fuel_capacity,
        water_tank,
        water_capacity,
        grey_tank,
        grey_water_capacity,
        black_tank_level,
        black_water_capacity,
        battery_status,
        utilization,
        next_service,
        engine_info,
        generator_info,
        photo_data_url,
        notes,
        display_order,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertUserVesselStatement = database.prepare(`
      INSERT INTO user_vessels (
        vessel_id, manufacturer_id, model_id, model_spec_id, model_year, vessel_type, hull_material, is_custom, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const vesselIdLookup = new Map();
    normalizedVessels.forEach((vessel) => {
      const existingDbId = /^\d+$/.test(vessel.clientId) ? Number.parseInt(vessel.clientId, 10) : null;
      const params = [
        vessel.name,
        vessel.builder,
        vessel.model,
        vessel.yearBuilt,
        vessel.vesselType,
        vessel.hullMaterial,
        vessel.length,
        vessel.beam,
        vessel.draft,
        vessel.guests,
        vessel.status,
        vessel.berth,
        vessel.captain,
        vessel.location,
        vessel.fuel,
        vessel.fuelCapacity,
        vessel.waterTank,
        vessel.waterCapacity,
        vessel.greyTank,
        vessel.greyWaterCapacity,
        vessel.blackTankLevel,
        vessel.blackWaterCapacity,
        vessel.batteryStatus,
        vessel.utilization,
        vessel.nextService,
        vessel.engineInfo,
        vessel.generatorInfo,
        vessel.photoDataUrl,
        vessel.notes,
        vessel.displayOrder,
        persistedAt,
        persistedAt,
      ];
      const vesselResult = existingDbId
        ? insertVesselWithIdStatement.run(existingDbId, ...params)
        : insertVesselStatement.run(...params);
      const vesselId = existingDbId || Number(vesselResult.lastInsertRowid);
      vesselIdLookup.set(vessel.clientId, vesselId);
      insertUserVesselStatement.run(
        vesselId,
        vessel.catalogManufacturerId > 0 ? vessel.catalogManufacturerId : null,
        vessel.catalogModelId > 0 ? vessel.catalogModelId : null,
        vessel.catalogSpecId > 0 ? vessel.catalogSpecId : null,
        vessel.yearBuilt,
        vessel.vesselType,
        vessel.hullMaterial,
        vessel.isCustom ? 1 : 0,
        persistedAt,
        persistedAt
      );
    });

    const insertMaintenanceStatement = database.prepare(`
      INSERT INTO maintenance_logs (
        id, vessel_id, title, category, status, priority, asset_id, template_id, template_task_id,
        due_date, due_hours, last_completed, last_completed_hours, interval_days, interval_hours,
        reminder_days, reminder_hours, recurrence_mode, meter_source_type, meter_source_id, is_custom,
        notes, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertMaintenanceAssetStatement = database.prepare(`
      INSERT INTO maintenance_assets (
        id, vessel_id, template_id, name, asset_type, manufacturer, model, serial_number, location,
        meter_source_type, meter_source_id, current_hours, notes, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertMaintenanceHistoryStatement = database.prepare(`
      INSERT INTO maintenance_history (
        id, vessel_id, maintenance_log_id, asset_id, template_task_id, work_order_id, source, completed_at, completion_date,
        completed_hours, work_done, systems_checked, issues, notes, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertInventoryStatement = database.prepare(`
      INSERT INTO inventory (
        id, vessel_id, name, location, quantity, unit, minimum_quantity, status, notes, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertExpenseStatement = database.prepare(`
      INSERT INTO expenses (
        id, vessel_id, title, vendor, category, amount, currency, expense_date, status, notes, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertWorkOrderStatement = database.prepare(`
      INSERT INTO work_orders (
        id, vessel_id, title, priority, status, due_date, notes, sort_order, created_at, updated_at,
        week_start, week_end, entry_date, work_done, systems_checked, issues, maintenance_log_id, origin_type, completed_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertCharterStatement = database.prepare(`
      INSERT INTO charters (
        id, vessel_id, client, start_date, end_date, berth, status, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertCrewStatement = database.prepare(`
      INSERT INTO crew_members (
        id, vessel_id, name, role, certification, rotation, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertWeeklyReportStatement = database.prepare(`
      INSERT INTO weekly_reports (
        id, vessel_id, week_start, week_end, status, vessel_snapshot_json, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertWeeklyReportEntryStatement = database.prepare(`
      INSERT INTO weekly_report_entries (
        id, report_id, item, report_date, work_done, systems_checked, issues, notes, source_work_order_id, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertVendorStatement = database.prepare(`
      INSERT INTO vendors (
        id, vessel_id, name, contact, email, phone, status, category, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const insertVoyageStatement = database.prepare(`
      INSERT INTO voyages (
        id, vessel_id, route, departure, weather, status, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertEngineStatement = database.prepare(`
      INSERT INTO engines (
        id, vessel_id, label, manufacturer, model, rating, hours, last_service_hours, service_interval_hours,
        last_service_date, next_service_date, notes, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertGeneratorStatement = database.prepare(`
      INSERT INTO generators (
        id, vessel_id, label, manufacturer, model, rating, hours, last_service_hours, service_interval_hours,
        last_service_date, next_service_date, notes, sort_order, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    normalizedVessels.forEach((vessel) => {
      const vesselId = vesselIdLookup.get(vessel.clientId);
      const bundle = vesselBundleLookup.get(vessel.clientId) || fallbackBundle;
      const automatedBundle = applyConnectedAutomationToBundle(bundle, vesselId, {
        persistedAt,
        activeWorkWeekStart,
        engines: vessel.engines,
        generators: vessel.generators,
      });
      generatedMaintenanceHistoryEntries.push(...automatedBundle.historyEntries);

      vessel.engines.forEach((item, index) => {
        insertEngineStatement.run(
          normalizeRowId(item?.id, "engine", index),
          vesselId,
          normalizeTextValue(item?.label, `Engine ${index + 1}`),
          normalizeTextValue(item?.manufacturer, ""),
          normalizeTextValue(item?.model, ""),
          normalizeTextValue(item?.rating, ""),
          normalizeIntegerValue(item?.hours, 0),
          normalizeIntegerValue(item?.lastServiceHours, 0),
          normalizeIntegerValue(item?.serviceIntervalHours, 0),
          normalizeNullableTextValue(item?.lastServiceDate),
          normalizeNullableTextValue(item?.nextServiceDate),
          normalizeTextValue(item?.notes, ""),
          index,
          persistedAt,
          persistedAt
        );
      });

      vessel.generators.forEach((item, index) => {
        insertGeneratorStatement.run(
          normalizeRowId(item?.id, "generator", index),
          vesselId,
          normalizeTextValue(item?.label, `Generator ${index + 1}`),
          normalizeTextValue(item?.manufacturer, ""),
          normalizeTextValue(item?.model, ""),
          normalizeTextValue(item?.rating, ""),
          normalizeIntegerValue(item?.hours, 0),
          normalizeIntegerValue(item?.lastServiceHours, 0),
          normalizeIntegerValue(item?.serviceIntervalHours, 0),
          normalizeNullableTextValue(item?.lastServiceDate),
          normalizeNullableTextValue(item?.nextServiceDate),
          normalizeTextValue(item?.notes, ""),
          index,
          persistedAt,
          persistedAt
        );
      });

      automatedBundle.maintenanceAssets.forEach((item, index) => {
        insertMaintenanceAssetStatement.run(
          normalizeRowId(item?.id, "maintenance-asset", index),
          vesselId,
          normalizeNullableTextValue(item?.templateId),
          normalizeTextValue(item?.name, `Installed asset ${index + 1}`),
          normalizeTextValue(item?.assetType, ""),
          normalizeTextValue(item?.manufacturer, ""),
          normalizeTextValue(item?.model, ""),
          normalizeTextValue(item?.serialNumber, ""),
          normalizeTextValue(item?.location, ""),
          normalizeTextValue(item?.meterSourceType, "none"),
          normalizeNullableTextValue(item?.meterSourceId),
          normalizeIntegerValue(item?.currentHours, 0),
          normalizeTextValue(item?.notes, ""),
          index,
          normalizeTextValue(item?.createdAt, persistedAt),
          normalizeTextValue(item?.updatedAt, persistedAt)
        );
      });

      automatedBundle.maintenance.forEach((item, index) => {
        insertMaintenanceStatement.run(
          normalizeRowId(item?.id, "maintenance", index),
          vesselId,
          normalizeTextValue(item?.title, `Maintenance ${index + 1}`),
          normalizeTextValue(item?.category, "General"),
          normalizeTextValue(item?.status, "Not Started"),
          normalizeTextValue(item?.priority, "Medium"),
          normalizeNullableTextValue(item?.assetId),
          normalizeNullableTextValue(item?.templateId),
          normalizeNullableTextValue(item?.templateTaskId),
          normalizeNullableTextValue(item?.dueDate),
          normalizeIntegerValue(item?.dueHours, 0),
          normalizeNullableTextValue(item?.lastCompleted),
          normalizeIntegerValue(item?.lastCompletedHours, 0),
          normalizeIntegerValue(item?.intervalDays, 0),
          normalizeIntegerValue(item?.intervalHours, 0),
          normalizeIntegerValue(item?.reminderDays, 0),
          normalizeIntegerValue(item?.reminderHours, 0),
          normalizeMaintenanceRecurrenceMode(item?.recurrenceMode),
          normalizeTextValue(item?.meterSourceType, "none"),
          normalizeNullableTextValue(item?.meterSourceId),
          normalizeIntegerValue(item?.isCustom, 1),
          normalizeTextValue(item?.notes, ""),
          index,
          normalizeTextValue(item?.createdAt, persistedAt),
          normalizeTextValue(item?.updatedAt, persistedAt)
        );
      });

      bundle.inventory.forEach((item, index) => {
        insertInventoryStatement.run(
          normalizeRowId(item?.id, "inventory", index),
          vesselId,
          normalizeTextValue(item?.name, `Inventory item ${index + 1}`),
          normalizeTextValue(item?.location, ""),
          normalizeNumberValue(item?.quantity, 0),
          normalizeTextValue(item?.unit, ""),
          normalizeNumberValue(item?.minimumQuantity, 0),
          normalizeTextValue(item?.status, ""),
          normalizeTextValue(item?.notes, ""),
          index,
          persistedAt,
          persistedAt
        );
      });

      bundle.expenses.forEach((item, index) => {
        insertExpenseStatement.run(
          normalizeRowId(item?.id, "expense", index),
          vesselId,
          normalizeTextValue(item?.title, `Expense ${index + 1}`),
          normalizeTextValue(item?.vendor, ""),
          normalizeTextValue(item?.category, ""),
          normalizeNumberValue(item?.amount, 0),
          normalizeTextValue(item?.currency, "USD"),
          normalizeNullableTextValue(item?.expenseDate),
          normalizeTextValue(item?.status, ""),
          normalizeTextValue(item?.notes, ""),
          index,
          persistedAt,
          persistedAt
        );
      });

      automatedBundle.workOrders.forEach((item, index) => {
        const reportDate = formatDateOnly(item?.reportDate || item?.entryDate || item?.dueDate || nowIso());
        const weekRange = getWorkWeekRange(item?.weekStart || item?.weekEnd || reportDate);
        const workOrderCreatedAt = normalizeTextValue(item?.createdAt, persistedAt);
        const workOrderUpdatedAt = normalizeTextValue(item?.updatedAt, persistedAt);
        const mergedFields = mergeWorkOrderFieldSets(
          normalizeWorkOrderFieldSet(item),
          resolvePreservedWorkOrderFields(workOrderPreservationIndex, {
            id: item?.id,
            vesselId,
            weekStart: weekRange.start,
            weekEnd: weekRange.end,
            reportDate,
            title: item?.item || item?.title,
          })
        );
        insertWorkOrderStatement.run(
          normalizeRowId(item?.id, "work-order", index),
          vesselId,
          normalizeTextValue(item?.item || item?.title, `Weekly entry ${index + 1}`),
          normalizeTextValue(item?.priority, "Medium"),
          normalizeTextValue(item?.status, "Open"),
          normalizeNullableTextValue(reportDate),
          mergedFields.notes,
          index,
          workOrderCreatedAt,
          workOrderUpdatedAt,
          weekRange.start,
          weekRange.end,
          reportDate,
          mergedFields.workDone,
          mergedFields.systemsChecked,
          mergedFields.issues,
          normalizeNullableTextValue(item?.maintenanceLogId),
          normalizeWorkOrderOriginType(item?.originType),
          normalizeNullableTextValue(item?.completedAt)
        );
      });

      bundle.charters.forEach((item, index) => {
        insertCharterStatement.run(
          normalizeRowId(item?.id, "charter", index),
          vesselId,
          normalizeTextValue(item?.client, `Charter ${index + 1}`),
          normalizeTextValue(item?.start, ""),
          normalizeTextValue(item?.end, ""),
          normalizeTextValue(item?.berth, ""),
          normalizeTextValue(item?.status, ""),
          index,
          persistedAt,
          persistedAt
        );
      });

      bundle.crew.forEach((item, index) => {
        insertCrewStatement.run(
          normalizeRowId(item?.id, "crew", index),
          vesselId,
          normalizeTextValue(item?.name, `Crew ${index + 1}`),
          normalizeTextValue(item?.role, ""),
          normalizeTextValue(item?.certification, ""),
          normalizeTextValue(item?.rotation, ""),
          index,
          persistedAt,
          persistedAt
        );
      });

      bundle.reports.forEach((item, index) => {
        const reportId = normalizeRowId(item?.id, `weekly-report-${vessel.clientId}`, index);
        const weekRange = getWorkWeekRange(item?.weekStart || item?.weekEnd || item?.entries?.[0]?.reportDate || nowIso());
        const weekStart = formatDateOnly(item?.weekStart) || weekRange.start;
        const weekEnd = formatDateOnly(item?.weekEnd) || weekRange.end;
        const reportCreatedAt = normalizeTextValue(item?.createdAt, persistedAt);
        const reportUpdatedAt = normalizeTextValue(item?.updatedAt, persistedAt);
        insertWeeklyReportStatement.run(
          reportId,
          vesselId,
          weekStart,
          weekEnd,
          normalizeWeeklyReportStatus(item?.status),
          JSON.stringify(
            parseWeeklyReportVesselSnapshot(
              item?.vesselSnapshot,
              vessel,
              reportUpdatedAt
            )
          ),
          index,
          reportCreatedAt,
          reportUpdatedAt
        );

        normalizeCollectionItems(item?.entries).forEach((entry, entryIndex) => {
          insertWeeklyReportEntryStatement.run(
            normalizeRowId(entry?.id, `${reportId}-entry`, entryIndex),
            reportId,
            normalizeTextValue(entry?.item, "General update"),
            formatDateOnly(entry?.reportDate || weekStart),
            normalizeTextValue(entry?.workDone, ""),
            normalizeTextValue(entry?.systemsChecked, ""),
            normalizeTextValue(entry?.issues, ""),
            normalizeTextValue(entry?.notes, ""),
            normalizeTextValue(entry?.sourceWorkOrderId ?? entry?.source_work_order_id, ""),
            entryIndex,
            normalizeTextValue(entry?.createdAt, reportCreatedAt),
            normalizeTextValue(entry?.updatedAt, reportUpdatedAt)
          );
        });
      });

      bundle.vendors.forEach((item, index) => {
        insertVendorStatement.run(
          normalizeRowId(item?.id, "vendor", index),
          vesselId,
          normalizeTextValue(item?.name, `Vendor ${index + 1}`),
          normalizeTextValue(item?.contact, ""),
          normalizeTextValue(item?.email, ""),
          normalizeTextValue(item?.phone, ""),
          normalizeTextValue(item?.status, ""),
          normalizeTextValue(item?.category, ""),
          index,
          persistedAt,
          persistedAt
        );
      });

      bundle.voyages.forEach((item, index) => {
        insertVoyageStatement.run(
          normalizeRowId(item?.id, "voyage", index),
          vesselId,
          normalizeTextValue(item?.route, `Voyage ${index + 1}`),
          normalizeTextValue(item?.departure, ""),
          normalizeTextValue(item?.weather, ""),
          normalizeTextValue(item?.status, ""),
          index,
          persistedAt,
          persistedAt
        );
      });
    });

    mergeMaintenanceHistoryRecords(existingMaintenanceHistory, generatedMaintenanceHistoryEntries).forEach((entry) => {
      insertMaintenanceHistoryStatement.run(
        normalizeTextValue(entry.id, randomUUID()),
        Number(entry.vesselId ?? entry.vessel_id),
        normalizeTextValue(entry.maintenanceLogId ?? entry.maintenance_log_id, ""),
        normalizeNullableTextValue(entry.assetId ?? entry.asset_id),
        normalizeNullableTextValue(entry.templateTaskId ?? entry.template_task_id),
        normalizeNullableTextValue(entry.workOrderId ?? entry.work_order_id),
        normalizeTextValue(entry.source, "manual"),
        normalizeTextValue(entry.completedAt ?? entry.completed_at, persistedAt),
        formatDateOnly(
          (entry.completionDate ?? entry.completion_date ?? entry.completedAt ?? entry.completed_at ?? persistedAt)
        ),
        normalizeIntegerValue(entry.completedHours ?? entry.completed_hours, 0),
        normalizeTextValue(entry.workDone ?? entry.work_done, ""),
        normalizeTextValue(entry.systemsChecked ?? entry.systems_checked, ""),
        normalizeTextValue(entry.issues, ""),
        normalizeTextValue(entry.notes, ""),
        normalizeTextValue(entry.createdAt ?? entry.created_at, persistedAt),
        normalizeTextValue(entry.updatedAt ?? entry.updated_at, persistedAt)
      );
    });

    if (options.actorUser && !hasFullFleetAccess(options.actorUser) && grantAccessClientIds.size) {
      const grantedVesselIds = Array.from(grantAccessClientIds)
        .map((clientId) => vesselIdLookup.get(clientId))
        .filter((value) => Number.isFinite(value) && value > 0);
      grantUserVesselAccess(options.actorUser.id, grantedVesselIds);
    }

    const insertPreferenceStatement = database.prepare(`
      INSERT INTO app_preferences (key, value, updated_at)
      VALUES (?, ?, ?)
    `);
    const persistedActiveVesselId = vesselIdLookup.get(activeClientVesselId)
      || vesselIdLookup.get(normalizedVessels[0]?.clientId);
    const appPreferences = [
      ...basePreferences,
      ["activeVesselId", persistedActiveVesselId ? String(persistedActiveVesselId) : ""],
    ];
    appPreferences.forEach(([key, value]) => {
      insertPreferenceStatement.run(key, value, persistedAt);
    });
  });
}

function mergeStateForScopedVesselAccess(incomingState, existingState, allowedExistingIds) {
  const nextIncoming = incomingState && typeof incomingState === "object" ? incomingState : {};
  const nextExisting = existingState && typeof existingState === "object" ? existingState : {};
  const allowedExistingSet = new Set(Array.from(allowedExistingIds || []).map((value) => String(value)));
  const incomingVessels = Array.isArray(nextIncoming.vessels) ? nextIncoming.vessels : [];
  const existingVessels = Array.isArray(nextExisting.vessels) ? nextExisting.vessels : [];
  const allowedClientIds = new Set();

  incomingVessels.forEach((vessel) => {
    const vesselId = String(vessel?.id ?? "").trim();
    if (!vesselId) {
      return;
    }
    if (!/^\d+$/u.test(vesselId) || allowedExistingSet.has(vesselId)) {
      allowedClientIds.add(vesselId);
    }
  });

  const incomingVesselsById = new Map(incomingVessels.map((vessel) => [String(vessel?.id ?? ""), vessel]).filter(([id]) => id));
  const mergedVessels = [];
  const seen = new Set();

  existingVessels.forEach((vessel) => {
    const vesselId = String(vessel?.id ?? "");
    if (!vesselId || seen.has(vesselId)) {
      return;
    }
    if (allowedClientIds.has(vesselId) && incomingVesselsById.has(vesselId)) {
      mergedVessels.push(incomingVesselsById.get(vesselId));
    } else {
      mergedVessels.push(vessel);
    }
    seen.add(vesselId);
  });

  incomingVessels.forEach((vessel) => {
    const vesselId = String(vessel?.id ?? "");
    if (!vesselId || seen.has(vesselId) || !allowedClientIds.has(vesselId)) {
      return;
    }
    mergedVessels.push(vessel);
    seen.add(vesselId);
  });

  const mergedBundles = {
    ...(nextExisting.vesselBundles && typeof nextExisting.vesselBundles === "object" ? nextExisting.vesselBundles : {}),
  };
  const incomingBundles = nextIncoming.vesselBundles && typeof nextIncoming.vesselBundles === "object"
    ? nextIncoming.vesselBundles
    : {};
  Object.entries(incomingBundles).forEach(([vesselId, bundle]) => {
    if (allowedClientIds.has(String(vesselId))) {
      mergedBundles[String(vesselId)] = bundle;
    }
  });

  const nextActiveVesselId = (() => {
    const requestedActiveId = String(nextIncoming.activeVesselId ?? "").trim();
    if (requestedActiveId && allowedClientIds.has(requestedActiveId)) {
      return requestedActiveId;
    }
    const existingActiveId = String(nextExisting.activeVesselId ?? "").trim();
    if (existingActiveId && mergedVessels.some((vessel) => String(vessel?.id ?? "") === existingActiveId)) {
      return existingActiveId;
    }
    return String(mergedVessels[0]?.id ?? "");
  })();

  const activeBundle = mergedBundles[nextActiveVesselId] || {};
  const activeVessel = mergedVessels.find((vessel) => String(vessel?.id ?? "") === nextActiveVesselId) || mergedVessels[0] || nextExisting.vessel || nextIncoming.vessel || {};

  return {
    ...nextExisting,
    ...nextIncoming,
    vessels: mergedVessels,
    activeVesselId: nextActiveVesselId,
    vesselBundles: mergedBundles,
    vessel: activeVessel,
    maintenance: activeBundle.maintenance || nextIncoming.maintenance || nextExisting.maintenance || [],
    workOrders: activeBundle.workOrders || nextIncoming.workOrders || nextExisting.workOrders || [],
    inventory: activeBundle.inventory || nextIncoming.inventory || nextExisting.inventory || [],
    expenses: activeBundle.expenses || nextIncoming.expenses || nextExisting.expenses || [],
    charters: activeBundle.charters || nextIncoming.charters || nextExisting.charters || [],
    crew: activeBundle.crew || nextIncoming.crew || nextExisting.crew || [],
    reports: activeBundle.reports || nextIncoming.reports || nextExisting.reports || [],
    vendors: activeBundle.vendors || nextIncoming.vendors || nextExisting.vendors || [],
    voyages: activeBundle.voyages || nextIncoming.voyages || nextExisting.voyages || [],
  };
}

function saveState(body, sessionUser) {
  const parsed = parseJsonBody(body);

  if (!sessionUser || hasFullFleetAccess(sessionUser)) {
    saveStructuredState(parsed, {
      actorUser: sessionUser,
    });
    return;
  }

  const existingState = getStoredState()?.state || {};
  const allowedExistingIds = getUserAccessibleVesselIds(sessionUser);
  const mergedState = mergeStateForScopedVesselAccess(parsed, existingState, allowedExistingIds);
  const requestedClientIds = Array.isArray(parsed?.vessels)
    ? parsed.vessels.map((vessel) => String(vessel?.id ?? "").trim()).filter(Boolean)
    : [];

  saveStructuredState(mergedState, {
    actorUser: sessionUser,
    grantAccessClientIds: requestedClientIds.filter((value) => !/^\d+$/u.test(value) || allowedExistingIds.includes(Number(value))),
  });
}

function migrateLegacyStateIfNeeded() {
  if (hasStructuredState()) {
    return;
  }

  const legacyRow = selectLegacyStateStatement.get();
  if (!legacyRow?.state_json) {
    return;
  }

  try {
    const parsedLegacyState = parseJsonBody(legacyRow.state_json);
    saveStructuredState(parsedLegacyState, {
      updatedAt: legacyRow.updated_at || nowIso(),
    });
  } catch (error) {
    console.warn("Unable to migrate legacy Harbor Command app_state into structured tables.", error);
  }
}

migrateLegacyStateIfNeeded();
migrateLegacyReportsIfNeeded();
migrateCurrentWeekWorkspaceIfNeeded();
repairWorkOrderDatesFromReportsIfNeeded();
backfillLegacyUserVesselAccess();

function resolveStaticPath(requestPath) {
  const requested = !requestPath || requestPath === "/" ? "index.html" : requestPath.replace(/^\/+/, "");
  const decodedPath = decodeURIComponent(requested.split("?")[0]);
  const normalizedPath = decodedPath ? decodedPath : "index.html";
  const fullPath = path.resolve(__dirname, normalizedPath);

  if (!fullPath.startsWith(__dirname)) {
    return null;
  }

  if (!existsSync(fullPath)) {
    return null;
  }

  if (path.extname(fullPath) === "" && existsSync(path.join(fullPath, "index.html"))) {
    return path.join(fullPath, "index.html");
  }

  return fullPath;
}

function serveStaticFile(response, requestPath) {
  const filePath = resolveStaticPath(requestPath);

  if (!filePath) {
    applyStaticHeaders(response);
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Not found");
    return;
  }

  const extension = path.extname(filePath).toLowerCase();
  const contentType = contentTypes[extension] ?? "application/octet-stream";
  const body = readFileSync(filePath);

  applyStaticHeaders(response);
  response.writeHead(200, {
    "Content-Type": contentType,
    "Content-Length": body.length,
    "Cache-Control": "no-store, no-cache, must-revalidate",
    Pragma: "no-cache",
    Expires: "0",
  });
  response.end(body);
}

async function handleApiRequest(request, response) {
    const url = new URL(request.url, localAppUrl);
  const hasUsers = countUsers() > 0;

  applyApiHeaders(request, response);

  const originValidationError = validateApiOrigin(request);
  if (originValidationError) {
    sendJson(response, 403, { ok: false, error: originValidationError });
    return;
  }

  const rateLimitResult = checkRateLimit(request, url);
  if (rateLimitResult) {
    sendJson(
      response,
      429,
      { ok: false, error: rateLimitResult.message },
      { "Retry-After": String(rateLimitResult.retryAfterSeconds) }
    );
    return;
  }

  if (request.method === "OPTIONS") {
    sendEmpty(response, 204);
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/health") {
    sendJson(response, 200, { ok: true });
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/auth/status") {
    const sessionUser = getSessionUser(request);
    sendJson(response, 200, {
      ok: true,
      hasUsers,
      authenticated: Boolean(sessionUser),
      user: sanitizeUser(sessionUser),
    });
    return;
  }

  if (request.method === "POST" && url.pathname === "/api/auth/setup") {
    if (hasUsers) {
      sendJson(response, 409, { ok: false, error: "An account already exists. Please log in." });
      return;
    }

    try {
      const body = await readRequestBody(request);
      const payload = validateSetupPayload(parseJsonBody(body));

      if (findUserByEmail(payload.email)) {
        sendJson(response, 409, { ok: false, error: "Email is already in use." });
        return;
      }

      const user = createUser(payload);
      const sessionToken = createSession(user.id);

      sendJson(
        response,
        200,
        {
          ok: true,
          hasUsers: true,
          authenticated: true,
          user: sanitizeUser(user),
        },
        {
          "Set-Cookie": buildSessionCookie(request, sessionToken, SESSION_MAX_AGE_SECONDS),
        }
      );
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to create account.",
      });
    }

    return;
  }

  if (request.method === "POST" && url.pathname === "/api/auth/login") {
    if (!hasUsers) {
      sendJson(response, 409, { ok: false, error: "No account exists yet. Create the first user." });
      return;
    }

    try {
      const body = await readRequestBody(request);
      const payload = validateLoginPayload(parseJsonBody(body));
      const user = findUserByEmail(payload.email);

      if (!user || !verifyPassword(payload.password, user.password_hash)) {
        sendJson(response, 401, { ok: false, error: "Email or password is incorrect." });
        return;
      }

      if (!user.is_active) {
        sendJson(response, 403, { ok: false, error: "This account is inactive. Contact vessel management." });
        return;
      }

      const sessionToken = createSession(user.id);

      sendJson(
        response,
        200,
        {
          ok: true,
          hasUsers: true,
          authenticated: true,
          user: sanitizeUser(user),
        },
        {
          "Set-Cookie": buildSessionCookie(request, sessionToken, SESSION_MAX_AGE_SECONDS),
        }
      );
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to log in.",
      });
    }

    return;
  }

  if (request.method === "POST" && url.pathname === "/api/auth/logout") {
    destroySession(getSessionToken(request));
    sendJson(
      response,
      200,
      {
        ok: true,
      },
      {
        "Set-Cookie": buildSessionCookie(request, "", 0),
      }
    );
    return;
  }

  const inviteAcceptMatch = url.pathname.match(/^\/api\/invite\/([^/]+)\/accept$/);
  const invitePublicMatch = url.pathname.match(/^\/api\/invite\/([^/]+)$/);

  if (request.method === "GET" && invitePublicMatch && !inviteAcceptMatch) {
    const token = decodeURIComponent(invitePublicMatch[1]);
    const { invite, error } = getValidInvite(token);

    if (!invite) {
      sendJson(response, 404, { ok: false, error });
      return;
    }

    sendJson(response, 200, { ok: true, invite: sanitizePublicInvite(invite) });
    return;
  }

  if (request.method === "POST" && inviteAcceptMatch) {
    const token = decodeURIComponent(inviteAcceptMatch[1]);
    const { invite, error } = getValidInvite(token);

    if (!invite) {
      sendJson(response, 400, { ok: false, error });
      return;
    }

    try {
      const body = await readRequestBody(request);
      const payload = validateInviteAcceptPayload(parseJsonBody(body));
      const user = createUser({
        name: payload.name,
        email: invite.email,
        password: payload.password,
        role: invite.role,
        vesselIds: getInviteAccessibleVesselIds(invite),
      });
      markInviteUsed(invite.id);
      const sessionToken = createSession(user.id);

      sendJson(
        response,
        200,
        {
          ok: true,
          hasUsers: true,
          authenticated: true,
          user: sanitizeUser(user),
        },
        {
          "Set-Cookie": buildSessionCookie(request, sessionToken, SESSION_MAX_AGE_SECONDS),
        }
      );
    } catch (routeError) {
      sendJson(response, 400, {
        ok: false,
        error: routeError instanceof Error ? routeError.message : "Unable to accept invite.",
      });
    }

    return;
  }

  const sessionUser = getSessionUser(request);

  if (!sessionUser) {
    sendJson(response, 401, { ok: false, error: "Authentication required." });
    return;
  }

  const userIdMatch = url.pathname.match(/^\/api\/users\/(\d+)$/);
  const inviteIdMatch = url.pathname.match(/^\/api\/invites\/(\d+)$/);
  const inviteSendMatch = url.pathname.match(/^\/api\/invites\/(\d+)\/send$/);
  const weeklyReportPdfMatch = url.pathname.match(/^\/api\/weekly-reports\/([^/]+)\/pdf$/);
  const weeklyReportMatch = url.pathname.match(/^\/api\/weekly-reports\/([^/]+)$/);
  const weeklyReportEntriesMatch = url.pathname.match(/^\/api\/weekly-reports\/([^/]+)\/entries$/);
  const weeklyReportEntryMatch = url.pathname.match(/^\/api\/weekly-report-entries\/([^/]+)$/);

  if (request.method === "GET" && url.pathname === "/api/invites") {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage invites." });
      return;
    }

    sendJson(response, 200, {
      ok: true,
      invites: listPendingInvites(),
      delivery: getInviteDeliveryStatus(),
    });
    return;
  }

  if (request.method === "POST" && url.pathname === "/api/invites") {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage invites." });
      return;
    }

    try {
      const body = await readRequestBody(request);
      const payload = validateInvitePayload(parseJsonBody(body));

      if (findUserByEmail(payload.email)) {
        sendJson(response, 409, { ok: false, error: "An account already exists for this email." });
        return;
      }

      if (findPendingInviteByEmail(payload.email)) {
        sendJson(response, 409, { ok: false, error: "A pending invite already exists for this email." });
        return;
      }

      const invite = createInvite(payload, sessionUser.id);
      let emailSent = false;
      let emailError = "";

      try {
        await sendInviteEmail(invite);
        emailSent = true;
      } catch (emailSendError) {
        emailError = emailSendError instanceof Error ? emailSendError.message : "Unable to send invite email.";
      }

      sendJson(response, 200, {
        ok: true,
        invite,
        emailSent,
        emailError,
        delivery: getInviteDeliveryStatus(),
      });
    } catch (routeError) {
      sendJson(response, 400, {
        ok: false,
        error: routeError instanceof Error ? routeError.message : "Unable to create invite.",
      });
    }

    return;
  }

  if (request.method === "POST" && inviteSendMatch) {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage invites." });
      return;
    }

    const inviteId = Number(inviteSendMatch[1]);
    const targetInvite = findInviteById(inviteId);

    if (!targetInvite) {
      sendJson(response, 404, { ok: false, error: "Invite not found." });
      return;
    }

    const { invite, error } = getValidInvite(targetInvite.token);
    if (!invite) {
      sendJson(response, 400, { ok: false, error });
      return;
    }

    try {
      const emailResult = await sendInviteEmail(invite);
      sendJson(response, 200, {
        ok: true,
        invite: sanitizeInvite(invite),
        delivery: getInviteDeliveryStatus(),
        emailId: emailResult?.id || "",
      });
    } catch (routeError) {
      sendJson(response, 400, {
        ok: false,
        error: routeError instanceof Error ? routeError.message : "Unable to send invite email.",
        delivery: getInviteDeliveryStatus(),
      });
    }

    return;
  }

  if (request.method === "DELETE" && inviteIdMatch) {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage invites." });
      return;
    }

    const inviteId = Number(inviteIdMatch[1]);
    const targetInvite = findInviteById(inviteId);

    if (!targetInvite) {
      sendJson(response, 404, { ok: false, error: "Invite not found." });
      return;
    }

    revokeInvite(inviteId);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/users") {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage users." });
      return;
    }

    sendJson(response, 200, { ok: true, users: listUsers(), vessels: listManageableVessels(sessionUser) });
    return;
  }

  if (request.method === "POST" && url.pathname === "/api/users") {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage users." });
      return;
    }

    try {
      const body = await readRequestBody(request);
      const payload = validateManagedUserPayload(parseJsonBody(body));

      if (findUserByEmail(payload.email)) {
        sendJson(response, 409, { ok: false, error: "Email is already in use." });
        return;
      }

      const user = createUser(payload);
      sendJson(response, 200, { ok: true, user: sanitizeManagedUser(findManagedUserById(user.id)) });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to create user.",
      });
    }

    return;
  }

  if (request.method === "PATCH" && userIdMatch) {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage users." });
      return;
    }

    const userId = Number(userIdMatch[1]);
    const targetUser = findManagedUserById(userId);

    if (!targetUser) {
      sendJson(response, 404, { ok: false, error: "User not found." });
      return;
    }

    try {
      const body = await readRequestBody(request);
      const payload = parseJsonBody(body || "{}");

      if (Array.isArray(payload?.vesselIds)) {
        if (hasFullFleetAccess(targetUser.role)) {
          sendJson(response, 200, {
            ok: true,
            user: sanitizeManagedUser(findManagedUserById(userId)),
          });
          return;
        }

        const vesselIds = resolveExistingVesselIds(payload.vesselIds);
        if (!vesselIds.length && listAllVesselIds().length) {
          sendJson(response, 400, { ok: false, error: "Select at least one vessel for this user." });
          return;
        }

        setUserVesselAccess(userId, vesselIds);
        sendJson(response, 200, {
          ok: true,
          user: sanitizeManagedUser(findManagedUserById(userId)),
        });
        return;
      }

      if (typeof payload?.isActive !== "boolean") {
        sendJson(response, 400, { ok: false, error: "A valid active state is required." });
        return;
      }

      if (targetUser.id === sessionUser.id) {
        sendJson(response, 400, { ok: false, error: "You cannot change the status of the account you are currently using." });
        return;
      }

      const isDisablingLastAdmin =
        !payload.isActive &&
        Boolean(targetUser.is_active) &&
        USER_ADMIN_ROLES.has(targetUser.role) &&
        countActiveAdminUsers() <= 1;

      if (isDisablingLastAdmin) {
        sendJson(response, 400, { ok: false, error: "At least one active Captain or Management account must remain." });
        return;
      }

      setUserActive(userId, payload.isActive);
      if (!payload.isActive) {
        destroySessionsForUser(userId);
      }

      sendJson(response, 200, {
        ok: true,
        user: sanitizeManagedUser(findManagedUserById(userId)),
      });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to update user.",
      });
    }

    return;
  }

  if (request.method === "DELETE" && userIdMatch) {
    if (!canManageUsers(sessionUser)) {
      sendJson(response, 403, { ok: false, error: "Only Captain or Management can manage users." });
      return;
    }

    const userId = Number(userIdMatch[1]);
    const targetUser = findManagedUserById(userId);

    if (!targetUser) {
      sendJson(response, 404, { ok: false, error: "User not found." });
      return;
    }

    if (targetUser.id === sessionUser.id) {
      sendJson(response, 400, { ok: false, error: "You cannot delete the account you are currently using." });
      return;
    }

    const isDeletingLastAdmin =
      Boolean(targetUser.is_active) &&
      USER_ADMIN_ROLES.has(targetUser.role) &&
      countActiveAdminUsers() <= 1;

    if (isDeletingLastAdmin) {
      sendJson(response, 400, { ok: false, error: "At least one active Captain or Management account must remain." });
      return;
    }

    deleteUser(userId);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/weekly-reports") {
    const vesselId = normalizeIntegerValue(url.searchParams.get("vesselId"), 0);
    if (vesselId <= 0) {
      sendJson(response, 400, { ok: false, error: "A vesselId query value is required." });
      return;
    }

    if (!isVesselAccessibleToUser(sessionUser, vesselId)) {
      sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
      return;
    }

    sendJson(response, 200, {
      ok: true,
      reports: loadWeeklyReportsForVessel(vesselId),
    });
    return;
  }

  if (request.method === "POST" && url.pathname === "/api/weekly-reports") {
    try {
      const body = await readRequestBody(request);
      const payload = validateWeeklyReportPayload(parseJsonBody(body), { requireVesselId: true });
      if (!isVesselAccessibleToUser(sessionUser, payload.vesselId)) {
        sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
        return;
      }
      const report = createWeeklyReport(payload);
      sendJson(response, 200, { ok: true, report });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to create weekly report.",
      });
    }
    return;
  }

  if (request.method === "POST" && url.pathname === "/api/weekly-reports/generate") {
    try {
      const body = await readRequestBody(request);
      const payload = validateWeeklyReportPayload(parseJsonBody(body), { requireVesselId: true });
      if (!isVesselAccessibleToUser(sessionUser, payload.vesselId)) {
        sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
        return;
      }
      const report = generateWeeklyReportFromWorkspace(payload);
      sendJson(response, 200, { ok: true, report });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to generate the weekly report.",
      });
    }
    return;
  }

  if (request.method === "GET" && weeklyReportPdfMatch) {
    const report = getWeeklyReportRecord(decodeURIComponent(weeklyReportPdfMatch[1]));
    if (!report) {
      sendJson(response, 404, { ok: false, error: "Weekly report not found." });
      return;
    }

    if (!isVesselAccessibleToUser(sessionUser, report.vesselId)) {
      sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
      return;
    }

    const vessel = findVesselById(report.vesselId);
    if (!vessel) {
      sendJson(response, 404, { ok: false, error: "Vessel not found for this weekly report." });
      return;
    }

    const pdfBuffer = buildWeeklyReportPdfBuffer(report, vessel);
    sendBuffer(response, 200, "application/pdf", pdfBuffer, {
      "Content-Disposition": `inline; filename="${buildWeeklyReportPdfFilename(report, vessel)}"`,
    });
    return;
  }

  if (request.method === "GET" && weeklyReportMatch && !weeklyReportEntriesMatch) {
    const report = getWeeklyReportRecord(decodeURIComponent(weeklyReportMatch[1]));
    if (!report) {
      sendJson(response, 404, { ok: false, error: "Weekly report not found." });
      return;
    }

    if (!isVesselAccessibleToUser(sessionUser, report.vesselId)) {
      sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
      return;
    }

    sendJson(response, 200, { ok: true, report });
    return;
  }

  if (request.method === "PATCH" && weeklyReportMatch && !weeklyReportEntriesMatch) {
    try {
      const body = await readRequestBody(request);
      const existingReport = getWeeklyReportRecord(decodeURIComponent(weeklyReportMatch[1]));
      if (!existingReport) {
        sendJson(response, 404, { ok: false, error: "Weekly report not found." });
        return;
      }
      if (!isVesselAccessibleToUser(sessionUser, existingReport.vesselId)) {
        sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
        return;
      }
      const payload = parseJsonBody(body);
      const report = updateWeeklyReport(decodeURIComponent(weeklyReportMatch[1]), payload);
      sendJson(response, 200, { ok: true, report });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to update weekly report.",
      });
    }
    return;
  }

  if (request.method === "POST" && weeklyReportEntriesMatch) {
    try {
      const body = await readRequestBody(request);
      const existingReport = getWeeklyReportRecord(decodeURIComponent(weeklyReportEntriesMatch[1]));
      if (!existingReport) {
        sendJson(response, 404, { ok: false, error: "Weekly report not found." });
        return;
      }
      if (!isVesselAccessibleToUser(sessionUser, existingReport.vesselId)) {
        sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
        return;
      }
      const payload = validateWeeklyReportEntryPayload(parseJsonBody(body));
      const report = createWeeklyReportEntry(decodeURIComponent(weeklyReportEntriesMatch[1]), payload);
      sendJson(response, 200, { ok: true, report });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to create report entry.",
      });
    }
    return;
  }

  if (request.method === "PATCH" && weeklyReportEntryMatch) {
    try {
      const body = await readRequestBody(request);
      const existingReport = getWeeklyReportRecord(getWeeklyReportEntryReportId(decodeURIComponent(weeklyReportEntryMatch[1])));
      if (!existingReport) {
        sendJson(response, 404, { ok: false, error: "Report entry not found." });
        return;
      }
      if (!isVesselAccessibleToUser(sessionUser, existingReport.vesselId)) {
        sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
        return;
      }
      const payload = validateWeeklyReportEntryPayload(parseJsonBody(body));
      const report = updateWeeklyReportEntry(decodeURIComponent(weeklyReportEntryMatch[1]), payload);
      sendJson(response, 200, { ok: true, report });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to update report entry.",
      });
    }
    return;
  }

  if (request.method === "DELETE" && weeklyReportEntryMatch) {
    try {
      const existingReport = getWeeklyReportRecord(getWeeklyReportEntryReportId(decodeURIComponent(weeklyReportEntryMatch[1])));
      if (!existingReport) {
        sendJson(response, 404, { ok: false, error: "Report entry not found." });
        return;
      }
      if (!isVesselAccessibleToUser(sessionUser, existingReport.vesselId)) {
        sendJson(response, 403, { ok: false, error: "You do not have access to that vessel." });
        return;
      }
      const report = deleteWeeklyReportEntry(decodeURIComponent(weeklyReportEntryMatch[1]));
      sendJson(response, 200, { ok: true, report });
    } catch (error) {
      sendJson(response, 400, {
        ok: false,
        error: error instanceof Error ? error.message : "Unable to delete report entry.",
      });
    }
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/bootstrap") {
    const stored = getStoredState(sessionUser);
    sendJson(response, 200, stored ? { hasData: true, state: stored.state, catalog: loadVesselCatalog() } : { hasData: false, state: null, catalog: loadVesselCatalog() });
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/vessel-catalog") {
    sendJson(response, 200, { ok: true, catalog: loadVesselCatalog() });
    return;
  }

  if (request.method === "PUT" && url.pathname === "/api/bootstrap") {
    try {
      const body = await readRequestBody(request);

      if (!body.trim()) {
        sendJson(response, 400, { ok: false, error: "Request body is required." });
        return;
      }

      saveState(body, sessionUser);
      sendJson(response, 200, { ok: true });
    } catch (error) {
      sendJson(response, 400, { ok: false, error: error instanceof Error ? error.message : "Invalid request." });
    }

    return;
  }

  sendJson(response, 404, { ok: false, error: "Not found." });
}

const server = createServer(async (request, response) => {
  try {
    if (!request.url) {
      applyStaticHeaders(response);
      response.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
      response.end("Bad request");
      return;
    }

    if (request.url.startsWith("/api/")) {
      await handleApiRequest(request, response);
      return;
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      applyStaticHeaders(response);
      response.writeHead(405, { "Content-Type": "text/plain; charset=utf-8" });
      response.end("Method not allowed");
      return;
    }

    serveStaticFile(response, request.url);
  } catch (error) {
    applyCommonSecurityHeaders(response);
    response.writeHead(500, { "Content-Type": "application/json; charset=utf-8" });
    response.end(JSON.stringify({ ok: false, error: error instanceof Error ? error.message : "Unexpected server error" }));
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log("");
  console.log("Harbor Command API is running.");
  console.log(`App URL: ${localAppUrl}/`);
  console.log(`Database: ${databasePath}`);
  console.log("Authentication: enabled");
  console.log(`Invite email delivery: ${getInviteDeliveryStatus().ready ? "Resend configured" : "copy-link fallback"}`);
  console.log("Press Ctrl+C to stop the server.");
  console.log("");
});
