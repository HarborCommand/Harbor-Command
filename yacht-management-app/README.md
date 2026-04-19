# Harbor Command

Harbor Command is a single-vessel operations dashboard for managing maintenance, work orders, reports, vendors, crew, charters, and voyage planning from one focused command center.

## What it includes

- Navigation-driven workspace with separate sections for overview, vessel systems, maintenance, work orders, charters, crew, reports, vendors, inventory, expenses, and voyage activity
- Single-vessel profile with editable captain, berth, fuel, utilization, and command notes
- Dedicated vessel systems tab for fuel reserve, water tank level, grey tank level, and next service tracking
- Section-based maintenance planner with scrollable system selectors, search, reminder windows, service intervals, and rescheduling-friendly recurring service tracking, plus a separate work order board for one-off issues
- Charter planner for owner or guest bookings
- Crew list, a weekly log table, an editable vendor directory, and voyage alert feed
- A local Node.js backend API with SQLite persistence so vessel data is stored outside the browser in structured tables for vessels, maintenance, work orders, vendors, reports, crew, charters, voyages, inventory, and expenses
- The SQLite database lives in `data/harbor-command.db`, so data persists to a file on disk and survives server restarts
- Session-based authentication with first-user setup, secure login, logout, and protected vessel data routes
- Admin-managed access with invite links, an in-app Access popup, direct Resend invite emails, and activate or remove controls for existing accounts
- Built-in app hardening with security headers, same-origin write protection, request-size limits, and rate limiting on auth and access-management routes
- `localStorage` kept as a lightweight fallback and migration backup

## Run it

Use `start-harbor-command.cmd`.

That will:

- reuse the existing local Harbor Command service if it is already healthy
- restart a stale Harbor Command `node` process automatically if it is occupying port `8787` but not responding
- start the local Node.js API if it is not already running
- wait for the local service to become healthy before opening the app
- avoid launching duplicate Harbor Command servers on the same port
- create `data/harbor-command.db` the first time it runs
- open the app at `http://127.0.0.1:8787/`
- prompt you to create the first account before the dashboard unlocks

To create a reusable Desktop shortcut for Harbor Command, run:

```powershell
.\create-harbor-command-shortcut.ps1
```

That creates `Harbor Command.lnk` on your Desktop and launches the same local startup flow.

If Harbor Command cannot start from the shortcut, check:

- [start-harbor-command.cmd](C:/Users/arive/OneDrive/Documents/New%20project/yacht-management-app/start-harbor-command.cmd)
- [server.mjs](C:/Users/arive/OneDrive/Documents/New%20project/yacht-management-app/server.mjs)

If you prefer to start it manually, run:

```powershell
& "C:\Program Files\nodejs\node.exe" .\server.mjs
```

Or from the project folder:

```powershell
& "C:\Program Files\nodejs\npm.cmd" start
```

## Resend invite email setup

To send invite links directly from the `Access` popup, create a `.env.local` file in the project folder with:

```env
RESEND_API_KEY=re_xxxxxxxxx
RESEND_FROM_EMAIL=Harbor Command <onboarding@yourdomain.com>
HARBOR_COMMAND_PUBLIC_URL=https://your-app.example.com
```

Optional:

```env
RESEND_REPLY_TO=ops@yourdomain.com
```

Notes:

- `HARBOR_COMMAND_PUBLIC_URL` should be the real app URL your crew will open. If the app is only running on `localhost`, emailed invite links will only work on your own machine.
- If Resend is not configured yet, the app still lets you copy and share invite links manually.

## Security notes

- Harbor Command now blocks cross-origin write requests, adds protective browser headers, and rate-limits login, invite, and state-save routes.
- This is app-level protection, not a full network firewall. For a true perimeter firewall, pair it with your hosting layer such as Cloudflare, a reverse proxy, or a VPS/security-group allowlist.

## Good next steps

- Add password reset email, invite resend history, and section-level role permissions
- Connect maps, weather, marina availability, and document storage
- Add accounting, provisioning, and compliance workflows
