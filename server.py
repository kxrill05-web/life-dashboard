#!/usr/bin/env python3
"""Life-Dashboard Backend — speichert Todos, Kalender-Termine und Trainingsplan lokal in SQLite.

Endpunkte:
  GET  /state   -> kompletter Zustand als JSON {todos, events, trainingPlan, trainingLog}
  POST /state   -> kompletten Zustand speichern (überschreibt alle Tabellen)

Alles läuft lokal. Datenbank liegt als life_dashboard.db neben diesem Skript.
"""
import base64
import datetime
import hashlib
import hmac
import html
import io
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
import wave
import xml.etree.ElementTree as ET
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import caldav
import icalendar
import garminconnect
import psycopg2
import psycopg2.extras

PORT = int(os.environ.get("PORT", 5006))
ENV_PATH = Path(__file__).parent / ".env"
WEEKDAYS = ['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag']
GEMINI_MODEL = "gemini-3.5-flash"
CLAUDE_MODEL = "claude-sonnet-5"
GEMINI_TTS_MODEL = "gemini-2.5-flash-preview-tts"
GEMINI_TTS_VOICE = "Charon"  # maennlich, klar/informativ — siehe https://ai.google.dev/gemini-api/docs/speech-generation
ICLOUD_CALENDAR_NAME = "Home"  # Kirills tatsächlich genutzter Hauptkalender (verifiziert 02.07.2026 — "Kalender" hatte nur 1 Eintrag, "Home" die echten Termine)
GARMIN_SYNC_MIN_INTERVAL = datetime.timedelta(minutes=30)
NEWS_FEED_URL = "https://www.tagesschau.de/index~rss2.xml"
NEWS_CACHE_TTL = datetime.timedelta(minutes=30)
MARKETS_CACHE_TTL = datetime.timedelta(minutes=15)
# Kirills TradingView-Instrumente (MNQ1!/MES1!/XAUUSD) — TradingView selbst hat keine kostenlose
# oeffentliche Kurs-API, darum ueber Yahoo Finance mit den passenden Micro-Futures-Tickern (per
# echtem Kursabruf verifiziert). XAUUSD (Spot-Gold) gibt es bei Yahoo unter keiner Schreibweise —
# GC=F (COMEX-Gold-Future, bereits im Einsatz) laeuft aber praktisch deckungsgleich mit Spot-Gold.
MARKET_SYMBOLS = [("NASDAQ 100", "MNQ=F"), ("S&P 500", "MES=F"), ("Gold", "GC=F")]


def load_env():
    """Auf Render kommen Secrets als echte Umgebungsvariablen (Dashboard) - lokal aus .env (gitignored)."""
    env = dict(os.environ)
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


ENV = load_env()
GEMINI_API_KEY = ENV.get("GEMINI_API_KEY", "")
ANTHROPIC_API_KEY = ENV.get("ANTHROPIC_API_KEY", "")
GARMIN_EMAIL = ENV.get("GARMIN_EMAIL", "")
GARMIN_PASSWORD = ENV.get("GARMIN_PASSWORD", "")
APPLE_ID_EMAIL = ENV.get("APPLE_ID_EMAIL", "")
APPLE_APP_PASSWORD = ENV.get("APPLE_APP_PASSWORD", "")
DATABASE_URL = ENV.get("DATABASE_URL", "")
PAGE_PASSWORD = ENV.get("PAGE_PASSWORD", "")
# Abgeleiteter Sitzungs-Token: wird nach erfolgreichem Login ans Frontend gegeben und
# muss danach bei jeder Anfrage als X-Auth-Header mitkommen. Ohne PAGE_PASSWORD (lokal) kein Zwang.
AUTH_TOKEN = hashlib.sha256(f"life-dashboard-session:{PAGE_PASSWORD}".encode()).hexdigest() if PAGE_PASSWORD else ""


class _PGConn:
    """Duennes Kompatibilitaets-Layer: gibt der bestehenden SQLite-Aufrufsyntax (conn.execute mit '?',
    with-conn-Transaktionen, dict-artige Rows) eine Postgres-Verbindung darunter, ohne die 59 bestehenden
    Aufrufstellen einzeln umschreiben zu muessen."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql.replace("?", "%s"), params)
        return cur

    def executescript(self, script):
        cur = self._conn.cursor()
        for stmt in script.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        self._conn.commit()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        return False


def get_db():
    return _PGConn(psycopg2.connect(DATABASE_URL))


def _ensure_column(conn, table, column, coldef):
    cur = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?", (table,))
    cols = [r["column_name"] for r in cur]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}")


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS todos (
            id TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            done INTEGER NOT NULL DEFAULT 0,
            date TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS calendar_events (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_time TEXT,
            recurrence TEXT NOT NULL DEFAULT 'none',
            recurrence_interval INTEGER NOT NULL DEFAULT 1,
            priority TEXT NOT NULL DEFAULT 'small',
            source TEXT NOT NULL DEFAULT 'dashboard',
            icloud_uid TEXT,
            exceptions TEXT NOT NULL DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS training_plan (
            weekday TEXT PRIMARY KEY,
            session_type TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS training_log (
            date TEXT PRIMARY KEY,
            done INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS journal_entries (
            id TEXT PRIMARY KEY,
            entry_date TEXT NOT NULL,
            mood TEXT NOT NULL,
            productivity INTEGER NOT NULL DEFAULT 0,
            note TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS goals (
            id TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            created_date TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS assistant_log (
            id TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            role TEXT NOT NULL,
            message TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS garmin_days (
            date TEXT PRIMARY KEY,
            sleep_seconds INTEGER,
            sleep_score INTEGER,
            resting_hr INTEGER,
            steps INTEGER,
            synced_at TEXT
        );
        CREATE TABLE IF NOT EXISTS garmin_activities (
            activity_id TEXT PRIMARY KEY,
            start_time TEXT,
            name TEXT,
            type TEXT,
            duration_seconds INTEGER,
            distance_m REAL,
            calories INTEGER,
            avg_hr INTEGER
        );
        CREATE TABLE IF NOT EXISTS supplements_log (
            date TEXT PRIMARY KEY,
            done INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS sync_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS categories (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            color TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0
        );
    """)
    with conn:
        for day in WEEKDAYS:
            conn.execute(
                "INSERT INTO training_plan (weekday, session_type, description) VALUES (?, '', '') "
                "ON CONFLICT (weekday) DO NOTHING",
                (day,))
        _ensure_column(conn, "todos", "priority", "TEXT NOT NULL DEFAULT 'none'")
        _ensure_column(conn, "todos", "repeat_type", "TEXT NOT NULL DEFAULT 'none'")
        _ensure_column(conn, "todos", "repeat_weekdays", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "todos", "repeat_interval", "INTEGER")
        _ensure_column(conn, "todos", "repeat_spawned", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "calendar_events", "category", "TEXT NOT NULL DEFAULT 'privat'")
        _ensure_column(conn, "training_plan", "options", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "training_log", "choice", "TEXT")
        _ensure_column(conn, "garmin_days", "deep_sleep_seconds", "INTEGER")
        _ensure_column(conn, "garmin_days", "light_sleep_seconds", "INTEGER")
        _ensure_column(conn, "garmin_days", "rem_sleep_seconds", "INTEGER")
        _ensure_column(conn, "garmin_days", "awake_sleep_seconds", "INTEGER")
        _ensure_column(conn, "garmin_days", "vo2max", "REAL")
        _ensure_column(conn, "garmin_days", "fitness_age", "REAL")
        _ensure_column(conn, "calendar_events", "end_date", "TEXT")
        _ensure_column(conn, "calendar_events", "note", "TEXT")
        # Die 4 Standard-Kategorien nur beim allerersten Start anlegen —
        # danach verwaltet Kirill sie selbst (löschen/hinzufügen über die UI)
        if conn.execute("SELECT COUNT(*) AS n FROM categories").fetchone()["n"] == 0:
            for cid, name, color, pos in [("trading", "Trading", "#6c63ff", 0),
                                          ("schule", "Schule", "#4dabf7", 1),
                                          ("sport", "Sport", "#2dd4bf", 2),
                                          ("privat", "Privat", "#ff8fa3", 3)]:
                conn.execute("INSERT INTO categories (id, name, color, position) VALUES (?,?,?,?) "
                             "ON CONFLICT (id) DO NOTHING", (cid, name, color, pos))
    conn.close()


def load_state():
    conn = get_db()
    todos = [dict(id=r["id"], text=r["text"], done=bool(r["done"]), date=r["date"], priority=r["priority"],
                  repeatType=r["repeat_type"], repeatWeekdays=json.loads(r["repeat_weekdays"] or "[]"),
                  repeatInterval=r["repeat_interval"], repeatSpawned=bool(r["repeat_spawned"]))
             for r in conn.execute("SELECT * FROM todos")]
    events = [dict(id=r["id"], title=r["title"], date=r["event_date"], endDate=r["end_date"], time=r["event_time"],
                   recurrence=r["recurrence"], interval=r["recurrence_interval"], priority=r["priority"],
                   category=r["category"], note=r["note"],
                   source=r["source"], icloudUid=r["icloud_uid"], exceptions=json.loads(r["exceptions"] or "[]"))
              for r in conn.execute("SELECT * FROM calendar_events")]
    categories = [dict(id=r["id"], name=r["name"], color=r["color"])
                  for r in conn.execute("SELECT * FROM categories ORDER BY position")]
    training_plan = {r["weekday"]: {"sessionType": r["session_type"], "description": r["description"],
                                     "options": json.loads(r["options"] or "[]")}
                      for r in conn.execute("SELECT * FROM training_plan")}
    training_log = [dict(date=r["date"], done=bool(r["done"]), choice=r["choice"])
                     for r in conn.execute("SELECT * FROM training_log")]
    journal_entries = [dict(id=r["id"], date=r["entry_date"], mood=r["mood"],
                             productivity=r["productivity"], note=r["note"])
                        for r in conn.execute("SELECT * FROM journal_entries ORDER BY entry_date DESC")]
    goals = [dict(id=r["id"], text=r["text"], createdDate=r["created_date"])
             for r in conn.execute("SELECT * FROM goals ORDER BY created_date")]
    assistant_log = [dict(id=r["id"], ts=r["ts"], role=r["role"], message=r["message"])
                      for r in conn.execute("SELECT * FROM assistant_log ORDER BY ts")]
    garmin_days = [dict(date=r["date"], sleepSeconds=r["sleep_seconds"], sleepScore=r["sleep_score"],
                         restingHr=r["resting_hr"], steps=r["steps"],
                         deepSleepSeconds=r["deep_sleep_seconds"], lightSleepSeconds=r["light_sleep_seconds"],
                         remSleepSeconds=r["rem_sleep_seconds"], awakeSleepSeconds=r["awake_sleep_seconds"],
                         vo2max=r["vo2max"], fitnessAge=r["fitness_age"])
                   for r in conn.execute("SELECT * FROM garmin_days ORDER BY date DESC LIMIT 370")]
    garmin_activities = [dict(id=r["activity_id"], startTime=r["start_time"], name=r["name"], type=r["type"],
                               durationSeconds=r["duration_seconds"], distanceM=r["distance_m"],
                               calories=r["calories"], avgHr=r["avg_hr"])
                          for r in conn.execute("SELECT * FROM garmin_activities ORDER BY start_time DESC LIMIT 20")]
    supplements_log = [dict(date=r["date"], done=bool(r["done"]))
                        for r in conn.execute("SELECT * FROM supplements_log ORDER BY date DESC LIMIT 370")]
    last_sync_row = conn.execute("SELECT value FROM sync_meta WHERE key='garmin_last_sync'").fetchone()
    fm_row = conn.execute("SELECT value FROM sync_meta WHERE key='fitness_metrics'").fetchone()
    conn.close()
    return {"todos": todos, "events": events, "categories": categories, "trainingPlan": training_plan, "trainingLog": training_log,
            "journalEntries": journal_entries, "goals": goals, "assistantLog": assistant_log,
            "garminDays": garmin_days, "garminActivities": garmin_activities, "supplementsLog": supplements_log,
            "fitnessMetrics": json.loads(fm_row["value"]) if fm_row else None,
            "garminLastSync": last_sync_row["value"] if last_sync_row else None}


def compute_next_repeat_date(from_date_str, repeat_type, repeat_weekdays, repeat_interval):
    """Naechstes Datum nach from_date_str fuer die gegebene Wiederholungsart, oder None."""
    from_date = datetime.date.fromisoformat(from_date_str)
    if repeat_type == "daily":
        return (from_date + datetime.timedelta(days=1)).isoformat()
    if repeat_type == "interval_days":
        n = max(1, int(repeat_interval or 1))
        return (from_date + datetime.timedelta(days=n)).isoformat()
    if repeat_type == "weekly_days" and repeat_weekdays:
        for i in range(1, 8):
            cand = from_date + datetime.timedelta(days=i)
            if WEEKDAYS[cand.weekday()] in repeat_weekdays:
                return cand.isoformat()
    return None


def spawn_next_todo_if_needed(conn, row):
    """Legt bei einem gerade erledigten, wiederkehrenden To-Do (falls noch nicht geschehen) die
    naechste Instanz an. row braucht: id, text, date, priority, repeat_type, repeat_weekdays (JSON-Str), repeat_interval."""
    if row["repeat_type"] == "none" or row["repeat_spawned"]:
        return
    next_date = compute_next_repeat_date(row["date"], row["repeat_type"],
                                          json.loads(row["repeat_weekdays"] or "[]"), row["repeat_interval"])
    if not next_date:
        return
    nid = "id" + uuid.uuid4().hex[:12]
    conn.execute("""INSERT INTO todos (id, text, done, date, priority, repeat_type, repeat_weekdays, repeat_interval, repeat_spawned)
        VALUES (?,?,0,?,?,?,?,?,0)""",
        (nid, row["text"], next_date, row["priority"], row["repeat_type"], row["repeat_weekdays"], row["repeat_interval"]))
    conn.execute("UPDATE todos SET repeat_spawned=1 WHERE id=?", (row["id"],))


def save_state(data):
    conn = get_db()
    with conn:
        conn.execute("DELETE FROM todos")
        for t in data.get("todos", []):
            conn.execute("""INSERT INTO todos
                (id, text, done, date, priority, repeat_type, repeat_weekdays, repeat_interval, repeat_spawned)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (t["id"], t["text"], int(bool(t["done"])), t["date"], t.get("priority", "none"),
                 t.get("repeatType", "none"), json.dumps(t.get("repeatWeekdays") or []),
                 t.get("repeatInterval"), int(bool(t.get("repeatSpawned")))))
        # Erledigte wiederkehrende To-Dos, die noch keine Folge-Instanz haben (z. B. weil sie gerade
        # per Checkbox abgehakt wurden), bekommen jetzt lazy ihre naechste Instanz — nicht alles im Voraus.
        for t in data.get("todos", []):
            if t.get("done") and t.get("repeatType", "none") != "none" and not t.get("repeatSpawned"):
                row = conn.execute(
                    "SELECT id,text,date,priority,repeat_type,repeat_weekdays,repeat_interval,repeat_spawned FROM todos WHERE id=?",
                    (t["id"],)).fetchone()
                if row:
                    spawn_next_todo_if_needed(conn, row)

        conn.execute("DELETE FROM calendar_events")
        for e in data.get("events", []):
            icloud_uid = e.get("icloudUid")
            # Neuer, nicht-wiederkehrender Dashboard-Termin ohne icloud_uid -> automatisch nach iCloud pushen
            # (wiederkehrende Termine bewusst ausgenommen, siehe project_life_dashboard.md Phase-3-Notiz)
            if not icloud_uid and e.get("source", "dashboard") == "dashboard" and e.get("recurrence", "none") == "none":
                try:
                    icloud_uid = caldav_push_event(e["title"], e["date"], e.get("time"), e.get("endDate"))
                except Exception as ex:
                    print(f"[save_state] iCloud-Push fuer '{e['title']}' fehlgeschlagen: {ex}")
            conn.execute("""INSERT INTO calendar_events
                (id, title, event_date, end_date, event_time, recurrence, recurrence_interval, priority, category, note, source, icloud_uid, exceptions)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (e["id"], e["title"], e["date"], e.get("endDate"), e.get("time"), e.get("recurrence", "none"),
                 int(e.get("interval", 1)), e.get("priority", "small"), e.get("category", "privat"),
                 e.get("note"), e.get("source", "dashboard"), icloud_uid, json.dumps(e.get("exceptions", []))))

        # Kategorien nur ersetzen, wenn das Frontend welche mitschickt — schützt davor,
        # dass ein noch offener Tab mit altem JavaScript die Liste leert
        cats = data.get("categories")
        if isinstance(cats, list) and cats:
            conn.execute("DELETE FROM categories")
            for i, c in enumerate(cats):
                conn.execute("INSERT INTO categories (id, name, color, position) VALUES (?,?,?,?)",
                             (c["id"], c["name"], c["color"], i))

        for day, entry in data.get("trainingPlan", {}).items():
            conn.execute("""INSERT INTO training_plan (weekday, session_type, description, options) VALUES (?,?,?,?)
                ON CONFLICT(weekday) DO UPDATE SET session_type=excluded.session_type,
                    description=excluded.description, options=excluded.options""",
                (day, entry.get("sessionType", ""), entry.get("description", ""),
                 json.dumps(entry.get("options", []))))

        conn.execute("DELETE FROM training_log")
        for l in data.get("trainingLog", []):
            conn.execute("INSERT INTO training_log (date, done, choice) VALUES (?,?,?)",
                         (l["date"], int(bool(l["done"])), l.get("choice")))

        conn.execute("DELETE FROM journal_entries")
        for j in data.get("journalEntries", []):
            conn.execute("INSERT INTO journal_entries (id, entry_date, mood, productivity, note) VALUES (?,?,?,?,?)",
                         (j["id"], j["date"], j["mood"], int(j.get("productivity", 0)), j.get("note", "")))

        conn.execute("DELETE FROM goals")
        for g in data.get("goals", []):
            conn.execute("INSERT INTO goals (id, text, created_date) VALUES (?,?,?)",
                         (g["id"], g["text"], g.get("createdDate", "")))

        conn.execute("DELETE FROM assistant_log")
        for a in data.get("assistantLog", []):
            conn.execute("INSERT INTO assistant_log (id, ts, role, message) VALUES (?,?,?,?)",
                         (a["id"], a["ts"], a["role"], a["message"]))

        conn.execute("DELETE FROM supplements_log")
        for s in data.get("supplementsLog", []):
            conn.execute("INSERT INTO supplements_log (date, done) VALUES (?,?)",
                         (s["date"], int(bool(s["done"]))))
    conn.close()


def today_str():
    return datetime.date.today().isoformat()


# ── PHASE 3: GARMIN-SYNC (Best-Effort, gecacht in garmin_days) ──────
def garmin_sync(days_back=3):
    """Holt Schlaf/Puls/Schritte der letzten `days_back` Tage von Garmin und cached sie lokal.
    Best-Effort: bei Fehlern (Login/Netzwerk/API-Aenderung) bleibt die letzte gecachte DB unveraendert."""
    if not GARMIN_EMAIL or not GARMIN_PASSWORD:
        return False, "Garmin-Zugangsdaten fehlen in .env"
    try:
        client = garminconnect.Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
        client.login()
    except Exception as e:
        return False, f"Garmin-Login fehlgeschlagen: {e}"

    conn = get_db()
    synced_days = []
    try:
        with conn:
            for i in range(days_back):
                day = (datetime.date.today() - datetime.timedelta(days=i)).isoformat()
                sleep_seconds = sleep_score = resting_hr = steps = None
                deep_sleep = light_sleep = rem_sleep = awake_sleep = None
                vo2max = fitness_age = None
                try:
                    sleep = client.get_sleep_data(day)
                    dto = (sleep or {}).get("dailySleepDTO") or {}
                    sleep_seconds = dto.get("sleepTimeSeconds")
                    sleep_score = (dto.get("sleepScores") or {}).get("overall", {}).get("value")
                    deep_sleep = dto.get("deepSleepSeconds")
                    light_sleep = dto.get("lightSleepSeconds")
                    rem_sleep = dto.get("remSleepSeconds")
                    awake_sleep = dto.get("awakeSleepSeconds")
                except Exception as e:
                    print(f"[garmin_sync] Schlafdaten {day} nicht abrufbar: {e}")
                try:
                    stats = client.get_stats(day)
                    resting_hr = stats.get("restingHeartRate")
                    steps = stats.get("totalSteps")
                except Exception as e:
                    print(f"[garmin_sync] Stats {day} nicht abrufbar: {e}")
                conn.execute("""INSERT INTO garmin_days
                        (date, sleep_seconds, sleep_score, resting_hr, steps, deep_sleep_seconds, light_sleep_seconds, rem_sleep_seconds, awake_sleep_seconds, vo2max, fitness_age, synced_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(date) DO UPDATE SET sleep_seconds=excluded.sleep_seconds, sleep_score=excluded.sleep_score,
                        resting_hr=excluded.resting_hr, steps=excluded.steps, deep_sleep_seconds=excluded.deep_sleep_seconds,
                        light_sleep_seconds=excluded.light_sleep_seconds, rem_sleep_seconds=excluded.rem_sleep_seconds,
                        awake_sleep_seconds=excluded.awake_sleep_seconds, vo2max=excluded.vo2max, fitness_age=excluded.fitness_age,
                        synced_at=excluded.synced_at""",
                    (day, sleep_seconds, sleep_score, resting_hr, steps, deep_sleep, light_sleep, rem_sleep,
                     awake_sleep, vo2max, fitness_age, datetime.datetime.now().isoformat()))
                synced_days.append(day)
            try:
                for a in client.get_activities(0, 15):
                    conn.execute("""INSERT INTO garmin_activities
                            (activity_id, start_time, name, type, duration_seconds, distance_m, calories, avg_hr)
                        VALUES (?,?,?,?,?,?,?,?)
                        ON CONFLICT(activity_id) DO UPDATE SET start_time=excluded.start_time, name=excluded.name,
                            type=excluded.type, duration_seconds=excluded.duration_seconds, distance_m=excluded.distance_m,
                            calories=excluded.calories, avg_hr=excluded.avg_hr""",
                        (str(a.get("activityId")), a.get("startTimeLocal"), a.get("activityName"),
                         ((a.get("activityType") or {}).get("typeKey")),
                         int(a.get("duration") or 0), a.get("distance"),
                         int(a.get("calories") or 0) or None,
                         int(a.get("averageHR") or 0) or None))
            except Exception as e:
                print(f"[garmin_sync] Aktivitaeten nicht abrufbar: {e}")
            try:
                # VO2max gibt es nur an Tagen mit GPS-Lauf — deshalb gezielt das Datum des letzten Laufs abfragen
                run_row = conn.execute("""SELECT start_time FROM garmin_activities
                    WHERE type LIKE '%running%' AND start_time IS NOT NULL
                    ORDER BY start_time DESC LIMIT 1""").fetchone()
                if run_row:
                    run_day = run_row["start_time"][:10]
                    mm = client.get_max_metrics(run_day)
                    gen = ((mm or [{}])[0] or {}).get("generic") or {}
                    vo2 = gen.get("vo2MaxPreciseValue") or gen.get("vo2MaxValue")
                    if vo2:
                        conn.execute("""INSERT INTO sync_meta (key, value) VALUES ('fitness_metrics', ?)
                            ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                            (json.dumps({"vo2max": vo2, "fitnessAge": gen.get("fitnessAge"), "date": run_day}),))
            except Exception as e:
                print(f"[garmin_sync] VO2max nicht abrufbar: {e}")
            conn.execute("""INSERT INTO sync_meta (key, value) VALUES ('garmin_last_sync', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value""", (datetime.datetime.now().isoformat(),))
    finally:
        conn.close()
    return True, f"{len(synced_days)} Tage synchronisiert"


def garmin_last_sync(conn):
    row = conn.execute("SELECT value FROM sync_meta WHERE key='garmin_last_sync'").fetchone()
    return datetime.datetime.fromisoformat(row["value"]) if row else None


def garmin_sync_if_stale(conn):
    last = garmin_last_sync(conn)
    if last is None or datetime.datetime.now() - last > GARMIN_SYNC_MIN_INTERVAL:
        ok, msg = garmin_sync()
        print(f"[garmin_sync] {msg}" if ok else f"[garmin_sync] Fehler: {msg}")


# ── PHASE 3: ICLOUD-KALENDER (CalDAV, Best-Effort) ───────────────────
def _caldav_calendar():
    if not APPLE_ID_EMAIL or not APPLE_APP_PASSWORD:
        raise RuntimeError("Apple-ID-Zugangsdaten fehlen in .env")
    client = caldav.DAVClient(url="https://caldav.icloud.com", username=APPLE_ID_EMAIL, password=APPLE_APP_PASSWORD)
    principal = client.principal()
    for cal in principal.calendars():
        if cal.name == ICLOUD_CALENDAR_NAME:
            return cal
    raise RuntimeError(f"Kalender '{ICLOUD_CALENDAR_NAME}' nicht gefunden")


def caldav_pending_events(conn):
    """Liefert iCloud-Termine (naechste 90 Tage, letzte 7 Tage), die noch nicht als calendar_events importiert wurden."""
    known_uids = {r["icloud_uid"] for r in conn.execute(
        "SELECT icloud_uid FROM calendar_events WHERE icloud_uid IS NOT NULL")}
    cal = _caldav_calendar()
    start = datetime.datetime.now() - datetime.timedelta(days=7)
    end = datetime.datetime.now() + datetime.timedelta(days=90)
    events = cal.search(start=start, end=end, event=True, expand=True)
    pending = []
    backfills = []
    for e in events:
        ical = icalendar.Calendar.from_ical(e.data)
        for comp in ical.walk("VEVENT"):
            uid = str(comp.get("uid"))
            dtstart = comp.get("dtstart").dt
            is_datetime = isinstance(dtstart, datetime.datetime)
            start_date = dtstart.date() if is_datetime else dtstart
            end_date = None
            dtend_prop = comp.get("dtend")
            if dtend_prop is not None:
                dtend = dtend_prop.dt
                if isinstance(dtend, datetime.datetime):
                    raw_end = dtend.date()
                else:
                    # Ganztags-DTEND ist exklusiv (Apple speichert den Tag NACH dem letzten Urlaubstag)
                    raw_end = dtend - datetime.timedelta(days=1)
                if raw_end > start_date:
                    end_date = raw_end.isoformat()
            if uid in known_uids:
                if end_date:
                    backfills.append((end_date, uid))
                continue
            pending.append({
                "icloudUid": uid,
                "title": str(comp.get("summary") or "(ohne Titel)"),
                "date": start_date.isoformat(),
                "endDate": end_date,
                "time": dtstart.strftime("%H:%M") if is_datetime else None,
            })
    if backfills:
        # Bereits importierte Termine nachträglich mit ihrem Enddatum versorgen (z. B. Slowenien 8.-16.8.)
        with conn:
            for end_date, uid in backfills:
                conn.execute("UPDATE calendar_events SET end_date=? WHERE icloud_uid=? AND (end_date IS NULL OR end_date='')",
                             (end_date, uid))
    return pending


def caldav_push_event(title, date, time, end_date=None):
    """Legt einen neuen Termin im iCloud-Hauptkalender an, gibt die neue icloud_uid zurueck."""
    cal = _caldav_calendar()
    uid = str(uuid.uuid4())
    ical = icalendar.Calendar()
    ical.add("prodid", "-//Life Dashboard//DE")
    ical.add("version", "2.0")
    vevent = icalendar.Event()
    vevent.add("uid", uid)
    vevent.add("summary", title)
    if time:
        h, m = map(int, time.split(":"))
        dt = datetime.datetime.combine(datetime.date.fromisoformat(date), datetime.time(h, m))
        vevent.add("dtstart", dt)
        if end_date:
            end_dt = datetime.datetime.combine(datetime.date.fromisoformat(end_date), datetime.time(h, m))
            vevent.add("dtend", end_dt + datetime.timedelta(hours=1))
        else:
            vevent.add("dtend", dt + datetime.timedelta(hours=1))
    else:
        d = datetime.date.fromisoformat(date)
        last_day = datetime.date.fromisoformat(end_date) if end_date else d
        vevent.add("dtstart", d)
        # DTEND ist bei Ganztags-Terminen exklusiv, deshalb +1 Tag nach dem letzten Tag
        vevent.add("dtend", last_day + datetime.timedelta(days=1))
    ical.add_component(vevent)
    cal.save_event(ical.to_ical().decode("utf-8"))
    return uid


# ── NEWS + MÄRKTE (Tagesschau-RSS + Yahoo Finance, gecacht in sync_meta) ──
def _http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _cache_raw(conn, key):
    row = conn.execute("SELECT value FROM sync_meta WHERE key=?", (key,)).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["value"])
    except json.JSONDecodeError:
        return None


def _cache_get(conn, key, ttl):
    """Gibt (payload, ist_noch_frisch) zurueck — alter Cache bleibt als Fallback nutzbar."""
    data = _cache_raw(conn, key)
    if not data:
        return None, False
    try:
        fetched = datetime.datetime.fromisoformat(data["fetched"])
    except (KeyError, ValueError):
        return None, False
    return data.get("payload"), datetime.datetime.now() - fetched <= ttl


def _cache_set(conn, key, payload):
    fetched_at = datetime.datetime.now().isoformat()
    with conn:
        conn.execute("""INSERT INTO sync_meta (key, value) VALUES (?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, json.dumps({"fetched": fetched_at, "payload": payload})))
    return fetched_at


def fetch_news():
    root = ET.fromstring(_http_get(NEWS_FEED_URL))
    items = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        desc = (it.findtext("description") or "").strip()
        link = (it.findtext("link") or "").strip()
        if not title or not desc:
            continue
        encoded = it.findtext("{http://purl.org/rss/1.0/modules/content/}encoded") or ""
        img = re.search(r'<img[^>]+src="(https://[^"]+)"', encoded)
        time_label = None
        try:
            pub = datetime.datetime.strptime((it.findtext("pubDate") or "")[:31], "%a, %d %b %Y %H:%M:%S %z")
            time_label = pub.astimezone().strftime("%H:%M")
        except ValueError:
            pass
        items.append({
            "title": html.unescape(title),
            "text": html.unescape(re.sub(r"<[^>]+>", "", desc)).strip(),
            "link": link,
            "image": img.group(1) if img else None,
            "time": time_label,
        })
        if len(items) >= 5:
            break
    return items


def fetch_markets():
    quotes = []
    for name, symbol in MARKET_SYMBOLS:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}?interval=15m&range=1d"
            result = json.loads(_http_get(url))["chart"]["result"][0]
            meta = result["meta"]
            price = meta.get("regularMarketPrice")
            prev = meta.get("chartPreviousClose") or meta.get("previousClose")
            closes = [c for c in ((result.get("indicators", {}).get("quote") or [{}])[0].get("close") or []) if c is not None]
            step = max(1, len(closes) // 24)
            quotes.append({
                "name": name,
                "price": price,
                "changePct": round((price / prev - 1) * 100, 2) if price and prev else None,
                "spark": [round(c, 2) for c in closes[::step][-24:]],
            })
        except Exception as e:
            print(f"[markets] {symbol} nicht abrufbar: {e}")
    return quotes


def cached_external(conn, key, ttl, fetcher, force=False):
    """Gibt (payload, fetchedAtIso) zurueck, damit das Frontend anzeigen kann, wann zuletzt echt abgerufen wurde."""
    payload, fresh = _cache_get(conn, key, ttl)
    if fresh and not force:
        raw = _cache_raw(conn, key)
        return payload, (raw or {}).get("fetched")
    try:
        payload = fetcher()
        fetched_at = _cache_set(conn, key, payload)
    except Exception as e:
        print(f"[{key}] Abruf fehlgeschlagen, nutze alten Cache: {e}")
        raw = _cache_raw(conn, key)
        fetched_at = (raw or {}).get("fetched")
    return payload or [], fetched_at


# ── KI-ASSISTENT: TOOLS ──────────────────────────────────────────────
def h_add_todo(conn, text, date=None, priority='none'):
    tid = 'id' + uuid.uuid4().hex[:12]
    d = date or today_str()
    with conn:
        conn.execute("INSERT INTO todos (id, text, done, date, priority) VALUES (?,?,0,?,?)",
                     (tid, text, d, priority))
    return f"To-Do angelegt: {text} ({d})"


def h_set_todo_done(conn, id, done=True):
    with conn:
        conn.execute("UPDATE todos SET done=? WHERE id=?", (int(bool(done)), id))
        if done:
            row = conn.execute(
                "SELECT id,text,date,priority,repeat_type,repeat_weekdays,repeat_interval,repeat_spawned FROM todos WHERE id=?",
                (id,)).fetchone()
            if row:
                spawn_next_todo_if_needed(conn, row)
    return f"To-Do als {'erledigt' if done else 'offen'} markiert"


def h_delete_todo(conn, id):
    with conn:
        conn.execute("DELETE FROM todos WHERE id=?", (id,))
    return "To-Do gelöscht"


def h_move_todo(conn, id, date):
    with conn:
        conn.execute("UPDATE todos SET date=? WHERE id=?", (date, id))
    return f"To-Do verschoben auf {date}"


def h_add_calendar_event(conn, title, date, time=None, end_date=None, recurrence='none', interval=1, priority='small', category='privat', note=None):
    eid = 'id' + uuid.uuid4().hex[:12]
    # Unbekannte Kategorie (z. B. von der KI erfunden oder inzwischen gelöscht) -> Privat bzw. erste vorhandene
    if not conn.execute("SELECT id FROM categories WHERE id=?", (category,)).fetchone():
        fb = (conn.execute("SELECT id FROM categories WHERE id='privat'").fetchone()
              or conn.execute("SELECT id FROM categories ORDER BY position LIMIT 1").fetchone())
        category = fb["id"] if fb else "privat"
    with conn:
        conn.execute("""INSERT INTO calendar_events
            (id, title, event_date, end_date, event_time, recurrence, recurrence_interval, priority, category, note, source, icloud_uid, exceptions)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (eid, title, date, end_date if end_date and end_date > date else None, time,
             recurrence, int(interval), priority, category, note, 'dashboard', None, '[]'))
    suffix = f" bis {end_date}" if end_date and end_date > date else (f" um {time}" if time else "")
    return f"Termin angelegt: {title} am {date}{suffix}"


def h_delete_calendar_event(conn, id):
    with conn:
        conn.execute("DELETE FROM calendar_events WHERE id=?", (id,))
    return "Termin gelöscht"


def h_set_training_plan(conn, weekday, session_type, description='', options=None):
    with conn:
        conn.execute("""INSERT INTO training_plan (weekday, session_type, description, options) VALUES (?,?,?,?)
            ON CONFLICT(weekday) DO UPDATE SET session_type=excluded.session_type,
                description=excluded.description, options=excluded.options""",
            (weekday, session_type, description, json.dumps(options or [])))
    return f"Trainingsplan {weekday} gesetzt: {session_type}"


def h_set_training_log(conn, date, done=True, choice=None):
    with conn:
        conn.execute("""INSERT INTO training_log (date, done, choice) VALUES (?,?,?)
            ON CONFLICT(date) DO UPDATE SET done=excluded.done, choice=excluded.choice""",
            (date, int(bool(done)), choice))
    suffix = f" ({choice})" if choice else ""
    return f"Training am {date} als {'erledigt' if done else 'offen'} markiert{suffix}"


def h_add_goal(conn, text):
    gid = 'id' + uuid.uuid4().hex[:12]
    with conn:
        conn.execute("INSERT INTO goals (id, text, created_date) VALUES (?,?,?)", (gid, text, today_str()))
    return f"Ziel hinzugefügt: {text}"


def h_delete_goal(conn, id):
    with conn:
        conn.execute("DELETE FROM goals WHERE id=?", (id,))
    return "Ziel gelöscht"


def h_add_journal_entry(conn, mood, productivity=0, note=''):
    jid = 'id' + uuid.uuid4().hex[:12]
    with conn:
        conn.execute("INSERT INTO journal_entries (id, entry_date, mood, productivity, note) VALUES (?,?,?,?,?)",
                     (jid, today_str(), mood, int(productivity), note))
    return "Tagebuch-Eintrag gespeichert"


TOOLS = {
    "add_todo": {
        "description": "Legt einen neuen Eintrag auf der To-Do-Liste an.",
        "schema": {"type": "object", "properties": {
            "text": {"type": "string", "description": "Inhalt des To-Dos"},
            "date": {"type": "string", "description": "Datum JJJJ-MM-TT, Standard: heute"},
            "priority": {"type": "string", "enum": ["none", "small", "big"], "description": "big = wichtig"},
        }, "required": ["text"]},
        "handler": h_add_todo,
    },
    "set_todo_done": {
        "description": "Markiert ein bestehendes To-Do (per id aus dem Kontext) als erledigt oder offen.",
        "schema": {"type": "object", "properties": {
            "id": {"type": "string"}, "done": {"type": "boolean"},
        }, "required": ["id", "done"]},
        "handler": h_set_todo_done,
    },
    "delete_todo": {
        "description": "Löscht ein bestehendes To-Do (per id aus dem Kontext).",
        "schema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]},
        "handler": h_delete_todo,
    },
    "move_todo": {
        "description": "Verschiebt ein bestehendes To-Do (per id aus dem Kontext) auf ein anderes Datum.",
        "schema": {"type": "object", "properties": {
            "id": {"type": "string"}, "date": {"type": "string", "description": "Neues Datum JJJJ-MM-TT"},
        }, "required": ["id", "date"]},
        "handler": h_move_todo,
    },
    "add_calendar_event": {
        "description": "Legt einen neuen Kalendertermin an, optional mehrtägig mit Enddatum (z. B. Urlaub von-bis).",
        "schema": {"type": "object", "properties": {
            "title": {"type": "string"},
            "date": {"type": "string", "description": "Datum JJJJ-MM-TT (bei mehrtägigen Terminen der erste Tag)"},
            "end_date": {"type": "string", "description": "Letzter Tag JJJJ-MM-TT bei mehrtägigen Terminen, optional"},
            "time": {"type": "string", "description": "Uhrzeit HH:MM, optional"},
            "recurrence": {"type": "string", "enum": ["none", "daily", "weekly", "monthly", "yearly"]},
            "interval": {"type": "integer", "description": "alle N Einheiten, Standard 1"},
            "priority": {"type": "string", "enum": ["small", "big"]},
            "category": {"type": "string", "description": "Kategorie-id — eine aus 'kategorien' im Kontext, Standard: privat"},
            "note": {"type": "string", "description": "Notiz zum Termin (Link, Adresse, Details), optional"},
        }, "required": ["title", "date"]},
        "handler": h_add_calendar_event,
    },
    "delete_calendar_event": {
        "description": "Löscht einen bestehenden Kalendertermin (per id aus dem Kontext).",
        "schema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]},
        "handler": h_delete_calendar_event,
    },
    "set_training_plan": {
        "description": "Setzt den Trainingsplan-Eintrag für einen Wochentag, optional mit mehreren auswählbaren Optionen (z. B. bei Cardio: Laufen/Schwimmen).",
        "schema": {"type": "object", "properties": {
            "weekday": {"type": "string", "enum": WEEKDAYS},
            "session_type": {"type": "string", "description": "z. B. Push, Beine, Ruhetag"},
            "description": {"type": "string"},
            "options": {"type": "array", "items": {"type": "string"},
                        "description": "Auswählbare Unter-Optionen für diesen Tag, z. B. [\"Laufen\",\"Schwimmen\"]"},
        }, "required": ["weekday", "session_type"]},
        "handler": h_set_training_plan,
    },
    "set_training_log": {
        "description": "Markiert ein Trainingsdatum als erledigt oder offen, optional mit welcher Option gemacht wurde.",
        "schema": {"type": "object", "properties": {
            "date": {"type": "string", "description": "Datum JJJJ-MM-TT, Standard: heute"}, "done": {"type": "boolean"},
            "choice": {"type": "string", "description": "Welche Option gewählt wurde, z. B. Laufen"},
        }, "required": ["date", "done"]},
        "handler": h_set_training_log,
    },
    "add_goal": {
        "description": "Fügt ein langfristiges Ziel hinzu.",
        "schema": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
        "handler": h_add_goal,
    },
    "delete_goal": {
        "description": "Löscht ein bestehendes Ziel (per id aus dem Kontext).",
        "schema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]},
        "handler": h_delete_goal,
    },
    "add_journal_entry": {
        "description": "Speichert einen Tagebuch-Eintrag für heute.",
        "schema": {"type": "object", "properties": {
            "mood": {"type": "string", "description": "z. B. ein Emoji oder Stimmungswort"},
            "productivity": {"type": "integer", "description": "0-10"},
            "note": {"type": "string"},
        }, "required": ["mood"]},
        "handler": h_add_journal_entry,
    },
}


def execute_tool(conn, name, args):
    tool = TOOLS.get(name)
    if not tool:
        return f"Unbekannte Funktion: {name}"
    try:
        return tool["handler"](conn, **args)
    except Exception as e:
        return f"Fehler bei {name}: {e}"


def to_anthropic_tools():
    return [{"name": k, "description": v["description"], "input_schema": v["schema"]} for k, v in TOOLS.items()]


def to_gemini_tools():
    return [{"function_declarations": [
        {"name": k, "description": v["description"], "parameters": v["schema"]} for k, v in TOOLS.items()
    ]}]


# ── KI-ASSISTENT: KONTEXT + PROMPT ───────────────────────────────────
KIRILL_PROFILE = (
    "Kirill Bojko, 19 Jahre, Azubi bis Januar 2027. Tradet NAS100 nach ICT-Strategie, "
    "hat ein Lucid-Prop-Konto (50.000$). Baut nebenbei ein KI-Business auf. Kein Programmierhintergrund. "
    "Nimmt täglich Supplements (die genaue Liste trägt er noch nach)."
)


def gather_context(conn):
    today = datetime.date.today()
    near = (today + datetime.timedelta(days=3)).isoformat()
    far = (today + datetime.timedelta(days=13)).isoformat()
    todos = [dict(r) for r in conn.execute(
        "SELECT id,text,done,date,priority FROM todos WHERE date>=? AND date<=? ORDER BY date",
        (today.isoformat(), near))]
    events = [dict(r) for r in conn.execute(
        "SELECT id,title,event_date,event_time,category,priority FROM calendar_events WHERE event_date>=? AND event_date<=? ORDER BY event_date",
        (today.isoformat(), far))]
    plan_row = conn.execute("SELECT session_type, description FROM training_plan WHERE weekday=?",
                             (WEEKDAYS[today.weekday()],)).fetchone()
    goals = [dict(r) for r in conn.execute("SELECT id,text FROM goals ORDER BY created_date")]
    journal = [dict(r) for r in conn.execute(
        "SELECT entry_date,mood,productivity,note FROM journal_entries ORDER BY entry_date DESC LIMIT 5")]
    supp_row = conn.execute("SELECT done FROM supplements_log WHERE date=?", (today.isoformat(),)).fetchone()
    cats = [dict(r) for r in conn.execute("SELECT id,name FROM categories ORDER BY position")]
    return {
        "heute": today.isoformat(),
        "wochentag": WEEKDAYS[today.weekday()],
        "kategorien": cats,
        "todos_naechste_tage": todos,
        "termine_naechste_2_wochen": events,
        "training_heute": dict(plan_row) if plan_row else None,
        "supplements_heute_genommen": bool(supp_row["done"]) if supp_row else False,
        "ziele": goals,
        "letzte_tagebucheintraege": journal,
    }


def build_system_prompt(role, context):
    role_note = (
        "Du bist für schnelle, einfache Aktionen zuständig (To-Do anlegen, Termin eintragen, kurze Fragen)."
        if role == 'gemini' else
        "Du bist für tiefere Gespräche zuständig: Tages-/Wochen-Check-ins, Muster über mehrere Wochen erkennen, "
        "einschätzen ob Kirill sich Richtung seiner Ziele bewegt oder nicht, und das ansprechen wenn nicht."
    )
    return (
        f"Du bist der Sprachassistent in Kirills Life Dashboard. {KIRILL_PROFILE} {role_note}\n"
        f"Heute ist {context['wochentag']}, {context['heute']}.\n"
        f"Aktueller Kontext als JSON: {json.dumps(context, ensure_ascii=False)}\n"
        "Nutze die verfügbaren Funktionen, wenn Kirill etwas im Dashboard ändern will "
        "(To-Do, Termin, Trainingsplan, Ziel, Tagebuch). Wenn er etwas Bestehendes meint, such die passende "
        "id im Kontext oben, statt nachzufragen wenn es eindeutig ist.\n"
        "Deine Antwort wird laut vorgelesen: kurz und natürlich sprechen, maximal 2-3 Sätze, "
        "keine Bullet-Points, keine Sternchen, keine Emojis, kein Markdown."
    )


def pick_model(message):
    low = message.lower()
    action_verbs = ["leg ", "füg", "erstell", "lösch", "verschieb", "markier", "hak", "änder", "trag ein",
                    "setz", "streich"]
    if any(v in low for v in action_verbs):
        return 'gemini'
    reflect_triggers = ["woche", "wochen", "muster", "fühl", "stimmung", "insgesamt", "letzte zeit",
                         "letzten zeit", "ziel", "ziele", "check-in", "checkin", "wie läuft", "wie lief"]
    return 'claude' if any(t in low for t in reflect_triggers) else 'gemini'


# ── KI-ASSISTENT: API-AUFRUFE (urllib, keine SDKs nötig) ─────────────
def _http_post_json(url, headers, payload, timeout=30):
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} von {url}: {detail[:300]}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Keine Verbindung zu {url}: {e}")


def _anthropic_request(system_prompt, messages):
    return _http_post_json(
        "https://api.anthropic.com/v1/messages",
        {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        {"model": CLAUDE_MODEL, "max_tokens": 600, "system": system_prompt, "messages": messages,
         "tools": to_anthropic_tools()},
    )


def _gemini_request(system_prompt, contents):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    return _http_post_json(
        url, {"content-type": "application/json"},
        {"system_instruction": {"parts": [{"text": system_prompt}]}, "contents": contents,
         "tools": to_gemini_tools(), "generationConfig": {"thinkingConfig": {"thinkingBudget": 0}}},
    )


def _pcm_to_wav_b64(pcm_bytes, sample_rate=24000, sample_width=2, channels=1):
    """Gemini liefert rohes PCM ohne Dateikopf — der Browser kann das nicht abspielen.
    Baut mit Pythons Bordmittel-`wave`-Modul einen minimalen WAV-Header drumherum."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(channels)
        wf.setsampwidth(sample_width)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_bytes)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def synthesize_speech_b64(text):
    """Vertont die Antwort ueber Gemini (nativ, gleicher Key wie fuer den Textteil — keine neue
    Anmeldung noetig). Best-Effort: fehlt der Key oder schlaegt der Aufruf fehl, gibt es None
    zurueck — das Frontend faellt dann auf die Browser-Sprachausgabe zurueck."""
    if not GEMINI_API_KEY:
        return None
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_TTS_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": text}]}],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": GEMINI_TTS_VOICE}}},
        },
    }
    try:
        resp = _http_post_json(url, {"content-type": "application/json"}, payload)
        inline = resp["candidates"][0]["content"]["parts"][0]["inlineData"]
        pcm_bytes = base64.b64decode(inline["data"])
        return _pcm_to_wav_b64(pcm_bytes)
    except Exception as e:
        print(f"[Gemini-TTS] Sprachausgabe fehlgeschlagen, Browser-Fallback wird genutzt: {e}")
        return None


def call_claude(conn, system_prompt, user_message):
    messages = [{"role": "user", "content": user_message}]
    actions = []
    for _ in range(4):
        resp = _anthropic_request(system_prompt, messages)
        content = resp.get("content", [])
        text_parts = [b["text"] for b in content if b.get("type") == "text"]
        tool_uses = [b for b in content if b.get("type") == "tool_use"]
        if not tool_uses:
            return {"reply": " ".join(text_parts).strip() or "Alles klar.", "actions": actions}
        messages.append({"role": "assistant", "content": content})
        results = []
        for tu in tool_uses:
            summary = execute_tool(conn, tu["name"], tu.get("input", {}))
            actions.append(summary)
            results.append({"type": "tool_result", "tool_use_id": tu["id"], "content": summary})
        messages.append({"role": "user", "content": results})
    return {"reply": "Ich hab das erledigt, komm aber gerade zu keiner runden Antwort mehr.", "actions": actions}


def call_gemini(conn, system_prompt, user_message):
    contents = [{"role": "user", "parts": [{"text": user_message}]}]
    actions = []
    for _ in range(4):
        resp = _gemini_request(system_prompt, contents)
        candidates = resp.get("candidates", [])
        if not candidates:
            return {"reply": "Keine Antwort von Gemini bekommen.", "actions": actions}
        parts = candidates[0].get("content", {}).get("parts", [])
        fn_calls = [p["functionCall"] for p in parts if "functionCall" in p]
        text_parts = [p["text"] for p in parts if "text" in p]
        if not fn_calls:
            return {"reply": " ".join(text_parts).strip() or "Alles klar.", "actions": actions}
        contents.append({"role": "model", "parts": parts})
        response_parts = []
        for fc in fn_calls:
            summary = execute_tool(conn, fc["name"], fc.get("args", {}))
            actions.append(summary)
            fr = {"name": fc["name"], "response": {"result": summary}}
            if fc.get("id"):
                fr["id"] = fc["id"]
            response_parts.append({"functionResponse": fr})
        contents.append({"role": "user", "parts": response_parts})
    return {"reply": "Ich hab das erledigt, komm aber gerade zu keiner runden Antwort mehr.", "actions": actions}


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth")

    def _authed(self):
        if not PAGE_PASSWORD:  # lokal ohne gesetztes Passwort: kein Login-Zwang
            return True
        return hmac.compare_digest(self.headers.get("X-Auth", ""), AUTH_TOKEN)

    def _json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        try:
            return json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return None

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._authed():
            self._json(401, {"error": "nicht angemeldet"})
            return
        split = urllib.parse.urlsplit(self.path)
        path = split.path
        qs = urllib.parse.parse_qs(split.query)
        force = qs.get("force", ["0"])[0] == "1"
        if path == "/state":
            try:
                conn = get_db()
                garmin_sync_if_stale(conn)
                conn.close()
                self._json(200, load_state())
            except Exception as e:
                print(f"[/state GET] Fehler: {e}")
                self._json(500, {"error": "konnte nicht laden", "detail": str(e)})
        elif path == "/icloud/pending":
            conn = get_db()
            try:
                self._json(200, {"pending": caldav_pending_events(conn)})
            except Exception as e:
                print(f"[/icloud/pending] Fehler: {e}")
                self._json(200, {"pending": [], "error": str(e)})
            finally:
                conn.close()
        elif path == "/garmin/refresh":
            ok, msg = garmin_sync()
            self._json(200 if ok else 500, {"ok": ok, "message": msg})
        elif path == "/news":
            conn = get_db()
            try:
                items, fetched_at = cached_external(conn, "news_cache", NEWS_CACHE_TTL, fetch_news, force=force)
                self._json(200, {"items": items, "fetchedAt": fetched_at})
            finally:
                conn.close()
        elif path == "/markets":
            conn = get_db()
            try:
                quotes, fetched_at = cached_external(conn, "markets_cache", MARKETS_CACHE_TTL, fetch_markets, force=force)
                self._json(200, {"quotes": quotes, "fetchedAt": fetched_at})
            finally:
                conn.close()
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/login":
            data = self._read_json()
            given = (data or {}).get("password", "")
            if PAGE_PASSWORD and hmac.compare_digest(given, PAGE_PASSWORD):
                self._json(200, {"token": AUTH_TOKEN})
            else:
                self._json(401, {"error": "falsches Passwort"})
            return
        if not self._authed():
            self._json(401, {"error": "nicht angemeldet"})
            return
        if self.path == "/state":
            data = self._read_json()
            if data is None:
                self._json(400, {"error": "invalid json"})
                return
            try:
                save_state(data)
            except Exception as e:
                print(f"[/state POST] Fehler: {e}")
                self._json(500, {"error": "konnte nicht speichern", "detail": str(e)})
                return
            self._json(200, {"ok": True})
        elif self.path == "/assistant":
            data = self._read_json()
            if data is None:
                self._json(400, {"error": "invalid json"})
                return
            self._handle_assistant(data)
        else:
            self._json(404, {"error": "not found"})

    def _handle_assistant(self, data):
        message = (data or {}).get("message", "").strip()
        if not message:
            self._json(400, {"error": "leere Nachricht"})
            return
        if not GEMINI_API_KEY or not ANTHROPIC_API_KEY:
            self._json(500, {"error": "API-Key fehlt in .env"})
            return
        conn = get_db()
        try:
            context = gather_context(conn)
            model = pick_model(message)
            system_prompt = build_system_prompt(model, context)
            result = call_claude(conn, system_prompt, message) if model == 'claude' \
                else call_gemini(conn, system_prompt, message)
            now = datetime.datetime.now().isoformat()
            with conn:
                conn.execute("INSERT INTO assistant_log (id, ts, role, message) VALUES (?,?,?,?)",
                             ('id' + uuid.uuid4().hex[:12], now, 'user', message))
                conn.execute("INSERT INTO assistant_log (id, ts, role, message) VALUES (?,?,?,?)",
                             ('id' + uuid.uuid4().hex[:12], now, 'assistant', result['reply']))
            audio_b64 = synthesize_speech_b64(result["reply"])
            self._json(200, {"reply": result["reply"], "model": model, "actions": result["actions"], "audioBase64": audio_b64})
        except Exception as e:
            print(f"[/assistant] Fehler: {e}")
            self._json(500, {"error": "Der KI-Anbieter hat gerade nicht geantwortet. Versuch's gleich nochmal."})
        finally:
            conn.close()

    def log_message(self, fmt, *args):
        pass


def main():
    init_db()
    if GARMIN_EMAIL and GARMIN_PASSWORD:
        ok, msg = garmin_sync()
        print(f"[Garmin] {msg}" if ok else f"[Garmin] Sync beim Start fehlgeschlagen (nicht schlimm, laeuft mit Cache weiter): {msg}")
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Life Dashboard Server läuft auf http://localhost:{PORT}")
    print(f"Datenbank: Postgres ({DATABASE_URL.split('@')[-1] if DATABASE_URL else 'nicht konfiguriert'})")
    print("Lass dieses Fenster offen, solange du das Dashboard benutzt. Beenden: Strg+C")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer gestoppt.")


if __name__ == "__main__":
    main()
