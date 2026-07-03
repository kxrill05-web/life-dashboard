#!/bin/bash
# Life Dashboard Launcher
# Startet den lokalen Server und öffnet die Website im Browser.
# Terminal-Fenster offen lassen — solange es offen ist, läuft der Server.

cd "$(dirname "$0")"

echo "════════════════════════════════════"
echo "  Life Dashboard wird gestartet ..."
echo "════════════════════════════════════"
echo ""
echo "Website öffnet sich gleich im Browser."
echo "Dieses Fenster BITTE OFFEN LASSEN — wenn du es schließt, geht der Server aus."
echo ""

open "index.html"

python3 server.py
