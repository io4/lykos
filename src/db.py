import src.settings as var
import sqlite3
import os
import json

# increment this whenever making a schema change so that the schema upgrade functions run on start
# they do not run by default for performance reasons
SCHEMA_VERSION = 1

conn = None

def init():
    need_install = not os.path.isfile("data.sqlite3")
    conn = sqlite3.connect("data.sqlite3")
    with conn:
        c = conn.cursor()
        c.execute("PRAGMA foreign_keys = ON")
        if need_install:
            _install()
        c.execute("PRAGMA user_version")
        row = c.fetchone()
        if row[0] == 0:
            # new schema does not exist yet, migrate from old schema
            # NOTE: game stats are NOT migrated to the new schema; the old gamestats table
            # will continue to exist to allow queries against it, however given how horribly
            # inaccurate the stats on it are, it would be a disservice to copy those inaccurate
            # statistics over to the new schema which has the capability of actually being accurate.
            _migrate()
        elif row[0] < SCHEMA_VERSION:
            _upgrade()

        c.execute("""SELECT
                       pl.account,
                       pl.hostmask,
                       pe.notice,
                       pe.simple,
                       pe.deadchat,
                       pe.pingif,
                       pe.stasis_amount,
                       pe.stasis_expires
                     FROM person pe
                     JOIN person_player pp
                       ON pp.person = pe.id
                     JOIN player pl
                       ON pl.id = pp.player
                     WHERE pl.active = 1""")
        for (acc, host, notice, simple, dc, pi, stasis, stasisexp) in c:
            if acc is not None:
                var.SIMPLE_NOTIFY_ACCS.add(acc) if simple == 1
                var.PREFER_NOTICE_ACCS.add(acc) if notice == 1
                var.STASISED_ACCS[acc] = stasis if stasis > 0
                var.PING_IF_PREFS_ACCS[acc] = pi if pi > 0
                var.PING_IF_NUMS_ACCS[pi].add(acc) if pi > 0
                var.DEADCHAT_PREFS_ACCS.add(acc) if dc == 1
            elif host is not None:
                var.SIMPLE_NOTIFY.add(host) if simple == 1
                var.PREFER_NOTICE.add(host) if notice == 1
                var.STASISED[host] = stasis if stasis > 0
                var.PING_IF_PREFS[host] = pi if pi > 0
                var.PING_IF_NUMS[pi].add(host) if pi > 0
                var.DEADCHAT_PREFS.add(host) if dc == 1

def toggle_simple(acc, hostmask):
    _toggle_thing("simple", acc, hostmask)

def toggle_notice(acc, hostmask):
    _toggle_thing("notice", acc, hostmask)

def toggle_deadchat(acc, hostmask):
    _toggle_thing("deadchat", acc, hostmask)

def set_pingif(val, acc, hostmask):
    _set_thing("pingif", val, acc, hostmask, raw=False)

def set_stasis(val, acc, hostmask):
    _set_thing("stasis_amount", val, acc, hostmask, raw=False)

def add_game(mode, size, started, finished, winner, players, options):
    """ Adds a game record to the database.

    mode: Game mode (string)
    size: Game size on start (int)
    started: Time when game started (timestamp)
    finished: Time when game ended (timestamp)
    winner: Winning team (string)
    players: List of players (sequence of dict, described below)
    options: Game options (role reveal, stats type, etc., freeform dict)

    Players dict format:
    {
        nick: "Nickname"
        account: "Account name" (or None, "*" is converted to None)
        ident: "Ident"
        host: "Host"
        role: "role name"
        templates: ["template names", ...]
        special: ["special qualities", ... (lover, entranced, etc.)]
        won: True/False
        iwon: True/False
        dced: True/False
    }
    """

    if mode == "roles":
        # Do not record stats for games with custom roles
        return

    # Normalize players dict
    for p in players:
        if p["account"] == "*":
            p["account"] = None
        p["hostmask"] = "{0}!{1}@{2}".format(p["nick"], p["ident"], p["host"])
        c = conn.cursor()
        p["personid"], p["playerid"] = _get_ids(p["account"], p["hostmask"])

    with conn:
        c = conn.cursor()
        if winner.startswith("@"):
            # fool won, convert the nick portion into a player id
            for p in players:
                if p["nick"] == winner[1:]:
                    winner = "@" + p["playerid"]
                    break
            else:
                # invalid winner? We can't find the fool's nick in the player list
                # maybe raise an exception here instead of silently failing
                return

        c.execute("""INSERT INTO game (gamemode, options, started, finished, gamesize, winner)
                     VALUES (?, ?, ?, ?, ?, ?)""", (mode, json.dumps(options), started, finished, size, winner))
        gameid = c.lastrowid
        for p in players:
            c.execute("""INSERT INTO game_player (game, player, team_win, indiv_win, dced)
                         VALUES (?, ?, ?, ?, ?)""", (gameid, p["playerid"], p["won"], p["iwon"], p["dced"]))
            gpid = c.lastrowid
            c.execute("""INSERT INTO game_player_role (game_player, role, special)
                         VALUES (?, ?, 0)""", (gpid, p["role"]))
            for tpl in p["templates"]:
                c.execute("""INSERT INTO game_player_role (game_player, role, special)
                             VALUES (?, ?, 0)""", (gpid, tpl))
            for sq in p["special"]:
                c.execute("""INSERT INTO game_player_role (game_player, role, special)
                             VALUES (?, ?, 1)""", (gpid, sq))

def get_player_stats(acc, hostmask, role):
    peid, plid = _get_ids(acc, hostmask)
    if not _total_games(peid):
        return "\u0002{0}\u0002 has not played any games.".format(acc if acc and acc != "*" else hostmask)
    c = conn.cursor()
    c.execute("""SELECT
                   gpr.role AS role
                   SUM(gp.team_win) AS team,
                   SUM(gp.indiv_win) AS indiv,
                   COUNT(1) AS total
                 FROM person pe
                 JOIN person_player pmap
                   ON pmap.person = pe.id
                 JOIN game_player gp
                   ON gp.player = pmap.player
                 JOIN game_player_role gpr
                   ON gpr.game_player = gp.id
                   AND gpr.role = ?
                 WHERE pe.id = ?
                 GROUP BY role""", (role, peid))
    row = c.fetchone()
    name = _get_display_name(peid)
    if row:
        msg = "\u0002{0}\u0002 as \u0002{1}\u0002 | Team wins: {2} (%d%%), Individual wins: {3} (%d%%), Total games: {4}.".format(name, *row)
        return msg % (round(row[1]/row[3] * 100), round(row[2]/row[3] * 100))
    return "No stats for \u0002{0}\u0002 as \u0002{1}\u0002.".format(name, role)

def get_player_totals(acc, hostmask):
    peid, plid = _get_ids(acc, hostmask)
    total_games = _total_games(peid)
    if not total_games:
        return "\u0002{0}\u0002 has not played any games.".format(acc if acc and acc != "*" else hostmask)
    c = conn.cursor()
    c.execute("""SELECT
                   gpr.role AS role,
                   COUNT(1) AS total
                 FROM person pe
                 JOIN person_player pmap
                   ON pmap.person = pe.id
                 JOIN game_player gp
                   ON gp.player = pmap.player
                 JOIN game_player_role gpr
                   ON gpr.game_player = gp.id
                 WHERE pe.id = ?
                 GROUP BY role""", (peid,))
    tmp = {}
    totals = []
    for row in c:
        tmp[row[0]] = row[1]
    order = var.role_order()
    name = _get_display_name(peid)
    #ordered role stats
    totals = ["\u0002{0}\u0002: {1}".format(r, tmp[r]) for r in order if r in tmp]
    #lover or any other special stats
    totals += ["\u0002{0}\u0002: {1}".format(r, t) for r, t in tmp.items() if r not in order]
    return "\u0002{0}\u0002's totals | \u0002{1}\u0002 games | {2}".format(name, total_games, break_long_message(totals, ", "))

def get_game_stats(mode, size):
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM games WHERE gamemode = ? AND gamesize = ?", (mode, size))
    total_games = c.fetchone()[0]
    if not total_games:
        return "No stats for \u0002{0}\u0002 player games.".format(size)
    c.execute("""SELECT
                   CASE substr(winner, 1, 1)
                     WHEN '@' THEN 'fools'
                     ELSE winner END AS team,
                   COUNT(1) AS games,
                   CASE winner
                     WHEN 'villagers' THEN 0,
                     WHEN 'wolves' THEN 1,
                     ELSE 2 END AS ord
                 FROM game
                 WHERE
                   gamemode = ?
                   AND gamesize = ?
                   AND winner IS NOT NULL
                 GROUP BY team
                 ORDER BY ord ASC, team ASC""", (mode, size))
    msg = "\u0002{0}\u0002 player games | {1}"
    bits = []
    for row in c:
        bits.append("%s wins: %d (%d%%)" % (var.singular(row[0]), row[1], round(row[1]/total_games * 100)))
    bits.append("total games: {0}".format(total_games))
    return msg.format(size, ", ".join(bits))

def get_game_totals(mode):
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM games WHERE gamemode = ?", (mode,))
    total_games = c.fetchone()[0]
    if not total_games:
        return "No games have been played in the {0} game mode.".format(mode)
    c.execute("""SELECT
                   gamesize,
                   COUNT(1) AS games
                 FROM game
                 WHERE gamemode = ?
                 GROUP BY gamesize
                 ORDER BY gamesize ASC""", (mode,))
    totals = []
    for row in c:
        totals.append("\u0002{0}p\u0002: {1}".format(*row))
    return "Total games ({0}) | {1}".format(total_games, ", ".join(totals))

def _upgrade():
    # no upgrades yet, once there are some, add methods like _add_table(), _add_column(), etc.
    # that check for the existence of that table/column/whatever and adds/drops/whatevers them
    # as needed. We can't do this purely in SQL because sqlite lacks a scripting-level IF statement.
    pass

def _migrate():
    with conn, open("db.sql", "rt") as f1, open("migrate.sql", "rt") as f2:
        c = conn.cursor()
        #######################################################
        # Step 1: install the new schema (from db.sql script) #
        #######################################################
        c.executescript(f1.read())

        ################################################################
        # Step 2: migrate relevant info from the old schema to the new #
        ################################################################
        c.executescript(f2.read())

        ######################################################################
        # Step 3: Indicate we have updated the schema to the current version #
        ######################################################################
        c.execute("PRAGMA user_version = ?", (SCHEMA_VERSION,))

def _install():
    with conn, open("db.sql", "rt") as f1:
        c = conn.cursor()
        c.executescript(f1.read())
        c.execute("PRAGMA user_version = ?", (SCHEMA_VERSION,))

def _get_ids(acc, hostmask):
    c = conn.cursor()
    if acc is None or acc == "*":
        c.execute("""SELECT pe.id, pl.id
                     FROM player pl
                     JOIN person_player pp
                       ON pp.player = pl.id
                     JOIN person pe
                       ON pe.id = pp.person
                     WHERE
                       pl.account IS NULL
                       AND pl.hostmask = ?
                       AND pl.active = 1""", (hostmask,))
    else:
        c.execute("""SELECT pe.id, pl.id
                     FROM player pl
                     JOIN person_player pp
                       ON pp.player = pl.id
                     JOIN person pe
                       ON pe.id = pp.person
                     WHERE
                       pl.account = ?
                       AND pl.hostmask IS NULL
                       AND pl.active = 1""", (acc,))
    row = c.fetchone()
    if row:
        return row
    return (None, None)

def _get_display_name(peid):
    if peid is None:
        return None
    c = conn.cursor()
    c.execute("""SELECT COALESCE(pp.account, pp.hostmask)
                 FROM person pe
                 JOIN player pp
                   ON pp.id = pe.primary_player
                 WHERE pe.id = ?""", (peid,))
    return c.fetchone()[0]

def _total_games(peid):
    if peid is None:
        return 0
    c = conn.cursor()
    c.execute("""SELECT COUNT(DISTINCT gp.game)
                 FROM person pe
                 JOIN person_player pmap
                   ON pmap.person = pe.id
                 JOIN game_player gp
                   ON gp.player = pmap.player
                 WHERE
                   pe.id = ?""", (peid,))
    # aggregates without GROUP BY always have exactly one row,
    # so no need to check for None here
    return c.fetchone()[0]

def _set_thing(thing, val, acc, hostmask, raw=False):
    with conn:
        c = conn.cursor()
        if acc is None or acc == "*":
            if raw:
                params = (hostmask,)
            else:
                params = (hostmask, val)
                val = "?"
            c.execute("""WITH pmap AS (
                           SELECT pp.person
                           FROM player pl
                           JOIN person_player pp
                             ON pp.player = pl.id
                           WHERE
                             pl.account IS NULL
                             AND pl.hostmask = ?
                             AND pl.active = 1
                         )
                         UPDATE person
                         SET {0} = {1}
                         WHERE person.id = pmap.person""".format(thing, val), params)
        else:
            if raw:
                params = (acc,)
            else:
                params = (acc, val)
                val = "?"
            c.execute("""WITH pmap AS (
                           SELECT pp.person
                           FROM player pl
                           JOIN person_player pp
                             ON pp.player = pl.id
                           WHERE
                             pl.account = ?
                             AND pl.hostmask IS NULL
                             AND pl.active = 1
                         )
                         UPDATE person
                         SET {0} = {1}
                         WHERE person.id = pmap.person""".format(thing, val), params)

def _toggle_thing(thing, acc, hostmask):
    _set_thing(thing, "CASE {0} WHEN 1 THEN 0 ELSE 1 END".format(thing), acc, hostmask, raw=True)

# vim: set expandtab:sw=4:ts=4:
