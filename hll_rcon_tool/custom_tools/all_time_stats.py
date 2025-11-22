"""
all_time_stats.py

A plugin for HLL CRCON (https://github.com/MarechJ/hll_rcon_tool)
that displays a player's all-time stats on chat command and on player's connection.

Source : https://github.com/ElGuillermo

Feel free to use/modify/distribute, as long as you keep this note in your code
"""

from datetime import datetime
from logging import getLogger

from sqlalchemy.sql import text

from rcon.models import enter_session
from rcon.player_history import get_player_profile
from rcon.rcon import Rcon, StructuredLogLineWithMetaData
from rcon.utils import get_server_number


logger = getLogger(__name__)


# Configuration (you must review/change these !)
# -----------------------------------------------------------------------------

# Can be enabled/disabled on your different game servers
# ie : ["1"]           = enabled only on server 1
#      ["1", "2"]      = enabled on servers 1 and 2
#      ["2", "4", "5"] = enabled on servers 2, 4 and 5
ENABLE_ON_SERVERS = ["1"]

# Should we display the stats to every player on connect ?
# True or False
DISPLAY_ON_CONNECT = True

# The command the players have to enter in chat to display their stats
# Note : the command is not case sensitive (ie : '!me' or '!ME' will work)
CHAT_COMMAND = ["!me"]

# Strings translations
# Available : 0 for english, 1 for french, 2 for german, 3 for polish
LANG = 1

# Stats to display
# If you're hosting a console game server,
# you want to avoid the message to be scrollable (you only have 16 lines available).
STATS_TO_DISPLAY = {
    "playername": True,         # 1 line
    "firsttimehere": True,      # 2 lines  # Console : set it to False
    "tot_sessions": True,       # 1 line   # Console : set it to False
    "tot_playedgames": True,    # 1 line
    "cumulatedplaytime": True,  # 2 lines
    "avg_sessiontime": True,    # 1 line   # Console : set it to False
    "tot_punishments": True,    # up to 4 lines (2 lines of header + 1 or 2 lines of stats)  # Console : set it to False

    # "averages" header (2 lines) will be added if any of the 4 following is True
    # 2 stats can be displayed on a line, so the whole thing will take
    # - 3 lines (2 lines of header + 1 line of stats) if only one or two stats are True,
    # - 4 lines if three or all stats are True
    "avg_combat": True,
    "avg_offense": True,
    "avg_defense": True,
    "avg_support": True,

    # "totals" header (2 lines) will be added if any of the 4 following is True
    # As "tot_teamkills" and "tot_deaths_by_tk" can follow "tot_kills" and "top_deaths" on their lines,
    # setting the 4 values to True will add 4 lines (2 lines of header + 2 lines of stats)
    "tot_kills": True,          # 1 line
    "tot_teamkills": True,      # 1 line or 0 if "tot_kills" is True
    "tot_deaths": True,         # 1 line
    "tot_deaths_by_tk": True,   # 1 line or 0 if "tot_deaths" is True

    "kd_ratio": True,           # 1 line

    "most_killed": True,        # 5 lines (2 lines of header + 3 lines of stats)  # Console : set it to False
    "most_death_by": True,      # 5 lines (2 lines of header + 3 lines of stats)  # Console : set it to False
    "most_used_weapons": True   # 5 lines (2 lines of header + 3 lines of stats)  # Console : set it to False
}

# Should we display seconds in the durations ?
# True or False
DISPLAY_SECS = False

# Translations
# format is : "key": ["english", "french", "german", "polish"]
# ----------------------------------------------
TRANSL = {
    "nostat": ["No stat to display", "Aucune stat", "Keine Statistik zum Anzeigen", "Brak statystyk do wyświetlenia"],
    "onfirstsession": ["First time here !\nWelcome !", "C'est ta première visite !\nBienvenue !", "Zum ersten Mal hier! \nWillkommen!", "Pierwszy raz tutaj!\nWitamy!"],
    "years": ["years", "années", "Jahre", "Lata"],
    "months": ["months", "mois", "Monate", "Miesiące"],
    "days": ["days", "jours", "Tage", "Dni"],
    "hours": ["hours", "heures", "Dienststunden", "Godziny"],
    "minutes": ["minutes", "minutes", "Minuten", "Minuty"],
    "seconds": ["seconds", "secondes", "Sekunden", "Sekundy"],
    "firsttimehere": ["▒ First time here", "▒ Arrivé(e) il y a", "▒ Zum ersten Mal hier", "▒ Pierwszy raz tutaj"],
    "tot_sessions": ["▒ Game sessions", "▒ Sessions de jeu", "▒ Spielesitzungen", "▒ Sesji"],
    "playedgames": ["▒ Played games", "▒ Parties jouées", "▒ gespielte Spiele", "▒ Rozegranych gier"],
    "cumulatedplaytime": ["▒ Cumulated play time", "▒ Temps de jeu cumulé", "▒ Kumulierte Spielzeit", "▒ Łączny czas gry"],
    "avg_sessiontime": ["▒ Average session", "▒ Session moyenne", "▒ Durchschnittliche Sitzung", "▒ Średnio na sesje"],
    "tot_punishments": ["▒ Punishments ▒", "▒ Punitions ▒", "▒ Strafen ▒", "▒ Kary ▒"],
    "nopunish": ["None ! Well done !", "Aucune ! Félicitations !", "Keiner! Gut gemacht!", "Brak! Dobra robota!"],
    "averages": ["▒ Averages", "▒ Moyennes ▒", "▒ Durchschnittswerte", "▒ Średnie"],
    "avg_combat": ["combat", "combat", "kampf", "walka"],
    "avg_offense": ["attack", "attaque", "angriff", "ofensywa"],
    "avg_defense": ["defense", "défense", "verteidigung", "defensywa"],
    "avg_support": ["support", "soutien", "unterstützung", "wsparcie"],
    "totals": ["▒ Totals ▒", "▒ Totaux ▒", "▒ Gesamtsummen ▒", "▒ Łącznie ▒"],
    "kills": ["kills", "kills", "tötet", "zabójstwa"],
    "tks": ["TKs", "TKs", "TKs", "TKs"],
    "deaths": ["deaths", "morts", "todesfälle", "śmierci"],
    "ratio": ["ratio", "ratio", "verhältnis", "średnia"],
    "favoriteweapons": ["▒ Favorite weapons ▒", "▒ Armes favorites ▒", "▒ Lieblingswaffen ▒", "▒ Ulubione bronie ▒"],
    "games": ["games", "parties", "Spiele", "Gry"],
    "victims": ["▒ Victims ▒", "▒ Victimes ▒", "▒ Opfer ▒", "▒ Ofiary ▒"],
    "nemesis": ["▒ Nemesis ▒", "▒ Nemesis ▒", "▒ Nemesis ▒", "▒ Nemesis ▒"],
}


# (End of configuration)
# -----------------------------------------------------------------------------

AVAILABLE_QUERIES = {
    "tot_playedgames": "SELECT COUNT(*) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_combat": "SELECT ROUND(AVG(combat), 2) AS avg_combat FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_offense": "SELECT ROUND(AVG(offense), 2) AS avg_offense FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_defense": "SELECT ROUND(AVG(defense), 2) AS avg_defense FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_support": "SELECT ROUND(AVG(support), 2) AS avg_support FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_kills": "SELECT SUM(kills) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_teamkills": "SELECT SUM(teamkills) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_deaths": "SELECT SUM(deaths) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_deaths_by_tk": "SELECT SUM(deaths_by_tk) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "kd_ratio": "SELECT ROUND((SUM(kills) - SUM(teamkills))::numeric / CASE WHEN (SUM(deaths) - SUM(deaths_by_tk)) = 0 THEN 1 ELSE (SUM(deaths) - SUM(deaths_by_tk)) END, 2) AS ratio FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "most_killed": "SELECT key AS player_name, SUM(value::int) AS total_kills, count(*) FROM public.player_stats, jsonb_each_text(most_killed::jsonb) WHERE playersteamid_id = :db_player_id GROUP BY key ORDER BY total_kills DESC LIMIT 3",
    "most_death_by": "SELECT key AS player_name, SUM(value::int) AS total_kills, count(*) FROM public.player_stats, jsonb_each_text(death_by::jsonb) WHERE playersteamid_id = :db_player_id GROUP BY key ORDER BY total_kills DESC LIMIT 3",
    "most_used_weapons": "SELECT weapon, SUM(usage_count) AS total_usage FROM (SELECT playersteamid_id, weapon_data.key AS weapon, (weapon_data.value::text)::int AS usage_count FROM public.player_stats, jsonb_each(weapons::jsonb) AS weapon_data WHERE playersteamid_id = :db_player_id) AS weapon_usage GROUP BY weapon ORDER BY total_usage DESC LIMIT 3"
}


if LANG < 0 or LANG >= len(TRANSL["years"]):
    LANG = 0  # Default to English if LANG is out of bounds


def format_to_hms(hours: int, minutes: int, seconds: int, display_seconds: bool=True) -> str:
    """
    Formats the hours, minutes, and seconds as XXhXXmXXs or XXhXXm.
    """
    if display_seconds:
        return f"{int(hours)}h{int(minutes):02d}m{int(seconds):02d}s"
    return f"{int(hours)}h{int(minutes):02d}"


def readable_duration(seconds: int) -> str:
    """
    Returns a human-readable string (years, months, days, XXhXXmXXs)
    from a number of seconds.
    """
    seconds = int(seconds)
    years, remaining_seconds_in_year = divmod(seconds, 31536000)
    months, remaining_seconds_in_month = divmod(remaining_seconds_in_year, 2592000)
    days, remaining_seconds_in_day = divmod(remaining_seconds_in_month, 86400)
    hours, remaining_seconds_in_hour = divmod(remaining_seconds_in_day, 3600)
    minutes, remaining_seconds = divmod(remaining_seconds_in_hour, 60)

    time_string = []
    if years > 0:
        time_string.append(f"{years} {TRANSL['years'][LANG]}")
        time_string.append(", ")
    if months > 0:
        time_string.append(f"{months} {TRANSL['months'][LANG]}")
        time_string.append(", ")
    if days > 0:
        time_string.append(f"{days} {TRANSL['days'][LANG]}")
        time_string.append(", ")

    time_string.append(format_to_hms(hours, minutes, remaining_seconds, DISPLAY_SECS))

    return "".join(filter(None, time_string))


def get_penalties_message(player_profile_data) -> str:
    """
    Returns a string with the number of kicks, punishes, tempbans and permabans.
    """
    kicks = player_profile_data.get("penalty_count", {}).get("KICK", 0)
    punishes = player_profile_data.get("penalty_count", {}).get("PUNISH", 0)
    tempbans = player_profile_data.get("penalty_count", {}).get("TEMPBAN", 0)
    permabans = player_profile_data.get("penalty_count", {}).get("PERMABAN", 0)

    penalties_message = ""
    if kicks == 0 and punishes == 0 and tempbans == 0 and permabans == 0:
        penalties_message += f"{TRANSL['nopunish'][LANG]}"
    else:
        if punishes > 0:
            penalties_message += f"{punishes} punishes"
        if kicks > 0:
            if punishes > 0:
                penalties_message += ", "
            penalties_message += f"{kicks} kicks"
        if tempbans > 0:
            if punishes > 0 or kicks > 0:
                penalties_message += ", "
            if punishes > 0 and kicks > 0:
                penalties_message += "\n"
            penalties_message += f"{tempbans} tempbans"
        if permabans > 0:
            if punishes > 0 or kicks > 0 or tempbans > 0:
                penalties_message += ", "
            penalties_message += f"{permabans} permabans"

    return penalties_message


def get_profile_stats(player_id: str):
    """
    Ask for get_player_profile() if any of its data is required in user configuration
    """
    # Flag to check if we need player profile data
    stats_needing_profile = [
        "firsttimehere",
        "tot_sessions",
        "cumulatedplaytime",
        "avg_sessiontime",
        "tot_punishments"
    ]
    needs_player_profile = any(STATS_TO_DISPLAY[key] for key in stats_needing_profile)

    # Retrieve player profile data if needed
    player_profile = None
    if needs_player_profile:
        try:
            player_profile = get_player_profile(player_id=player_id, nb_sessions=0)
        except Exception as error:
            logger.error("Failed to retrieve player profile data: %s", error)
    else:
        logger.info("No stat requires player profile data.")

    return player_profile


def get_db_stats(player_id: str) -> dict:
    """
    Retrieves the db stats according to the user configuration
    """
    # Define the SQL queries to execute
    stats_needing_queries = [
        "tot_playedgames",
        "avg_combat",
        "avg_offense",
        "avg_defense",
        "avg_support",
        "tot_kills",
        "tot_teamkills",
        "tot_deaths",
        "tot_deaths_by_tk", 
        "kd_ratio",
        "most_killed",
        "most_death_by",
        "most_used_weapons"
    ]
    queries_to_execute = {key: AVAILABLE_QUERIES[key]
                          for key, include in STATS_TO_DISPLAY.items()
                          if include and key in stats_needing_queries}

    # If there's no query to execute
    if len(queries_to_execute) == 0:
        logger.info("No stat requires SQL queries.")
        return {}

    # Executing required queries
    with enter_session() as sess:

        # Retrieve the player's database id (it's not the same as its game id).
        player_id_query = "SELECT s.id FROM steam_id_64 AS s WHERE s.steam_id_64 = :player_id"
        db_player_id_row = sess.execute(text(player_id_query), {"player_id": player_id}).fetchone()
        db_player_id = db_player_id_row[0]

        # Can't find the player's database id
        if not db_player_id:
            logger.error("Couldn't find player's id in database. No database data have been processed.")
            return {}

        # Get the db_stats
        db_stats = {}
        for key, query in queries_to_execute.items():
            result = sess.execute(text(query), {"db_player_id": db_player_id}).fetchall()
            db_stats[key] = result

    return db_stats


def process_stats(player_profile, db_stats:dict) -> dict:
    """
    Store the stats to display in a dict.
    """
    message_vars = {}

    # Cancel all queries if this is the player's first session
    message_vars["onfirstsession"] = False
    if int(player_profile.get("sessions_count", 1)) == 1:
        message_vars["onfirstsession"] = True
        return message_vars

    # No stat requiring player_profile
    if player_profile is None:
        logger.info("No stat requires player profile data.")

    # Set message_vars from player_profile
    else:
        if STATS_TO_DISPLAY["firsttimehere"]:
            created: str = player_profile.get("created", "2025-01-01T00:00:00.000000")
            elapsed_time_seconds:int = (datetime.now() - datetime.fromisoformat(str(created))).total_seconds()
            message_vars["firsttimehere"] = str(readable_duration(elapsed_time_seconds))
        if STATS_TO_DISPLAY["tot_sessions"]:
            message_vars["tot_sessions"] = int(player_profile.get("sessions_count", 1))
        if STATS_TO_DISPLAY["cumulatedplaytime"]:
            total_playtime_seconds: int = player_profile.get("total_playtime_seconds", 5400)
            message_vars["cumulatedplaytime"] = str(readable_duration(total_playtime_seconds))
        if STATS_TO_DISPLAY["avg_sessiontime"]:
            total_playtime_seconds: int = player_profile.get("total_playtime_seconds", 5400)
            tot_sessions: int = player_profile.get("sessions_count", 1)
            message_vars["avg_sessiontime"] = str(readable_duration(int(total_playtime_seconds)/max(1, int(tot_sessions))))
        if STATS_TO_DISPLAY["tot_punishments"]:
            message_vars["tot_punishments"] = str(get_penalties_message(player_profile))

    # No stat requiring db_stats
    if len(db_stats) == 0:
        logger.info("No stat requires db data.")
        return message_vars

    # Set message_vars from SQL queries results
    if STATS_TO_DISPLAY["tot_playedgames"]:
        message_vars["tot_playedgames"] = int(db_stats["tot_playedgames"][0][0] or 0)
    if STATS_TO_DISPLAY["avg_combat"]:
        message_vars["avg_combat"] = float(db_stats["avg_combat"][0][0] or 0)
    if STATS_TO_DISPLAY["avg_offense"]:
        message_vars["avg_offense"] = float(db_stats["avg_offense"][0][0] or 0)
    if STATS_TO_DISPLAY["avg_defense"]:
        message_vars["avg_defense"] = float(db_stats["avg_defense"][0][0] or 0)
    if STATS_TO_DISPLAY["avg_support"]:
        message_vars["avg_support"] = float(db_stats["avg_support"][0][0] or 0)
    if STATS_TO_DISPLAY["tot_kills"]:
        message_vars["tot_kills"] = int(db_stats["tot_kills"][0][0] or 0)
    if STATS_TO_DISPLAY["tot_teamkills"]:
        message_vars["tot_teamkills"] = int(db_stats["tot_teamkills"][0][0] or 0)
    if STATS_TO_DISPLAY["tot_deaths"]:
        message_vars["tot_deaths"] = int(db_stats["tot_deaths"][0][0] or 0)
    if STATS_TO_DISPLAY["tot_deaths_by_tk"]:
        message_vars["tot_deaths_by_tk"] = int(db_stats["tot_deaths_by_tk"][0][0] or 0)
    if STATS_TO_DISPLAY["kd_ratio"]:
        message_vars["kd_ratio"] = float(db_stats["kd_ratio"][0][0] or 0)
    if STATS_TO_DISPLAY["most_killed"]:
        message_vars["most_killed"] = str("\n".join(
            f"{row[0]} : {row[1]} ({row[2]} {TRANSL['games'][LANG]})"
            for row in db_stats["most_killed"]
        ))
    if STATS_TO_DISPLAY["most_death_by"]:
        message_vars["most_death_by"] =  str("\n".join(
            f"{row[0]} : {row[1]} ({row[2]} {TRANSL['games'][LANG]})"
            for row in db_stats["most_death_by"]
        ))
    if STATS_TO_DISPLAY["most_used_weapons"]:
        message_vars["most_used_weapons"] = str("\n".join(
            f"{row[0]} ({row[1]} kills)"
            for row in db_stats["most_used_weapons"]
        ))

    return message_vars


def construct_message(player_name:str, message_vars: dict) -> str:
    """
    Constructs the final message to send to the player.
    """
    # (Shouldn't happen unless all STATS_TO_DISPLAY are set to False)
    if len(message_vars) == 1 and not message_vars["onfirstsession"]:
        return TRANSL["nostat"][LANG]

    # On first connection
    if message_vars["onfirstsession"]:
        return TRANSL["onfirstsession"][LANG]

    message = ""

    if STATS_TO_DISPLAY["playername"]:
        message += f"─ {player_name} ─\n"
    if STATS_TO_DISPLAY["firsttimehere"]:
        message += f"{TRANSL['firsttimehere'][LANG]} :\n{message_vars['firsttimehere']}\n"
    if STATS_TO_DISPLAY["tot_sessions"]:
        message += f"{TRANSL['tot_sessions'][LANG]} : {message_vars['tot_sessions']}\n"
    if STATS_TO_DISPLAY["tot_playedgames"]:
        message += f"{TRANSL['playedgames'][LANG]} : {message_vars['tot_playedgames']}\n"
    if STATS_TO_DISPLAY["cumulatedplaytime"]:
        message += f"{TRANSL['cumulatedplaytime'][LANG]} :\n{message_vars['cumulatedplaytime']}\n"
    if STATS_TO_DISPLAY["avg_sessiontime"]:
        message += f"{TRANSL['avg_sessiontime'][LANG]} : {message_vars['avg_sessiontime']}\n"

    if STATS_TO_DISPLAY["tot_punishments"]:
        message += f"\n{TRANSL['tot_punishments'][LANG]}\n{message_vars['tot_punishments']}\n"

    # Averages header (if any of the 4 following is True)
    if (
        STATS_TO_DISPLAY["avg_combat"]
        or STATS_TO_DISPLAY["avg_offense"]
        or STATS_TO_DISPLAY["avg_defense"]
        or STATS_TO_DISPLAY["avg_support"]
    ):
        message += f"\n{TRANSL['averages'][LANG]}\n"
    # Averages (4 following)
    if STATS_TO_DISPLAY["avg_combat"]:
        message += f"{TRANSL['avg_combat'][LANG]} : {message_vars['avg_combat']}"
        if (
            not STATS_TO_DISPLAY["avg_offense"]
            and not STATS_TO_DISPLAY["avg_defense"]
            and not STATS_TO_DISPLAY["avg_support"]
        ):
            message += "\n"
        else:
            message += " ; "
    if STATS_TO_DISPLAY["avg_offense"]:
        message += f"{TRANSL['avg_offense'][LANG]} : {message_vars['avg_offense']}"
        if not STATS_TO_DISPLAY["avg_combat"]:
            message += " ; "
        else:
            message += "\n"
    if STATS_TO_DISPLAY["avg_defense"]:
        message += f"{TRANSL['avg_defense'][LANG]} : {message_vars['avg_defense']}"
        if (
            not STATS_TO_DISPLAY["avg_combat"]
            and not STATS_TO_DISPLAY["avg_offense"]
            and not STATS_TO_DISPLAY["avg_support"]
        ):
            message += "\n"
        else:
            message += " ; "
    if STATS_TO_DISPLAY["avg_support"]:
        message += f"{TRANSL['avg_support'][LANG]} {message_vars['avg_support']}\n"

    # Totals header (if any of the 4 following is True)
    if (
        STATS_TO_DISPLAY["tot_kills"]
        or STATS_TO_DISPLAY["tot_teamkills"]
        or STATS_TO_DISPLAY["tot_deaths"]
        or STATS_TO_DISPLAY["tot_deaths_by_tk"]
    ):
        message += f"\n{TRANSL['totals'][LANG]}\n"
    # Totals (4 following)
    if STATS_TO_DISPLAY["tot_kills"]:
        message += f"{TRANSL['kills'][LANG]} : {message_vars['tot_kills']}"
        if not STATS_TO_DISPLAY["tot_teamkills"]:
            message += "\n"
    if STATS_TO_DISPLAY["tot_teamkills"]:
        if STATS_TO_DISPLAY["tot_kills"]:
            message += f" ({message_vars['tot_teamkills']} {TRANSL['tks'][LANG]})\n"
        else:
            message += f"{TRANSL['kills'][LANG]} ({TRANSL['tks'][LANG]}) : {message_vars['tot_teamkills']}\n"
    if STATS_TO_DISPLAY["tot_deaths"]:
        message += f"{TRANSL['deaths'][LANG]} : {message_vars['tot_deaths']}"
        if not STATS_TO_DISPLAY["tot_deaths_by_tk"]:
            message += "\n"
    if STATS_TO_DISPLAY["tot_deaths_by_tk"]:
        if STATS_TO_DISPLAY["tot_deaths"]:
            message += f" ({message_vars['tot_deaths_by_tk']} {TRANSL['tks'][LANG]})\n"
        else:
            message += f"{TRANSL['deaths'][LANG]} ({TRANSL['tks'][LANG]}) : {message_vars['tot_deaths_by_tk']}\n"

    if STATS_TO_DISPLAY["kd_ratio"]:
        message += f"{TRANSL['ratio'][LANG]} {TRANSL['kills'][LANG]}/{TRANSL['deaths'][LANG]} : {message_vars['kd_ratio']}\n"

    if STATS_TO_DISPLAY["most_killed"]:
        message += f"\n{TRANSL['victims'][LANG]}\n{message_vars['most_killed']}\n"

    if STATS_TO_DISPLAY["most_death_by"]:
        message += f"\n{TRANSL['nemesis'][LANG]}\n{message_vars['most_death_by']}\n"

    if STATS_TO_DISPLAY["most_used_weapons"]:
        message += f"\n{TRANSL['favoriteweapons'][LANG]}\n{message_vars['most_used_weapons']}\n"

    return message


def all_time_stats(rcon: Rcon, struct_log: StructuredLogLineWithMetaData) -> None:
    """
    Collect, process and displays stats
    """
    # The calling log line sent by the server lacks mandatory data
    if (
        not (player_id := struct_log.get("player_id_1"))
        or not (player_name := struct_log.get("player_name_1"))
    ):
        logger.error("No player_id_1 or player_name_1 in CONNECTED or CHAT log")
        return

    try:
        # Collect
        player_profile = get_profile_stats(player_id)
        db_stats = get_db_stats(player_id)

        # Process
        message_vars = process_stats(player_profile, db_stats)
        message = construct_message(player_name, message_vars)

        # Display
        rcon.message_player(
            player_name=player_name,
            player_id=player_id,
            message=message,
            by="all_time_stats",
            save_message=False
        )

    except KeyError as error:
        logger.error("Missing key: %s", error)
    except ValueError as error:
        logger.error("Value error: %s", error)
    except Exception as error:
        logger.error("Unexpected error: %s", error, exc_info=True)


def all_time_stats_on_connected(rcon: Rcon, struct_log: StructuredLogLineWithMetaData) -> None:
    """
    Call the message on player's connection
    """
    server_number = get_server_number()
    if DISPLAY_ON_CONNECT and server_number in ENABLE_ON_SERVERS:
        all_time_stats(rcon, struct_log)


def all_time_stats_on_chat_command(rcon: Rcon, struct_log: StructuredLogLineWithMetaData) -> None:
    """
    Call the message on chat command
    """
    server_number = get_server_number()

    # The calling log line sent by the server lacks mandatory data
    if not (chat_message := struct_log.get("sub_content")) and server_number in ENABLE_ON_SERVERS:
        logger.error("No sub_content in CHAT log")
        return

    # Search for any configured chat command (case insensitive)
    if chat_message.lower() in (cmd.lower() for cmd in CHAT_COMMAND) and server_number in ENABLE_ON_SERVERS:
        all_time_stats(rcon, struct_log)
