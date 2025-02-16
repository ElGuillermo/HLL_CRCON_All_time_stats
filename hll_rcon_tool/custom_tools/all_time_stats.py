"""
all_time_stats.py

A plugin for HLL CRCON (https://github.com/MarechJ/hll_rcon_tool)
that displays a player's all-time stats on chat command and on player's connection.

Source : https://github.com/ElGuillermo

Feel free to use/modify/distribute, as long as you keep this note in your code
"""

from datetime import datetime
import logging

from sqlalchemy.sql import text

from rcon.models import enter_session
from rcon.player_history import get_player_profile
from rcon.rcon import Rcon, StructuredLogLineWithMetaData


logger = logging.getLogger(__name__)


# Configuration (you must review/change these !)
# -----------------------------------------------------------------------------

# Should we display the stats to every player on connect ?
# True or False
DISPLAY_ON_CONNECT = True

# The command the players have to enter in chat to display their stats
# Note : the command is not case sensitive (ie : '!me' or '!ME' will work)
CHAT_COMMAND = ["!me"]

# Strings translations
# Available : 0 for english, 1 for french, 2 for german, 3 for polish
LANG = 0

# Stats to display
# If you're hosting a console game server,
# you want to avoid the message to be scrollable,
# so you only have 16 lines to display.
STATS_TO_DISPLAY = {
    "playername": True,  # 2 lines
    "firsttimehere": True,  # 2 lines
    "tot_sessions": True,  # 1 line
    "tot_playedgames": True,  # 1 line
    "cumulatedplaytime": True,  # 2 lines
    "avg_sessiontime": True,  # 1 line
    "tot_punishments": True,  # 2 or 3 lines
    # "averages" header will be added if any of the 4 following is True
    "avg_combat": True,
    "avg_offense": True,
    "avg_defense": True,
    "avg_support": True,
    # "totals" header will be added if any of the 4 following is True
    "tot_kills": True,
    "tot_teamkills": True,
    "tot_deaths": True,
    "tot_deaths_by_tk": True,
    "kd_ratio": True,  # 1 line
    "most_killed": True,  # 5 lines
    "most_death_by": True,  # 5 lines
    "most_used_weapons": True  # 5 lines
}

# Should we display seconds in the durations ?
# True or False
DISPLAY_SECS = False

# Translations
# format is : "key": ["english", "french", "german", "polish"]
# ----------------------------------------------
TRANSL = {
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


def define_stats_to_display(player_id: str):
    """
    Define the stats to display according to the user configuration
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
            logger.error("Failed to retrieve player profile: %s", error)
    else:
        logger.info("No stat requires player profile data.")

    # Construct db queries dict
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

    if not queries_to_execute:
        logger.info("No stat requires SQL queries.")
        return player_profile, {}

    return player_profile, queries_to_execute


def process_stats_to_display(player_id:str, player_profile, queries_to_execute:dict) -> dict:
    """
    Store the stats to display in a dict.
    """
    message_vars = {}

    # Set message_vars from get_player_profile()
    if player_profile is None:
        logger.info("No stat requires player profile data.")
    else:
        if STATS_TO_DISPLAY["firsttimehere"]:
            created: str = player_profile.get("created", "2025-01-01T00:00:00.000000")
            elapsed_time_seconds:int = (datetime.now() - datetime.fromisoformat(str(created))).total_seconds()
            firsttimehere:str = readable_duration(elapsed_time_seconds)
            message_vars["firsttimehere"] = firsttimehere
        if STATS_TO_DISPLAY["tot_sessions"]:
            tot_sessions: int = player_profile.get("sessions_count", "1")
            message_vars["tot_sessions"] = tot_sessions
        if STATS_TO_DISPLAY["cumulatedplaytime"]:
            total_playtime_seconds: int = player_profile.get("total_playtime_seconds", "5400")
            cumulatedplaytime:str = readable_duration(total_playtime_seconds)
            message_vars["cumulatedplaytime"] = cumulatedplaytime
        if STATS_TO_DISPLAY["avg_sessiontime"]:
            total_playtime_seconds: int = player_profile.get("total_playtime_seconds", "5400")
            tot_sessions: int = player_profile.get("sessions_count", "1")
            avg_sessiontime:str = readable_duration(int(total_playtime_seconds)/max(1, int(tot_sessions)))
            message_vars["avg_sessiontime"] = avg_sessiontime
        if STATS_TO_DISPLAY["tot_punishments"]:
            tot_punishments:str = get_penalties_message(player_profile)
            message_vars["tot_punishments"] = tot_punishments

    # Set message_vars from SQL queries results
    if len(queries_to_execute) == 0:
        logger.info("No stat requires SQL queries.")
    else:
        with enter_session() as sess:
            # Retrieve the player's database id (it's not the same as its game id).
            player_id_query = "SELECT s.id FROM steam_id_64 AS s WHERE s.steam_id_64 = :player_id"
            db_player_id_row = sess.execute(text(player_id_query), {"player_id": player_id}).fetchone()
            db_player_id = db_player_id_row[0]

            if not db_player_id:
                logger.error("Couldn't find player's id in database. No database data have been processed.")
                return message_vars

            # Get the different SQL queries results
            results = {}
            for key, query in queries_to_execute.items():
                result = sess.execute(text(query), {"db_player_id": db_player_id}).fetchall()
                results[key] = result

        if STATS_TO_DISPLAY["tot_playedgames"]:
            tot_playedgames:int = int(results["tot_playedgames"][0][0] or 0)
            message_vars["tot_playedgames"] = tot_playedgames

        if STATS_TO_DISPLAY["avg_combat"]:
            avg_combat:float = float(results["avg_combat"][0][0] or 0)
            message_vars["avg_combat"] = avg_combat
        if STATS_TO_DISPLAY["avg_offense"]:
            avg_offense:float = float(results["avg_offense"][0][0] or 0)
            message_vars["avg_offense"] = avg_offense
        if STATS_TO_DISPLAY["avg_defense"]:
            avg_defense:float = float(results["avg_defense"][0][0] or 0)
            message_vars["avg_defense"] = avg_defense
        if STATS_TO_DISPLAY["avg_support"]:
            avg_support:float = float(results["avg_support"][0][0] or 0)
            message_vars["avg_support"] = avg_support

        if STATS_TO_DISPLAY["tot_kills"]:
            tot_kills:int = int(results["tot_kills"][0][0] or 0)
            message_vars["tot_kills"] = tot_kills
        if STATS_TO_DISPLAY["tot_teamkills"]:
            tot_teamkills:int = int(results["tot_teamkills"][0][0] or 0)
            message_vars["tot_teamkills"] = tot_teamkills
        if STATS_TO_DISPLAY["tot_deaths"]:
            tot_deaths:int = int(results["tot_deaths"][0][0] or 0)
            message_vars["tot_deaths"] = tot_deaths
        if STATS_TO_DISPLAY["tot_deaths_by_tk"]:
            tot_deaths_by_tk:int = int(results["tot_deaths_by_tk"][0][0] or 0)
            message_vars["tot_deaths_by_tk"] = tot_deaths_by_tk
        if STATS_TO_DISPLAY["kd_ratio"]:
            kd_ratio:float = float(results["kd_ratio"][0][0] or 0)
            message_vars["kd_ratio"] = kd_ratio

        if STATS_TO_DISPLAY["most_killed"]:
            most_killed:str = "\n".join(
                f"{row[0]} : {row[1]} ({row[2]} {TRANSL['games'][LANG]})"
                for row in results["most_killed"]
            )
            message_vars["most_killed"] = most_killed
        if STATS_TO_DISPLAY["most_death_by"]:
            most_death_by:str = "\n".join(
                f"{row[0]} : {row[1]} ({row[2]} {TRANSL['games'][LANG]})"
                for row in results["most_death_by"]
            )
            message_vars["most_death_by"] = most_death_by
        if STATS_TO_DISPLAY["most_used_weapons"]:
            most_used_weapons:str = "\n".join(
                f"{row[0]} ({row[1]} kills)"
                for row in results["most_used_weapons"]
            )
            message_vars["most_used_weapons"] = most_used_weapons

    return message_vars


def construct_message(player_name:str, message_vars: dict) -> str:
    """
    Constructs the final message to send to the player.
    """
    if not any(message_vars.values()):
        return "No stats to display"

    message = ""

    if STATS_TO_DISPLAY["playername"]:
        message += f"{player_name}\n\n"
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
    Collect, process and displays statistics
    """
    if (
        not (player_id := struct_log.get("player_id_1"))
        or not (player_name := struct_log.get("player_name_1"))
    ):
        logger.error("No player_id_1 or player_name_1 in CONNECTED or CHAT log")
        return

    try:
        player_profile, queries_to_execute = define_stats_to_display(player_id)
        message_vars = process_stats_to_display(player_id, player_profile, queries_to_execute)
        message = construct_message(player_name, message_vars)
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
    if DISPLAY_ON_CONNECT:
        all_time_stats(rcon, struct_log)


def all_time_stats_on_chat_command(rcon: Rcon, struct_log: StructuredLogLineWithMetaData) -> None:
    """
    Call the message on chat command
    """
    if not (chat_message := struct_log.get("sub_content")):
        logger.error("No sub_content in CHAT log")
        return

    if chat_message.lower() in (cmd.lower() for cmd in CHAT_COMMAND):
        all_time_stats(rcon, struct_log)
