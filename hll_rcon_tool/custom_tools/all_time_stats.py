"""
all_time_stats.py

A plugin for HLL CRCON (https://github.com/MarechJ/hll_rcon_tool)
that displays a player's all-time stats on chat command and on player's connection.

Source : https://github.com/ElGuillermo

Feel free to use/modify/distribute, as long as you keep this note in your code
"""

from datetime import datetime
import logging
import os
from typing import Any
from sqlalchemy.sql import text

from rcon.models import enter_session
from rcon.player_history import get_player_profile
from rcon.rcon import Rcon, StructuredLogLineWithMetaData
from rcon.utils import get_server_number

import custom_tools.all_time_stats_config as all_time_stats_config
from custom_tools.common_translations import TRANSL


# Setup logger
os.makedirs('/logs', exist_ok=True)
logger = logging.getLogger('all_time_stats_standalone')
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    file_handler = logging.FileHandler('/logs/custom_tools_all_time_stats.log', mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


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


def sanitize_to_int(value: Any, default: int = 0, min_val: int = 0, max_val: int|None = None) -> int:
    """
    Converts 'value' to a positive integer (truncating decimals).
    Returns 'default' :
    - if conversion isn't possible.
    - if 'value' is out of min-max range
    Returns 'value' (int) if converted
    """
    try:
        clean_val = int(float(value))
        if clean_val < min_val:
            logger.warning(f"Invalid value in config ('%s' is too low, min allowed is '%s'), using default value : %s", value, min_val, default)
            return default
        if max_val is not None and clean_val > max_val:
            logger.warning(f"Invalid value in config ('%s' is too high, max allowed is '%s'), using default value : %s", value, max_val, default)
            return default
        return clean_val
    except (ValueError, TypeError):
        logger.warning(f"Invalid value in config ('%s' is not a valid number), using default value : %s", value, default)
        return default


# Check config
num_langs = len(next(iter(TRANSL.values())))
all_time_stats_config.LANG = sanitize_to_int(all_time_stats_config.LANG, default=0, max_val=num_langs - 1)


def readable_duration(seconds: int) -> str:
    """
    Returns a human-readable string (years, months, days, XXhXXmXXs)
    from a number of seconds.
    """
    cfg = all_time_stats_config
    lang = cfg.LANG

    # seconds, TRANSL keys
    UNITS = [
        (31536000, 'years'),
        (2592000,  'months'),
        (86400,    'days')
    ]

    parts = []
    rem_secs = int(seconds)

    # years, months, days
    for seconds_in_unit, transl_key in UNITS:
        value, rem_secs = divmod(rem_secs, seconds_in_unit)
        if value > 0:
            parts.append(f"{value} {TRANSL[transl_key][lang]}")

    # hours, minutes, seconds
    h, rem = divmod(rem_secs, 3600)
    m, s = divmod(rem, 60)

    time_str = f"{h}h{m:02d}"
    if cfg.DISPLAY_SECS:
        time_str += f"m{s:02d}s"
    elif m == 0 and h == 0 and not parts:
        # display secs if there is less than 60 secs
        time_str = f"{s}s"
    elif m > 0 or h > 0:
        # adds an "m" to minutes if there is no secs left
        time_str += "m"

    if not parts:
        return time_str

    return ", ".join(parts) + f", {time_str}"


def get_penalties_data(player_profile_data) -> list[str]:
    """
    Returns a list of strings for each existing penalty type.
    """
    lang = all_time_stats_config.LANG
    counts = player_profile_data.get("penalty_count", {})

    # API key, TRANSL key
    penalties_to_check = [
        ("PUNISH", "punishes"),
        ("KICK", "kicks"),
        ("TEMPBAN", "tempbans"),
        ("PERMABAN", "permabans")
    ]

    active_penalties = []
    for key, label in penalties_to_check:
        count = counts.get(key, 0)
        if count > 0:
            active_penalties.append(f"{count} {label}")

    return active_penalties


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
    needs_player_profile = any(all_time_stats_config.STATS_TO_DISPLAY[key] for key in stats_needing_profile)

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
                          for key, include in all_time_stats_config.STATS_TO_DISPLAY.items()
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
            logger.error(f"Couldn't find player's id '%s' in database. No database data has been processed.", player_id)
            return {}

        # Get the db_stats
        db_stats = {}
        for key, query in queries_to_execute.items():
            result = sess.execute(text(query), {"db_player_id": db_player_id}).fetchall()
            db_stats[key] = result

    return db_stats


def process_stats(player_profile, db_stats: dict) -> dict[str, Any]:
    """
    Store the stats to display in a dict.
    """
    message_vars: dict[str, Any] = {"onfirstsession": False}
    config = all_time_stats_config
    display_cfg = config.STATS_TO_DISPLAY

    # First session
    sessions_count = int(player_profile.get("sessions_count", 1)) if player_profile else 1
    if sessions_count <= 1:
        message_vars["onfirstsession"] = True
        return message_vars

    # Profile data
    if player_profile:
        if display_cfg.get("firsttimehere"):
            created = player_profile.get("created", "2026-01-01T00:00:00.000000")
            delta = datetime.now() - datetime.fromisoformat(str(created))
            message_vars["firsttimehere"] = str(readable_duration(int(delta.total_seconds())))

        if display_cfg.get("tot_sessions"):
            message_vars["tot_sessions"] = sessions_count

        total_playtime = player_profile.get("total_playtime_seconds", 0)

        if display_cfg.get("cumulatedplaytime"):
            message_vars["cumulatedplaytime"] = str(readable_duration(total_playtime))

        if display_cfg.get("avg_sessiontime"):
            avg_time = int(total_playtime / max(1, sessions_count))
            message_vars["avg_sessiontime"] = str(readable_duration(avg_time))

        if display_cfg.get("tot_punishments"):
            message_vars["tot_punishments"] = get_penalties_data(player_profile)
    else:
        logger.info("No stat requires player profile data.")

    # Database data
    if not db_stats:
        logger.info("No stat requires db data.")
        return message_vars

    def get_sql_val(key, default=0, cast=int):
        try:
            val = db_stats[key][0][0]
            return cast(val) if val is not None else default
        except (KeyError, IndexError, TypeError):
            return default

    scalar_mappings = {
        "tot_playedgames": int,
        "avg_combat": float,
        "avg_offense": float,
        "avg_defense": float,
        "avg_support": float,
        "tot_kills": int,
        "tot_teamkills": int,
        "tot_deaths": int,
        "tot_deaths_by_tk": int,
        "kd_ratio": float
    }

    for key, cast_type in scalar_mappings.items():
        if display_cfg.get(key):
            message_vars[key] = get_sql_val(key, cast=cast_type)

    lang = config.LANG
    game_str = TRANSL['games'][lang]

    list_mappings = {
        "most_killed": lambda rows: "\n".join(f"{r[0]} : {r[1]} ({r[2]} {game_str})" for r in rows),
        "most_death_by": lambda rows: "\n".join(f"{r[0]} : {r[1]} ({r[2]} {game_str})" for r in rows),
        "most_used_weapons": lambda rows: "\n".join(f"{r[0]} ({r[1]} {TRANSL['kills'][lang]})" for r in rows)
    }

    for key, formatter in list_mappings.items():
        if display_cfg.get(key) and key in db_stats:
            message_vars[key] = str(formatter(db_stats[key]))

    return message_vars


def construct_message(player_name: str, message_vars: dict) -> str:
    """
    Constructs the final message to send to the player.
    """
    cfg = all_time_stats_config
    lang = cfg.LANG
    display = cfg.STATS_TO_DISPLAY

    # First session
    if message_vars.get("onfirstsession"):
        return TRANSL["onfirstsession"][lang]

    # No stats
    if len(message_vars) <= 1:
        return TRANSL["nostatsyet"][lang]

    lines = []

    # Header
    if display.get("playername"):
        lines.append(f"─ {player_name} ─")

    # value / translation key
    base_stats = [
        ("firsttimehere", "firsttimehere"),
        ("tot_sessions", "tot_sessions"),
        ("tot_playedgames", "playedgames"),
        ("cumulatedplaytime", "cumulatedplaytime"),
        ("avg_sessiontime", "avg_sessiontime"),
    ]
    for var_key, transl_key in base_stats:
        if display.get(var_key) and var_key in message_vars:
            sep = "\n"
            lines.append(f"┌ {TRANSL[transl_key][lang]}{sep}│ {message_vars[var_key]}")

    # Punish / kicks / bans
    if display.get("tot_punishments") and "tot_punishments" in message_vars:
        penalties = message_vars["tot_punishments"]
        if not penalties:
            items_to_show = [TRANSL['nopunish'][lang]]
        else:
            items_to_show = penalties

        header = f"┌ {TRANSL['tot_punishments'][lang]}"
        rows = "\n".join([f"│ · {item}" for item in items_to_show])
        lines.append(f"{header}\n{rows}")

    # Averages
    avg_fields = [
        ("avg_combat", "combat"),
        ("avg_offense", "offense"),
        ("avg_defense", "defense"),
        ("avg_support", "support")
    ]
    active_avgs = [f"│ · {TRANSL[tk][lang]} : {message_vars[vk]}" for vk, tk in avg_fields if display.get(vk)]
    if active_avgs:
        lines.append(f"┌ {TRANSL['averages'][lang]}")
        lines.append("\n".join(active_avgs))

    # Totals
    if any(display.get(k) for k in ["tot_kills", "tot_teamkills", "tot_deaths", "tot_deaths_by_tk"]):
        lines.append(f"┌ {TRANSL['totals'][lang]}")

        # Kills
        if display.get("tot_kills") or display.get("tot_teamkills"):
            k_msg = f"│ · {TRANSL['kills'][lang]} : {message_vars.get('tot_kills', 0)}"
            # TK
            if display.get("tot_teamkills"):
                k_msg += f" ({message_vars.get('tot_teamkills', 0)} {TRANSL['team_kills_short'][lang]})"

            lines.append(k_msg)

        # Deaths
        if display.get("tot_deaths") or display.get("tot_deaths_by_tk"):
            d_msg = f"│ · {TRANSL['deaths'][lang]} : {message_vars.get('tot_deaths', 0)}"
            # TK
            if display.get("tot_deaths_by_tk"):
                d_msg += f" ({message_vars.get('tot_deaths_by_tk', 0)} {TRANSL['team_kills_short'][lang]})"

            lines.append(d_msg)

    # KD
    if display.get("kd_ratio"):
        lines.append(f"│ · {TRANSL['kills'][lang]}/{TRANSL['deaths'][lang]} : {message_vars['kd_ratio']}")

    # Most killed / most died from / most used weapons
    final_sections = [
        ("most_killed", "victims"),
        ("most_death_by", "nemesis"),
        ("most_used_weapons", "favoriteweapons")
    ]
    for var_key, transl_key in final_sections:
        if display.get(var_key) and var_key in message_vars:
            lines.append(f"┌ {TRANSL[transl_key][lang]}\n│ · {message_vars[var_key]}")

    return "\n".join(lines)


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
    Call the message on player's connexion
    """
    server_number = get_server_number()
    if all_time_stats_config.DISPLAY_ON_CONNECT and server_number in all_time_stats_config.ENABLE_ON_SERVERS:
        all_time_stats(rcon, struct_log)


def all_time_stats_on_chat_command(rcon: Rcon, struct_log: StructuredLogLineWithMetaData) -> None:
    """
    Call the message on chat command
    """
    server_number = get_server_number()

    # The calling log line sent by the server lacks mandatory data
    if not (chat_message := struct_log.get("sub_content")) or server_number not in all_time_stats_config.ENABLE_ON_SERVERS:
        logger.error("No sub_content in CHAT log")
        return

    # Search for any configured chat command (case insensitive)
    if chat_message.lower() in (cmd.lower() for cmd in all_time_stats_config.CHAT_COMMAND) and server_number in all_time_stats_config.ENABLE_ON_SERVERS:
        logger.info(f"'%s' ('%s') asked for its all_time_stats using '%s' command in chat on server '%s'", struct_log.get("player_name_1"), struct_log.get("player_id_1"), chat_message.lower(), server_number)
        all_time_stats(rcon, struct_log)
