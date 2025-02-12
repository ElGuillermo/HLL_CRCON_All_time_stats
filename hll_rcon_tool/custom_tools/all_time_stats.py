"""
all_time_stats.py

A plugin for HLL CRCON (https://github.com/MarechJ/hll_rcon_tool)
that displays a player's all-time stats on chat command

Source : https://github.com/ElGuillermo

Feel free to use/modify/distribute, as long as you keep this note in your code
"""

from datetime import datetime
import logging

from sqlalchemy.sql import text

from rcon.models import enter_session
from rcon.player_history import get_player_profile
from rcon.rcon import Rcon, StructuredLogLineWithMetaData

# Configuration (you must review/change these !)
# -----------------------------------------------------------------------------

# Should we display the stats to every player on connect ?
# True or False
DISPLAY_ON_CONNECT = True

# Should we display full stats (display problem on console) ?
# True or False
DETAILED_DISPLAY = True

# The command the players have to enter in chat to display their stats
CHAT_COMMAND = ["!me"]

# Strings translations
# Available : 0 for english, 1 for french, 2 for german, 3 for polish
LANG = 0

# Translations
# format is : "key": ["english", "french", "german", "polish"]
# ----------------------------------------------
TRANSL = {
    "years": ["years", "années", "Jahre", "Lata"],
    "monthes": ["monthes", "mois", "Monate", "Miesiące"],
    "weeks": ["weeks", "semaines", "Wochen", "Tygodnie"],
    "days": ["days", "jours", "Tage", "Dni"],
    "hours": ["hours", "heures", "Dienststunden", "Godziny"],
    "minutes": ["minutes", "minutes", "Minuten", "Minuty"],
    "seconds": ["seconds", "secondes", "Sekunden", "Sekundy"],
    "nopunish": ["None ! Well done !", "Aucune ! Félicitations !", "Keiner! Gut gemacht!", "Brak! Dobra robota!"],
    "firsttimehere": ["first time here", "tu es venu(e) il y a", "zum ersten Mal hier", "Pierwszy raz tutaj"],
    "gamesessions": ["game sessions", "sessions de jeu", "Spielesitzungen", "Sesji"],
    "playedgames": ["played games", "parties jouées", "gespielte Spiele", "Rozegranych gier"],
    "cumulatedplaytime": ["cumulated play time", "temps de jeu cumulé", "kumulierte Spielzeit", "Łączny czas gry"],
    "averagesession": ["average session", "session moyenne", "Durchschnittliche Sitzung", "Średnio na sesje"],
    "punishments": ["punishments", "punitions", "Strafen", "Kary"],
    "averages": ["averages", "moyennes", "Durchschnittswerte", "Średnie"],
    "favoriteweapons": ["favorite weapons", "armes favorites", "Lieblingswaffen", "Ulubione bronie"],
    "victims": ["victims", "victimes", "Opfer", "Ofiary"],
    "nemesis": ["nemesis", "nemesis", "Nemesis", "Nemesis"],
    "combat": ["combat", "combat", "kampf", "walka"],
    "offense": ["attack", "attaque", "angriff", "ofensywa"],
    "defense": ["defense", "défense", "verteidigung", "defensywa"],
    "support": ["support", "soutien", "unterstützung", "wsparcie"],
    "totals": ["totals", "totaux", "Gesamtsummen", "Łącznie"],
    "kills": ["kills", "kills", "tötet", "zabójstwa"],
    "deaths": ["deaths", "morts", "todesfälle", "śmierci"],
    "ratio": ["ratio", "ratio", "verhältnis", "średnia"],
    "statistics": ["statistics", "statistiques", "statistiken", "statystyka"],
}

# (End of configuration)
# -----------------------------------------------------------------------------
SECONDS_IN_YEAR = 31536000
SECONDS_IN_MONTH = 2592000
SECONDS_IN_DAY = 86400
SECONDS_IN_HOUR = 3600
SECONDS_IN_MINUTE = 60
BASIC_DISPLAY_QUERIES = {
    "tot_games": "SELECT COUNT(*) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_combat": "SELECT AVG(combat) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_offense": "SELECT AVG(offense) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_defense": "SELECT AVG(defense) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "avg_support": "SELECT AVG(support) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_kills": "SELECT SUM(kills) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_teamkills": "SELECT SUM(teamkills) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_deaths": "SELECT SUM(deaths) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "tot_deaths_by_tk": "SELECT SUM(deaths_by_tk) FROM public.player_stats WHERE playersteamid_id = :db_player_id",
    "most_killed": """
        SELECT key AS player_name, SUM(value::int) AS total_kills, count(*)
        FROM public.player_stats, jsonb_each_text(most_killed)
        WHERE playersteamid_id = :db_player_id
        GROUP BY key
        ORDER BY total_kills DESC
        LIMIT 3
    """,
    "most_death_by": """
        SELECT key AS player_name, SUM(value::int) AS total_kills, count(*)
        FROM public.player_stats, jsonb_each_text(death_by)
        WHERE playersteamid_id = :db_player_id
        GROUP BY key
        ORDER BY total_kills DESC
        LIMIT 3
    """,
}
DETAILED_DISPLAY_QUERIES = {
    **BASIC_DISPLAY_QUERIES,
    "most_used_weapons": """
        SELECT weapon, SUM(usage_count) AS total_usage
        FROM (
            SELECT playersteamid_id, weapon_data.key AS weapon,
            (weapon_data.value::text)::int AS usage_count
            FROM public.player_stats,
            jsonb_each(weapons::jsonb) AS weapon_data
            WHERE playersteamid_id = :db_player_id
        ) AS weapon_usage
        GROUP BY weapon
        ORDER BY total_usage DESC
        LIMIT 3
    """,
}

def format_hours_minutes_seconds(hours: int, minutes: int, seconds: int) -> str:
    """
    Formats the hours, minutes, and seconds as XXhXXmXXs.
    """
    return f"{int(hours):02d}h{int(minutes):02d}m{int(seconds):02d}s"

def readable_duration(seconds: int) -> str:
    """
    Returns a human-readable string (years, months, days, XXhXXmXXs)
    from a number of seconds.
    """
    seconds = int(seconds)

    years, remaining_seconds_in_year = divmod(seconds, SECONDS_IN_YEAR)
    months, remaining_seconds_in_month = divmod(remaining_seconds_in_year, SECONDS_IN_MONTH)
    days, remaining_seconds_in_day = divmod(remaining_seconds_in_month, SECONDS_IN_DAY)
    hours, remaining_seconds_in_hour = divmod(remaining_seconds_in_day, SECONDS_IN_HOUR)
    minutes, remaining_seconds = divmod(remaining_seconds_in_hour, SECONDS_IN_MINUTE)

    time_string = []

    if years > 0:
        time_string.append(f"{years} {TRANSL['years'][LANG]}")
    if months > 0:
        time_string.append(f"{months} {TRANSL['months'][LANG]}")
    if days > 0:
        time_string.append(f"{days} {TRANSL['days'][LANG]}")

    if any([years, months, days]):
        time_string.append(",")

    time_string.append(format_hours_minutes_seconds(hours, minutes, remaining_seconds))

    return " ".join(filter(None, time_string))

def get_player_profile_data(player_id):
    """
    Retrieves the main data of the player profile.
    """
    player_profile_data = get_player_profile(player_id=player_id, nb_sessions=0)
    if not player_profile_data:
        logger.error("No player_profile_data")
        return None
    return player_profile_data

def get_queries():
    """
    Returns the necessary queries according to the DETAILED_DISPLAY configuration.
    """
    return DETAILED_DISPLAY_QUERIES if DETAILED_DISPLAY else BASIC_DISPLAY_QUERIES

def fetch_player_id(sess, player_id):
    """
    Retrieve the player ID from the database.
    """
    player_id_query = "SELECT s.id FROM steam_id_64 AS s WHERE s.steam_id_64 = :player_id"
    result = sess.execute(text(player_id_query), {"player_id": player_id}).fetchone()
    if not result:
        logger.error(f"No db_player_id for {player_id}")
        return None
    return result[0]

def execute_queries(sess, queries, params):
    """
    Runs queries to retrieve statistics.
    """
    results = {}
    for key, query in queries.items():
        results[key] = sess.execute(text(query), params).fetchall()
    return results

def get_player_database_stats(player_id):
    """
    Retrieves statistics from the database for a given player.
    """
    queries = get_queries()
    with enter_session() as sess:
        db_player_id = fetch_player_id(sess, player_id)
        if db_player_id is None:
            return None
        params = {"db_player_id": db_player_id}
        return execute_queries(sess, queries, params)

def format_top_results(rows, limit, pattern):
    return "\n".join(pattern.format(*row) for row in rows[:limit])

def generate_simplified_message(player_name, player_profile_data, database_stats):
    """
    Generates a simplified message for console servers.
    """
    total_playtime_seconds = player_profile_data["total_playtime_seconds"]

    tot_games = int(database_stats["tot_games"][0][0])
    avg_combat = round(float(database_stats["avg_combat"][0][0]), 2)
    avg_support = round(float(database_stats["avg_support"][0][0]), 2)
    avg_offense = round(float(database_stats["avg_offense"][0][0]), 2)
    avg_defense = round(float(database_stats["avg_defense"][0][0]), 2)
    tot_kills = int(database_stats["tot_kills"][0][0])
    tot_teamkills = int(database_stats["tot_teamkills"][0][0])
    tot_deaths = int(database_stats["tot_deaths"][0][0])
    most_killed = format_top_results(database_stats["most_killed"], 1, "{} : {:d} ({:d} games)")
    most_death_by = format_top_results(database_stats["most_death_by"], 1, "{} : {:d} ({:d} games)")

    ratio_kd = round(((tot_kills - tot_teamkills) / max(1, tot_deaths)), 2)

    message = (
        f"▒ {player_name} ▒\n"
        f"{TRANSL['playedgames'][LANG]} : {tot_games}\n"
        f"{TRANSL['cumulatedplaytime'][LANG]} : {readable_duration(total_playtime_seconds)}\n"
        "\n"
        f"▒ {TRANSL['statistics'][LANG]} ▒\n"
        f"{TRANSL['combat'][LANG]} : {avg_combat} ; {TRANSL['support'][LANG]} : {avg_support}\n"
        f"{TRANSL['offense'][LANG]} : {avg_offense} ; {TRANSL['defense'][LANG]} : {avg_defense}\n"
        "\n"
        f"{tot_kills} {TRANSL['kills'][LANG]} / {tot_deaths} {TRANSL['deaths'][LANG]} = {ratio_kd} {TRANSL['ratio'][LANG]}\n"
        "\n"
        f"{TRANSL['victims'][LANG]} : {most_killed}\n"
        f"{TRANSL['nemesis'][LANG]} : {most_death_by}"
    )

    return message

def get_penalties_message(player_profile_data):
    kicks = player_profile_data["penalty_count"]["KICK"]
    punishes = player_profile_data["penalty_count"]["PUNISH"]
    tempbans = player_profile_data["penalty_count"]["TEMPBAN"]

    penalties_message = ""
    if kicks == 0 and punishes == 0 and tempbans == 0:
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
            penalties_message += f"{tempbans} tempbans"

    return penalties_message

def generate_detailed_message(player_name, player_profile_data, database_stats):
    """
    Generates a detailed message with all available statistics.
    """
    created = player_profile_data['created']
    sessions_count = player_profile_data['sessions_count']
    total_playtime_seconds = player_profile_data["total_playtime_seconds"]

    tot_games = int(database_stats["tot_games"][0][0])
    avg_combat = round(float(database_stats["avg_combat"][0][0]), 2)
    avg_support = round(float(database_stats["avg_support"][0][0]), 2)
    avg_offense = round(float(database_stats["avg_offense"][0][0]), 2)
    avg_defense = round(float(database_stats["avg_defense"][0][0]), 2)
    tot_kills = int(database_stats["tot_kills"][0][0])
    tot_teamkills = int(database_stats["tot_teamkills"][0][0])
    tot_deaths = int(database_stats["tot_deaths"][0][0])
    tot_deaths_by_tk = int(database_stats["tot_deaths_by_tk"][0][0])
    most_used_weapons = format_top_results(database_stats["most_used_weapons"], 3, "{} ({:d} kills)")
    most_killed = format_top_results(database_stats["most_killed"], 3, "{} : {:d} ({:d} games)")
    most_death_by = format_top_results(database_stats["most_death_by"], 3, "{} : {:d} ({:d} games)")

    elapsed_time_seconds = (datetime.now() - datetime.fromisoformat(str(created))).total_seconds()
    ratio_kd = round(((tot_kills - tot_teamkills) / max(1, tot_deaths)), 2)

    message = (
        f"▒ {player_name} ▒\n"
        "\n"
        f"{TRANSL['firsttimehere'][LANG]}\n"
        f"{readable_duration(elapsed_time_seconds)}\n"
        f"{TRANSL['gamesessions'][LANG]} : {sessions_count}\n"
        f"{TRANSL['playedgames'][LANG]} : {tot_games}\n"
        "\n"
        f"{TRANSL['cumulatedplaytime'][LANG]}\n"
        f"{readable_duration(total_playtime_seconds)}\n"
        f"({TRANSL['averagesession'][LANG]} : {readable_duration(total_playtime_seconds/sessions_count)})\n"
        "\n"
        f"▒ {TRANSL['punishments'][LANG]} ▒\n"
        f"{get_penalties_message(player_profile_data)}\n"
        "\n"
        f"▒ {TRANSL['averages'][LANG]} ▒\n"
        f"{TRANSL['combat'][LANG]} : {avg_combat} ; {TRANSL['support'][LANG]} : {avg_support}\n"
        f"{TRANSL['offense'][LANG]} : {avg_offense} ; {TRANSL['defense'][LANG]} : {avg_defense}\n"
        "\n"
        f"▒ {TRANSL['totals'][LANG]} ▒\n"
        f"{TRANSL['kills'][LANG]} : {tot_kills} ({tot_teamkills} TKs)\n"
        f"{TRANSL['deaths'][LANG]} : {tot_deaths} ({tot_deaths_by_tk} TKs)\n"
        f"{TRANSL['ratio'][LANG]} {TRANSL['kills'][LANG]}/{TRANSL['deaths'][LANG]} : {ratio_kd}\n"
        "\n"
        f"▒ {TRANSL['favoriteweapons'][LANG]} ▒\n"
        f"{most_used_weapons}\n"
        "\n"
        f"▒ {TRANSL['victims'][LANG]} ▒\n"
        f"{most_killed}\n"
        "\n"
        f"▒ {TRANSL['nemesis'][LANG]} ▒\n"
        f"{most_death_by}"
    )
    return message

def all_time_stats(rcon: Rcon, struct_log: StructuredLogLineWithMetaData):
    """
    Collects and displays statistics based on the DETAILED_DISPLAY variable.
    """
    if not (player_id := struct_log["player_id_1"]) or not (player_name := struct_log["player_name_1"]):
        logger.error("No player_id or player_name")
        return

    try:
        player_profile_data = get_player_profile_data(player_id)
        if player_profile_data is None:
            return

        database_stats = get_player_database_stats(player_id)
        if database_stats is None:
            return

        if DETAILED_DISPLAY:
            message = generate_detailed_message(player_name, player_profile_data, database_stats)
        else:
            message = generate_simplified_message(player_name, player_profile_data, database_stats)

        rcon.message_player(
            player_name=player_name,
            player_id=player_id,
            message=message,
            by="all_time_stats",
            save_message=False
        )

    except Exception as error:
        logger.error(error, exc_info=True)

def all_time_stats_on_connected(rcon: Rcon, struct_log: StructuredLogLineWithMetaData):
    """
    Call the message on player's connection
    """
    if DISPLAY_ON_CONNECT:
        all_time_stats(rcon, struct_log)

def all_time_stats_on_chat_command(rcon: Rcon, struct_log: StructuredLogLineWithMetaData):
    """
    Call the message on chat command
    """
    if not (chat_message := struct_log["sub_content"]):
        logger.error("No sub_content in CHAT log")
        return
    if chat_message in CHAT_COMMAND:
        all_time_stats(rcon, struct_log)

logger = logging.getLogger('rcon')