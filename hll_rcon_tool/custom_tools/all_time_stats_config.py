"""
all_time_stats_config.py

A plugin for HLL CRCON (https://github.com/MarechJ/hll_rcon_tool)
that displays a player's all-time stats on chat command and on player's connection.

Source : https://github.com/ElGuillermo

Feel free to use/modify/distribute, as long as you keep this note in your code
"""

# Configuration (you must review/change these !)
# -----------------------------------------------------------------------------

# Translation
# Available : 0 english, 1 french, 2 german,
#             3 spanish, 4 polish, 5 brazilian portuguese,
#             6 russian, 7 chinese
LANG = 0

# Can be enabled/disabled on your different game servers
# ie : ["1"]           = enabled only on server 1
#      ["1", "2"]      = enabled on servers 1 and 2
#      ["2", "4", "5"] = enabled on servers 2, 4 and 5
ENABLE_ON_SERVERS = ["1"]

# The command the players have to enter in chat to display their stats
# Note : the command is not case sensitive (ie : '!me' or '!ME' will work)
CHAT_COMMAND = ["!me"]

# Should we display the stats to every player on connect ?
# True or False
DISPLAY_ON_CONNECT = True

# Stats to display
# ----------------------------------------
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


# (End of configuration)
# -----------------------------------------------------------------------------
