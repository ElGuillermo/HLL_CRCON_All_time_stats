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

# Should we display the stats to every player on connect ?
# True / False
DISPLAY_ON_CONNECT = True

# The command(s) the players have to enter in chat to display their stats
# You can have multiple commands
# ex : ["!me", "!mystats"]
# Note : the command is not case sensitive (ie : '!me' or '!ME' will work the same)
CHAT_COMMAND = ["!me"]

# Stats to display
# ----------------------------------------
# Hosting a console game server ? - You only have 16 lines available :/
# Suggestion 1 (16 lines) : Totals (4), all 3 most_* (12)
# Suggestion 2 (16 lines) : playername (1), cumulatedplaytime (2), tot_playedgames (2), avg_sessiontime (2), Averages (5), Totals (4)
STATS_TO_DISPLAY = {
    "playername": True,         # 1 line
    "firsttimehere": True,      # 1+1 lines
    "tot_sessions": True,       # 1+1 lines
    "tot_playedgames": True,    # 1+1 lines
    "cumulatedplaytime": True,  # 1+1 lines
    "avg_sessiontime": True,    # 1+1 lines

    # Averages (header)         # 1 line (if any of following 4 is enabled)
    "avg_combat": True,         # 1 line
    "avg_offense": True,        # 1 line
    "avg_defense": True,        # 1 line
    "avg_support": True,        # 1 line

    # Totals (header)           # 1 line (if any of following 5 is enabled)
    "tot_kills": True,          # 1 line
    "tot_teamkills": True,      # 0 line (same line as tot_kills)
    "tot_deaths": True,         # 1 line
    "tot_deaths_by_tk": True,   # 0 line (same line as tot_deaths)
    "kd_ratio": True,           # 1 line

    "most_killed": True,        # Victims : 1+3 lines (max)
    "most_death_by": True,      # Nemesis : 1+3 lines (max)
    "most_used_weapons": True,  # Favorite weapons : 1+3 lines (max)

    "tot_punishments": True,    # 1+3 lines (max)
}

# Should we display seconds in the durations ?
# True or False
# Recommended : False
DISPLAY_SECS = False
