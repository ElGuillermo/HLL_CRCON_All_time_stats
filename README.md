# HLL_CRCON_All_time_stats

A plugin for Hell Let Loose (HLL) CRCON (see : https://github.com/MarechJ/hll_rcon_tool)  
that displays statistic data about the player, either  
- on connect
- when asking for them in chat (`!me`) ;

Available in english, french, german, spanish, polish, brazilian portuguese, russian and chinese.

<img width="3826" height="1167" alt="image" src="https://github.com/user-attachments/assets/e4b6302e-5db2-4842-afc0-9cabae7a732b" />

## Install

> [!NOTE]
> The shell commands given below assume your CRCON is installed in `/root/hll_rcon_tool`.  

- Log into your CRCON host machine using SSH

- Download the tool (enter these commands, one line at a time)  
  ```shell
  cd /root/hll_rcon_tool

  wget -O https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_restart/refs/heads/main/restart.sh

  mkdir -p custom_tools
  
  cd /root/hll_rcon_tool/custom_tools

  wget -O https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_All_time_stats/refs/heads/main/hll_rcon_tool/custom_tools/all_time_stats.py

  wget -O https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_All_time_stats/refs/heads/main/hll_rcon_tool/custom_tools/all_time_stats_config.py

  wget -O https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_custom_common_translations.py/refs/heads/main/common_translations.py
  ```
- Edit `/root/hll_rcon_tool/rcon/hooks.py` and add these lines:
  - (in the import part, on top of the file)
    ```python
    import custom_tools.all_time_stats as all_time_stats
    ```
  - (at the very end of the file)
    ```python
    @on_connected()
    def alltimestats_on_connected(rcon: Rcon, struct_log: StructuredLogLineWithMetaData):
        all_time_stats.all_time_stats_on_connected(rcon, struct_log)

    @on_chat
    def alltimestats_on_chat_command(rcon: Rcon, struct_log: StructuredLogLineWithMetaData):
        all_time_stats.all_time_stats_on_chat_command(rcon, struct_log)
    ```

## Config
- Edit `/root/hll_rcon_tool/custom_tools/all_time_stats_config.py` and set the parameters to fit your needs.
- Restart CRCON :
  ```shell
  cd /root/hll_rcon_tool

  sh ./restart.sh
  ```
  If you don't want to use the `restart.sh` script, you can rebuild containers and restart CRCON using Docker commands :  
  ```shell
  cd /root/hll_rcon_tool

  sudo docker compose build && sudo docker compose down && sudo docker compose up -d --remove-orphans
  ```

## Limitations
⚠️ Any change to these files requires a CRCON rebuild and restart (using the `restart.sh` script) to be taken in account :
- `/root/hll_rcon_tool/custom_tools/all_time_stats.py`
- `/root/hll_rcon_tool/custom_tools/all_time_stats_config.py`
- `/root/hll_rcon_tool/custom_tools/common_translations.py`
- `/root/hll_rcon_tool/rcon/hooks.py`

⚠️ This plugin requires a modification of the `/root/hll_rcon_tool/rcon/hooks.py` file, which originates from the official CRCON depot.  
If any CRCON upgrade implies updating this file, the usual upgrade procedure, as given in official CRCON instructions, will **FAIL**.  
To successfully upgrade your CRCON, you'll have to revert the changes back, then reinstall this plugin.  
To revert to the original file :  
```shell
cd /root/hll_rcon_tool
git restore rcon/hooks.py
```
