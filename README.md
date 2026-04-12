# HLL_CRCON_All_time_stats

Unofficial plugin for the Hell Let Loose (HLL) [CRCON](https://github.com/MarechJ/hll_rcon_tool)

### Displays players historical stats on this game server.

![HLL_CRCON_All_time_stats](https://github.com/user-attachments/assets/e4b6302e-5db2-4842-afc0-9cabae7a732b)

---

## Features

- Select the servers on which the script will be activated. (ex : only on 1, 2, 3 and 7).
- Select the stats you want to see displayed.
- Stats can be displayed on player's connexion.
- Stats can be displayed anytime using a configurable chat command (default: `!me`).
- Available translations : english, french, spanish, german, russian, brazilian portuguese, polish and chinese.

---

> [!IMPORTANT]
> - The shell commands given below assume your CRCON is installed in `/root/hll_rcon_tool`  
>   You may have installed your CRCON in a different folder.  
>   If so, you'll have to adapt the commands below accordingly.
>
> - Always copy/paste/execute commands :warning: one line at a time :warning:

## Installation

### 1/3 - Log into your CRCON host machine using SSH

- See [this guide](https://github.com/MarechJ/hll_rcon_tool/wiki/Troubleshooting-&-Help-‐-Common-procedures-‐-How-to-enter-a-SSH-terminal) if you need help to do it.

### 2/3 - Download files

  - Copy/paste/execute these commands :  
    ```shell
    cd /root/hll_rcon_tool
    ```  
    ```shell
    wget -N https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_restart/refs/heads/main/restart.sh
    ```  
    ```shell
    mkdir -p custom_tools
    ```  
    ```shell
    cd /root/hll_rcon_tool/custom_tools
    ```  
    ```shell
    wget -N https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_custom_common_translations.py/refs/heads/main/common_translations.py
    ```  
    ```shell
    wget -N https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_All_time_stats/refs/heads/main/hll_rcon_tool/custom_tools/all_time_stats.py
    ```  
    ```shell
    wget -N https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_All_time_stats/refs/heads/main/hll_rcon_tool/custom_tools/all_time_stats_config.py
    ```

### 3/3 - Edit `/root/hll_rcon_tool/rcon/hooks.py`

  - Add this line in the import part, on top of the file
    ```python
    import custom_tools.all_time_stats as all_time_stats
    ```
  - Add these lines at the very bottom of the file
    ```python
    @on_connected()
    def alltimestats_on_connected(rcon: Rcon, struct_log: StructuredLogLineWithMetaData):
        all_time_stats.all_time_stats_on_connected(rcon, struct_log)

    @on_chat
    def alltimestats_on_chat_command(rcon: Rcon, struct_log: StructuredLogLineWithMetaData):
        all_time_stats.all_time_stats_on_chat_command(rcon, struct_log)
    ```

---

## Configuration

### 1/2 - Edit `/root/hll_rcon_tool/custom_tools/all_time_stats_config.py`

- Set the parameters to fit your needs (see inner comments for guidance).

### 2/2 - Rebuild and restart CRCON Docker containers

- Copy/paste/execute these commands :  
  ```shell
  cd /root/hll_rcon_tool
  ```  
  ```shell
  sh ./restart.sh
  ```

> [!TIP]
> 
>  If you don't want to use the `restart.sh` script :  
>  - Copy/paste/execute these commands :  
>  ```shell
>  cd /root/hll_rcon_tool
>  ```  
>  ```shell
>  sudo docker compose build && sudo docker compose down && sudo docker compose up -d --remove-orphans
>  ```

---

## Maintenance

### Disable this plugin

- Revert the changes made in [Installation 3/3](#33---edit-roothll_rcon_toolrconhookspy)

--

### Modify code or settings

:exclamation: Any change to these files requires to rebuild and restart CRCON Docker containers (same procedure as in [Configuration 2/2](#22---rebuild-and-restart-crcon-docker-containers)) :  
- `/root/hll_rcon_tool/rcon/hooks.py`
- `/root/hll_rcon_tool/custom_tools/common_translations.py`
- `/root/hll_rcon_tool/custom_tools/all_time_stats.py`
- `/root/hll_rcon_tool/custom_tools/all_time_stats_config.py`

--

### Upgrade CRCON

This plugin requires a modification of original CRCON file(s).  
:exclamation: If any CRCON update contains a new version of this file(s), the usual CRCON upgrade procedure will **FAIL**.

#### Restore the original CRCON file(s)

- Copy/paste/execute these commands : 
  ```shell
  cd /root/hll_rcon_tool
  ```  
  ```shell
  cp rcon/hooks.py rcon/hooks.py.backup
  ```  
  ```shell
  git restore rcon/hooks.py
  ```

#### Upgrade

- Follow the official upgrade instructions given in the new CRCON version announcement.
- Don't restart CRCON Docker containers yet (don't execute `docker compose up -d`).

#### Reapply changes

- copy/paste the changes from  
  `/root/hll_rcon_tool/rcon/hooks.py.backup`  
  into  
  `/root/hll_rcon_tool/rcon/hooks.py`
- Rebuild and restart CRCON Docker containers (same procedure as in [Configuration 2/2](#22---rebuild-and-restart-crcon-docker-containers)).
- If anything works as intended, you can delete the backup file :
  - Copy/paste/execute these commands :  
    ```
    cd /root/hll_rcon_tool
    ```  
    ```shell
    rm rcon/hooks.py.backup
    ```
