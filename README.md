# HLL_CRCON_All_time_stats EN ESPAÑOL CHILENO

A plugin for Hell Let Loose (HLL) CRCON (see : https://github.com/MarechJ/hll_rcon_tool)  
that displays statistic data about the player, either  
- on connect
- when asking for them in chat (`!me`) ;

![375490122-d8c7be50-aa6e-4949-b789-c327cacb2a1a](https://github.com/user-attachments/assets/4e9105d9-f87b-40e9-a489-da74cbb8f267)

## Install

> [!NOTE]
> The shell commands given below assume your CRCON is installed in `/root/hll_rcon_tool`.  
> You may have installed your CRCON in a different folder.  
>   
> Some Ubuntu Linux distributions disable the `root` user and `/root` folder by default.  
> In these, your default user is `ubuntu`, using the `/home/ubuntu` folder.  
> You should then find your CRCON in `/home/ubuntu/hll_rcon_tool`.  
>   
> If so, you'll have to adapt the commands below accordingly.

- Log into your CRCON host machine using SSH and enter these commands (one line at at time) :  

  First part  
  If you already have installed any other "custom tools" from ElGuillermo, you can skip this part.  
  (though it's always a good idea to redownload the files, as they could have been updated)
  ```shell
  cd /root/hll_rcon_tool
  wget https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_restart/refs/heads/main/restart.sh
  mkdir custom_tools
  ```
  Second part
  ```shell
  cd /root/hll_rcon_tool/custom_tools
  wget https://raw.githubusercontent.com/ElGuillermo/HLL_CRCON_All_time_stats/refs/heads/main/hll_rcon_tool/custom_tools/all_time_stats.py
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
- Edit `/root/hll_rcon_tool/custom_tools/all_time_stats.py` and set the parameters to fit your needs.
- Restart CRCON :
  ```shell
  cd /root/hll_rcon_tool
  sh ./restart.sh
  ```

## Limitations
⚠️ Any change to these files requires a CRCON rebuild and restart (using the `restart.sh` script) to be taken in account :
- `/root/hll_rcon_tool/custom_tools/all_time_stats.py`
- `/root/hll_rcon_tool/rcon/hooks.py`

⚠️ This plugin requires a modification of the `/root/hll_rcon_tool/rcon/hooks.py` file, which originates from the official CRCON depot.  
If any CRCON upgrade implies updating this file, the usual upgrade procedure, as given in official CRCON instructions, will **FAIL**.  
To successfully upgrade your CRCON, you'll have to revert the changes back, then reinstall this plugin.  
To revert to the original file :  
```shell
cd /root/hll_rcon_tool
git restore rcon/hooks.py
```

⚠️Nota importante ESPAÑOL⚠️


Es muy importante que actualices el sed de docker antes de ejecutar :

```shell
cd /root/hll_rcon_tool
sed -i 's/node:20-buster-slim/node:20-bookworm-slim/g' Dockerfile-frontend
```
Busca todas las menciones de la imagen de Docker node:20-buster-slim dentro del archivo Dockerfile-frontend y las reemplaza por node:20-bookworm-slim, guardando los cambios directamente en el archivo (si usas Debian).

Si está ok, entonces realiza:

```shell
sh ./restart.sh 
```
o en su defecto:

```shell
sudo docker compose down
sudo docker compose build
sudo docker compose up -d --remove-orphans
```


⚠️ Important Note ESP/ENG ⚠️

It is very important that you update the Docker sed before running:

```shell
cd /root/hll_rcon_tool
sed -i 's/node:20-buster-slim/node:20-bookworm-slim/g' Dockerfile-frontend
```

This command finds all instances of the Docker image node:20-buster-slim in the Dockerfile-frontend file and replaces them with node:20-bookworm-slim, saving the changes directly to the file (especially relevant if you are using Debian).

If it completes successfully, then run:

```shell
sh ./restart.sh 
```

or alternatively:

```shell
sudo docker compose down
sudo docker compose build
sudo docker compose up -d --remove-orphans
```
