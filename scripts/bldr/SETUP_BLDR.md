How to build Habitat Builder on Linux
===

In this document it's assumed that you are building Habitat Builder on Linux.
This document does not cover other OS environments like Mac OSX, or
Docker-based builds.
This document also does not cover every corner case during the builds.
It aims at a short introduction to how to build Habitat Builder.

## Preparation

First of all, please make sure that basic programs are already installed,
such as git, rust, cargo, etc.

Clone a git repo under `$HOME/Dev/`.

```
cd $HOME/Dev
git clone https://github.com/habitat-sh/habitat
```

Check out the latest stable version, because sometimes the master branch
can include bugs that prevent you from running Habitat Builder correctly.
As of 2017-11-15 the latest release is 0.38.0.

```
cd $HOME/Dev/habitat
git checkout 0.38.0
```

Copy an install script to `/tmp`.

```
cp components/hab/install.sh /tmp/
```

Run one of the distro-specific scripts `support/linux/install_dev_0_*.sh`,
for example choose a centos script, if you run it on Fedora.

```
sh support/linux/install_dev_0_centos_7.sh
```

Run also the common Linux scripts.

```
sh support/linux/install_dev_9_linux.sh
. ~/.profile
```

Build Habitat Builder services.

```
make build-srv
```

If everything is built without any error, you are ready to run Habitat Builder.
Before running that though, you also need to generate several config files.

## Generate configs

Prepare a Habitat config directory.

```
mkdir -p $HOME/habitat
```

Export the following environment variables:
Note that the origin and the auth token will vary for each user.

```
export HAB_AUTH_TOKEN=<your github token>
export HAB_BLDR_URL=http://localhost:9636
export HAB_ORIGIN=<your origin>
```

Create Habitat configs under `$HOME/habitat`.

First `config_api.toml`.

```
cat <<EOF > $HOME/habitat/config_api.toml
[depot]
key_dir = "$HOME/.hab/cache/keys"
EOF
```

`config_jobsrv.toml`

```
cat <<EOF > $HOME/habitat/config_jobsrv.toml
key_dir = "$HOME/.hab/cache/keys"

[archive]
backend = "local"
local_dir = "/tmp"
EOF
```

`config_sessionsrv.toml`


```
cat <<EOF > $HOME/habitat/config_sessionsrv.toml
[permissions]
admin_team = 1995301
build_worker_teams = [1995301]
early_access_teams = [1995301]
EOF
```

`config_worker.toml`.
Note that `auth_token` needs be the token given to `HAB_AUTH_TOKEN`.

```
cat <<EOF > $HOME/habitat/config_worker.toml
auth_token = "" # your github auth token
bldr_url = "http://localhost:9636"
auto_publish = true
EOF
```

Now change `support/Procfile` to make it read the config files created above.

```
sed -i -e 's/bldr-api start --path \/tmp\/depot$/bldr-api start --path \/tmp\/depot --config \$HOME\/habitat\/config_api.toml/' support/Procfile
sed -i -e 's/bldr-jobsrv start$/bldr-jobsrv start --config \$HOME\/habitat\/config_jobsrv.toml/' support/Procfile
sed -i -e 's/bldr-sessionsrv start$/bldr-sessionsrv start --config \$HOME\/habitat\/config_sessionsrv.toml/' support/Procfile
sed -i -e 's/bldr-worker start$/bldr-worker start --config \$HOME\/habitat\/config_worker.toml/' support/Procfile
```

## Run the Builder services

Now run the Habitat Builder.

```
sudo -E make bldr-run
```

Then you will probably be able to see that 7 bldr processes in total are
running in the background, like `bldr-api` or `bldr-admin`.

## Kill all the Builder daemons

The 7 bldr processes continue to run, even when you stopped `make bldr-run`
process. There's no fancy way to kill all bldr daemons. So it's recommended to
create a simple bash script like the following:

```
cat <<EOF > $HOME/bin/kill-bldr
#!/bin/bash

for name in api admin router jobsrv sessionsrv originsrv worker; do
    sudo killall bldr-$name;
done
EOF
chmod +x $HOME/bin/kill-bldr
```

## Modify and rebuild one of the Builder components

Let's assume that you want to change `components/builder-worker/src/runner/docker.rs`.

```
$ vim components/builder-worker/src/runner/docker.rs
# ... (change and save) ...
```

Then you need to go to the `builder-worker` directory to build one of
the components.

```
$ pushd components/builder-worker
$ cargo build
$ popd
$ ls -l target/debug/bldr-worker
-rwxrwxr-x 2 dpark dpark 83213216 Nov 15 16:21 target/debug/bldr-worker
```

It's clear that bldr-worker was rebuilt. Now make sure that you killed all
bldr processes, and run bldr again.

```
~/bin/kill-bldr
sudo -E make bldr-run
```
