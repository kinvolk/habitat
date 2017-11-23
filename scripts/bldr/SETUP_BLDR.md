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

Let's assume that your git repo is supposed to be located under `$HOME/Dev`.
Make sure that all parent directories of $HOME/Dev have executable bits,
so that the `hab` user is able to write logs into the habitat repo.

```
sudo chmod 755 /home
sudo chmod 755 $HOME
sudo chmod 755 $HOME/Dev
```

Clone a git repo under `$HOME/Dev/`.

```
cd $HOME/Dev
git clone https://github.com/habitat-sh/habitat
```

There are two possible versions that you can try: 0.40 (master) and
0.38 (stable release).

### 0.40 (master, bleeding-edge)

Check out the master branch, which could be unstable for some use cases,
but it provides definitely better development environments.

```
cd $HOME/Dev/habitat
git checkout master
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

### Generate configs

Prepare a Habitat config directory.

```
mkdir -p $HOME/habitat
```

Export the following environment variables:
Note that the origin and the auth token will vary for each user.

```
export HAB_AUTH_TOKEN=<your github token>
export HAB_ORIGIN=<your origin>
```

#### Github app secrets

You also need to get `app_id`,
`client_id`, and `client_secret` to your own secrets generated from
your github account. For details please see
[Registering github apps](https://developer.github.com/apps/building-integrations/setting-up-and-registering-github-apps/registering-github-apps/).

On your github account page, generate a private key, and save it to
`$PROJECT_ROOT/.secrets/builder-github-app.pem`.

Set the environment variables.

```
export GITHUB_CLIENT_ID="Iv1.0123456789012345"
export GITHUB_CLIENT_SECRET="0123456789012345678901234567890123456789"
```

Update IDs in `support/builder/config.sh`.

```
client_id = "$GITHUB_CLIENT_ID"
client_secret = "$GITHUB_CLIENT_SECRET"
app_id = 1234
```

Habitat configs will be already available under `/hab/svc`.
In these default `user.toml` files.
For example, change configs like the following:

`/hab/svc/builder-api/user.toml`.

```
[github]
client_id = "Iv1.0123456789012345"
client_secret = "0123456789012345678901234567890123456789"
app_id = 1234
```

`/hab/svc/builder-api-proxy/user.toml`.

```
[github]
client_id = "Iv1.0123456789012345"
client_secret = "0123456789012345678901234567890123456789"
app_id = 1234
```

`/hab/svc/builder-sessionsrv/user.toml`.

```
[github]
client_id = "Iv1.0123456789012345"
client_secret = "0123456789012345678901234567890123456789"
app_id = 1234
```

`/hab/svc/builder-worker/user.toml`.
Note that `auth_token` needs be the token given to `HAB_AUTH_TOKEN`.


```
auth_token = "" # your github auth token
auto_publish = true

[github]
client_id = "Iv1.0123456789012345"
client_secret = "0123456789012345678901234567890123456789"
app_id = 1234
```

(optionally) you can reduce the amount of logs by specifying an env variable `RUST_LOG` in `support/bldr.env`:

```
RUST_LOG=debug,postgres=error,habitat_builder_db=error,hyper=error,habitat_builder_router=error,zmq=error,habitat_net=error
```

### 0.38 stable release

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

(optionally) you can reduce the amount of logs by specifying an env variable `RUST_LOG` in `support/bldr.env`:

```
RUST_LOG=debug,postgres=error,habitat_builder_db=error,hyper=error,habitat_builder_router=error,zmq=error,habitat_net=error
```

## Run the Builder services

Now run the Habitat Builder.

```
sudo -E PATH=$PATH make bldr-run-no-build
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
    sudo killall -9 bldr-$name;
done

sudo killall -9 hab-launch
sudo killall -9 hab-sup
sudo killall -9 lite-server
sudo killall -9 postmaster
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
