FROM ubuntu:latest

ARG HAB_AUTH_TOKEN=UNDEFINED
ARG HAB_BLDR_URL=http://localhost:9636
ARG HAB_ORIGIN=UNDEFINED

RUN apt-get update -y && \
    apt-get install sudo git curl postgresql -y

# curl https://sh.rustup.rs -sSf > rustup_init.sh
# sh rustup_init.sh -y

#source $HOME/.cargo/env

RUN git clone https://github.com/habitat-sh/habitat

WORKDIR habitat
RUN git checkout 0.38.0

RUN echo $PWD
RUN echo $(ls)
RUN cp components/hab/install.sh /tmp/

RUN sh support/linux/install_dev_0_ubuntu_latest.sh

RUN sh support/linux/install_dev_9_linux.sh

RUN . ~/.profile

RUN make build-srv

RUN mkdir -p $HOME/habitat

RUN cat <<EOF > $HOME/habitat/config_api.toml \
[depot] \
key_dir = "$HOME/.hab/cache/keys" \
EOF

RUN cat <<EOF > $HOME/habitat/config_jobsrv.toml \
key_dir = "$HOME/.hab/cache/keys" \
\
[archive] \
backend = "local" \
local_dir = "/tmp" \
EOF

RUN cat <<EOF > $HOME/habitat/config_sessionsrv.toml \
[permissions] \
admin_team = 1995301 \
build_worker_teams = [1995301] \
early_access_teams = [1995301] \
EOF

RUN cat <<EOF > $HOME/habitat/config_worker.toml \
auth_token = "" # your github auth token \
bldr_url = "http://localhost:9636" \
auto_publish = true \
EOF

RUN sed -i -e 's/bldr-api start --path \/tmp\/depot$/bldr-api start --path \/tmp\/depot --config \$HOME\/habitat\/config_api.toml/' support/Procfile && \
    sed -i -e 's/bldr-jobsrv start$/bldr-jobsrv start --config \$HOME\/habitat\/config_jobsrv.toml/' support/Procfile && \
sed -i -e 's/bldr-sessionsrv start$/bldr-sessionsrv start --config \$HOME\/habitat\/config_sessionsrv.toml/' support/Procfile && \
sed -i -e 's/bldr-worker start$/bldr-worker start --config \$HOME\/habitat\/config_worker.toml/' support/Procfile

CMD ["make bldr-run"]
