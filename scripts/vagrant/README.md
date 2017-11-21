Vagrant setup
===

## Prerequisites

- [Create a Github app](https://github.com/settings/apps) and download the private key from the app's page.
- Put it under `$PROJECT_ROOT/.secrets/builder-github-app.pem`
- Create a new file at `$PROJECT_ROOT/.secrets/habitat-env` and write the following into it:

```
export APP_HOSTNAME=localhost:3000

export GITHUB_API_URL=https://api.github.com
export GITHUB_WEB_URL=https://github.com

export GITHUB_APP_ID=""

export GITHUB_CLIENT_ID=""
export GITHUB_CLIENT_SECRET=""

export WORKER_AUTH_TOKEN=""

export GITHUB_ADMIN_TEAM=""
export GITHUB_WORKER_TEAM=""

export GITHUB_WEBHOOK_SECRET=""
```

- Ask a cowrecker for secrets (easy). Or generate your own (painful).

## One step

This is fragile. Recommended to go manual at the moment.
`./scripts/vagrant/setup.sh`

## Manual

From project root run:

`vagrant destroy -f && vagrant up && vagrant ssh`

### Inside vagrant:

```
hab origin key generate <your-origin-name>
sudo su -
direnv allow
cd /src
make build-bin build-srv
```

## Running bldr

```
vagrant ssh
sudo su -
cd /src
direnv allow
make bldr-run-no-build
```

## Testing the setup

Try running the http examples shown in [BUILDER_DEV.md](../../BUILDER_DEV.md)

## Troubleshooting

- Logs are very verbose by default. Remove `RUST_LOG=debug,` from `support/bldr.env` to suppress `DEBUG` logs.
