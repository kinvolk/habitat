Vagrant setup
===

## Prerequisites

- Get the secrets from https://gist.github.com/indradhanush/184f19d26ff96ff537e336dd13c63c64 and place them under `$PROJECT_ROOT/.secrets/`

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
- Please make sure that the web interface daemon `lite-server` is running. If not, try to run `support/builder_web.sh`
