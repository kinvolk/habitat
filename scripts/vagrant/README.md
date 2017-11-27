# Vagrant setup

## Prerequisites

Get the secrets from Michael (private URL) and place them under
`$PROJECT_ROOT/.secrets/`. You should have two files there:

```
ls .secrets/
builder-github-app.pem  habitat-env
```

You need to update `components/builder-web/habitat.conf.sample.js` with the
`github_client_id` and `github_app_id` from the `.secrets/habitat-env` file
above. You also need to set the `github_app_url` to
`https://github.com/apps/habitat-dev-local`.

To avoid a cached "bad" `habitat.conf.js` file, delete it:

```
rm components/builder-web/habitat.conf.js
```

## Setup

From project root run:

```
vagrant destroy -f
vagrant up
vagrant ssh
```

Then, in the VM:

```
sudo su -
tmux # if you want :)
cd /src
direnv allow .
make build-bin build-srv
[...]
make bldr-run-no-build
```

Now go to http://localhost:3000/#/pkgs and click Sign-in. You should be
redirected to GitHub and asked if you allow the `habitat-dev-local`
GitHub app. You should be redirected to Habitat Builder (i.e. localhost:3000)
and be logged-in.

## Troubleshooting

* Logs are very verbose by default. Remove `RUST_LOG=debug,` from
  `support/bldr.env` to suppress `DEBUG` logs.
