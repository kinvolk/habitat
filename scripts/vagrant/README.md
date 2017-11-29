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

Set the following env variables:

```
export HAB_AUTH_TOKEN=<your github auth token>
export HAB_ORIGIN=<your origin>
```

The two variables can be different for each user.
Make sure that your github auth token has the following permissions,
to avoid errors like `401 unauthorized`.

* user:email
* read:org

Please make sure that the system clock is set correctly, e.g. by running ntpd.
When the time is drifted a lot, github could reject requests so that hab-bldr
cannot continue to do anything at all.

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

You should now be able to connect a plan from GitHub.

## Building a package

To be able to build a package, your Vagrant instance needs to provide the
required `core/...` packages.

First, create an origin `core` for that.

Second, for each required package, download it from upstream Habitat Builder
(or build it yourself, if necessary) and upload the package to your local
instance. Example for `core/hab-backline`, which is always needed:

```
test -z "$HAB_BLDR_URL" || echo "\$HAB_BLDR_URL is set, unset it first"
hab pkg install core/hab-backline
hab pkg upload --url http://localhost:9636/v1 --auth "${HAB_AUTH_TOKEN}" "/hab/cache/artifacts/core-hab-backline-0.40.0-20171128175957-x86_64-linux.hart" --channel stable
# ... The package + all dependencies will be uploaded to your *local*
# core origin
```

Now, trigger a new build. For a package with no dependencies, above should
be enough. Otherwise, repeat the process for every package reported
missing during the build.

If the build fails due to a missing public key, make sure you have both
a public and a private key in the `/home/krangschnak/.hab/cache/keys/`
directory, e.g.

```
mkdir --parent /home/krangschnak/.hab/cache/keys
cp /hab/cache/keys/foo-20171103084851.* /home/krangschnak/.hab/cache/keys/
```

## Troubleshooting

* Logs are very verbose by default. Remove `RUST_LOG=debug,` from
  `support/bldr.env` to suppress `DEBUG` logs.
* In the bldr log, please ignore messages like below. This message shows up
  no matter whether the authentication succeeded or not:

```
sessionsrv.1 | WARN:habitat_builder_sessionsrv::server::handlers: Failed to obtain installation token, GitHub App Authentication error, message=Not Found, documentation_url=https://developer.github.com/v3/apps/#create-a-new-installation-token
```
