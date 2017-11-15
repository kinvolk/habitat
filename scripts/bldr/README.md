Automated habitat builder setup 
===

## Prerequisites

1. [Docker](https://docs.docker.com/engine/installation/)

2. [Habitat source](https://github.com/habitat-sh/habitat)

3. Follow the instructions
  [here](https://github.com/habitat-sh/habitat/blob/master/BUILDER_CONTAINER.md)
  to create a github org, teams et al.

4. `cd $HABITAT_PROJECT_ROOT`

5. Generate the `bldr-env.sh` file and populate it with values obtained in step 1. Also choose a value for `BLDR_IMAGE`. 
```
cat <<EOF > scripts/bldr/bldr-env.sh
BLDR_IMAGE=""
SKIP_BUILD=false

WORKER_AUTH_TOKEN=""

GITHUB_CLIENT_ID=""
GITHUB_CLIENT_SECRET=""

GITHUB_ADDR="github.com"
GITHUB_API_URL="https://api.github.com/api/v3"
GITHUB_WEB_URL="https://github.com"

GITHUB_ADMIN_TEAM=""
GITHUB_WORKER_TEAM=""
EOF
```

## Usage

1. `./run-bldr.sh`

2. Go get a cup of coffee/tea. This will take some time.


## Notes

- You will need changes from
  [this branch](https://github.com/kinvolk/habitat/tree/indradhanush/worker-bootstrap-for-bldr-dockerfile)
  to run the docker image successfully since these changes are not
  merged to upstream yet.
- By default the script will always bulid a new docker image. To
  disable it set `SKIP_BUILD=true` in `bldr-env.sh`


## Troubleshooting

- Don't see changes being reflected? Check the value of `SKIP_BUILD`.

