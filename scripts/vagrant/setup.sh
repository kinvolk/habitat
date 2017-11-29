#!/bin/sh

set -euo pipefail

vagrant destroy -f

vagrant up

vagrant ssh -c "sudo su - && cd /src && direnv allow && make build-bin build-srv"

