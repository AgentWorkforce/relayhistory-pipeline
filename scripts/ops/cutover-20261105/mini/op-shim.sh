#!/bin/bash
# Forwards op to kjg-lap (Khaliq-approved 1Password path). No secrets stored here.
args=$(printf "%q " "$@")
exec ssh -o BatchMode=yes kjg-lap "export PATH=/opt/homebrew/bin:\$PATH; op $args"
