#!/usr/bin/env bash

[ ! -d "${HOME}/backup-logs" ] && mkdir "${HOME}/backup-logs"

mv logs/*.out "${HOME}/backup-logs"

rm -rf logs
mkdir logs