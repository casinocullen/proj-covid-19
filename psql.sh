#!/bin/bash
start=$SECONDS
psql -h cori-risi.c6zaibvi9wyg.us-east-1.rds.amazonaws.com -U risi_fanniemae -d data -a -f $1
end=$SECONDS
laps=$(( end - start ))

if [ $laps -gt 5 ]
then
function displaytime {
  local T=$1
  local D=$((T/60/60/24))
  local H=$((T/60/60%24))
  local M=$((T/60%60))
  local S=$((T%60))
  (( $D > 0 )) && printf '%d days ' $D
  (( $H > 0 )) && printf '%d hours ' $H
  (( $M > 0 )) && printf '%d minutes ' $M
  (( $D > 0 || $H > 0 || $M > 0 )) && printf 'and '
  printf '%d seconds\n' $S
}

time=$(displaytime $laps)


curl --request POST 'https://hooks.slack.com/services/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?link_names=1' --data "{'text': '$2 \`$1\` has finished running in *$time*'}"

fi
