#!/bin/bash

test_strat() {
  echo "Testing strategy: $1"
  python fetch_occur_csv.py --speed-info --strategy "$1" "$2" > "data/$1-output.csv" 2> "data/$1-info.txt"
  if [ $? -eq 0 ] ; then
    wc -l "data/$1-output.csv"
  else
    echo "FAILED"
  fi
}


if [ $# -eq 1 ] ; then
  mkdir -p data
  test_strat 'facet' $1
  test_strat 'search' $1
  test_strat 'download' $1
else
  echo "Usage: $0 LSID"
fi
