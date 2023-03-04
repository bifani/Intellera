#!/bin/bash

DIR="config"

REQ="$DIR/requirements.yml"
ENV="$DIR/environment.yml"
if [ "$1" != "" ]; then
    ENV="$1"
fi

echo
echo "$ENV"
echo
for PKG in `cat $REQ | grep -e "-" | grep -ve "#" | sed s:"  - "::g | grep -v name | grep -v analysis | grep -v conda-forge | grep -v defaults | grep -v dependencies | grep -v pip | grep -v prefix | sed s:"=":" ": | awk '{ print $1 }'`; do
#    echo $PKG
    grep -e " $PKG=" $ENV
done
echo

echo
echo "$DIR/requirement.txt"
echo
grep -e "  - " $ENV | grep -v name | grep -v analysis | grep -v conda-forge | grep -v defaults | grep -v dependencies | grep -v pip | grep -v prefix | sed s:"    - ":: | sed s:"  - ":: | sed s:"=":"==": | sed s:"===":"==":
echo
