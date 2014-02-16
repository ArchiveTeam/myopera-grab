#!/bin/bash
if ! dpkg-query -Wf'${Status}' python-lxml 2>/dev/null | grep -q '^i'
then
  echo "Installing python-lxml"
  sudo apt-get update
  sudo apt-get -y install python-lxml
fi

exit 0

