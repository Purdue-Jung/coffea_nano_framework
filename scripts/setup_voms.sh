#!/bin/sh
export X509_USER_PROXY=~/x509up_u`id -u`
voms-proxy-init -voms cms -valid 999:00:00