#!/bin/bash
./launch.sh random

./launch.sh hour 00
./launch.sh hour 06
./launch.sh hour 12
./launch.sh hour 18

./launch.sh comments desc
./launch.sh comments asc

./launch.sh points desc
./launch.sh points asc

./launch.sh proba desc
./launch.sh proba asc
./launch.sh proba desc 0.1
./launch.sh proba desc 0.5
./launch.sh proba desc 0.1 positive

./launch.sh hierarchy
./launch.sh fusion