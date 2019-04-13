#!/bin/bash

for i in $(ls hon/*.png); do
    echo "file '$i'" >> hon_files.txt;
done

ffmpeg -y -r 10 -f concat -safe 0 -i "hon_files.txt" -c:v libx264 -vf fps=25 -pix_fmt yuv420p hon.mp4

rm hon_files.txt
