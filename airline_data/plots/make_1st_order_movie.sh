#!/bin/bash

for i in $(ls 1st_order/*.png); do
    echo "file '$i'" >> 1st_order_files.txt;
done

ffmpeg -y -r 10 -f concat -safe 0 -i "1st_order_files.txt" -c:v libx264 -vf fps=25 -pix_fmt yuv420p 1st_order.mp4

rm 1st_order_files.txt
