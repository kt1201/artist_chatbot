#!/bin/bash

rm *.zip
zip spotify.zip -r *

aws s3 rm s3://spotify-app-kt/spotify.zip
aws s3 cp ./spotify.zip s3://spotify-app-kt/spotify.zip
aws lambda update-function-code --function-name spotify-lambda --s3-bucket spotify-app-kt --s3-key spotify.zip
