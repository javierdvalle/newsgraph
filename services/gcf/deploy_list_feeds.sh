set -x

bash build.sh

cd build/

gcloud functions deploy list_feeds --runtime python37 --trigger-http --region europe-west1
