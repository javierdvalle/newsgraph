set -x

bash build.sh

cd build/

gcloud functions deploy trigger_update_feeds --runtime python37 --trigger-http --region europe-west1
