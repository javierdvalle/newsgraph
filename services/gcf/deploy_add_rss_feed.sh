set -x

bash build.sh

cd build/

gcloud functions deploy add_rss_feed --runtime python37 --trigger-http --region europe-west1
