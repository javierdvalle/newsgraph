set -x

bash build.sh

cd build/

gcloud functions deploy update_feed --runtime python37 --trigger-http --region europe-west1
