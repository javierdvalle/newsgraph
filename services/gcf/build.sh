set -x

INSTANCE_DIR='/home/javier/wkf/newsgraph/instances/instance1'

rm build/ -r
mkdir -p build/

cp -r $INSTANCE_DIR build/instance
cp main.py build/
cp requirements.txt build/
cp -r ../../newsgraph build/
