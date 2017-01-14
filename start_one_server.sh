pkill hraftd

go build  

rm -rf log
rm -rf node0
rm  -rf leveldb0

./hraftd  node0

