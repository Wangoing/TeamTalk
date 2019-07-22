make clean
make
cd /root/TeamTalk/server/im-server-1
./stop.sh push_server
cp /root/TeamTalk/server/src/push_server/push_server /root/TeamTalk/server/im-server-1/push_server/
./restart.sh push_server
cd -
