make clean
make
cd /root/TeamTalk/server/im-server-1
./stop.sh msg_server
cp /root/TeamTalk/server/src/msg_server/msg_server /root/TeamTalk/server/im-server-1/msg_server/
./restart.sh msg_server
cd -
