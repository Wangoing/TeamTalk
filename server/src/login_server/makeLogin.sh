make clean
make
cd /root/TeamTalk/server/im-server-1
./stop.sh login_server
cp /root/TeamTalk/server/src/login_server/login_server /root/TeamTalk/server/im-server-1/login_server/
./restart.sh login_server
cd -
