make clean
make
cd /root/TeamTalk/server/im-server-1
./stop.sh db_proxy_server
cp /root/TeamTalk/server/src/db_proxy_server/db_proxy_server /root/TeamTalk/server/im-server-1/db_proxy_server/
./restart.sh db_proxy_server
cd -
