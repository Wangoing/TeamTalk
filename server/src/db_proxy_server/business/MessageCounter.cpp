/*================================================================
 *   Copyright (C) 2014 All rights reserved.
 *
 *   文件名称：MessageCounter.cpp
 *   创 建 者：Zhang Yuanhao
 *   邮    箱：bluefoxah@gmail.com
 *   创建日期：2014年12月15日
 *   描    述：
 *
 ================================================================*/

#include "../ProxyConn.h"
#include "../CachePool.h"
#include "MessageCounter.h"
#include "MessageModel.h"
#include "RelationModel.h"
#include "GroupMessageModel.h"
#include "IM.Message.pb.h"
#include "IM.BaseDefine.pb.h"
#include "IM.Login.pb.h"
#include "IM.Server.pb.h"
#include "UserModel.h"
#include<time.h>

namespace DB_PROXY {

    void getUnreadMsgCounter(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Message::IMUnreadMsgCntReq msg;
        IM::Message::IMUnreadMsgCntRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            CImPdu* pPduResp = new CImPdu;

            uint32_t nUserId = msg.user_id();

            list<IM::BaseDefine::UnreadInfo> lsUnreadCount;
            uint32_t nTotalCnt = 0;
            
            CMessageModel::getInstance()->getUnreadMsgCount(nUserId, nTotalCnt, lsUnreadCount);
            CGroupMessageModel::getInstance()->getUnreadMsgCount(nUserId, nTotalCnt, lsUnreadCount);
            msgResp.set_user_id(nUserId);
            msgResp.set_total_cnt(nTotalCnt);
            for(auto it= lsUnreadCount.begin(); it!=lsUnreadCount.end(); ++it)
            {
                IM::BaseDefine::UnreadInfo* pInfo = msgResp.add_unreadinfo_list();
    //            *pInfo = *it;
                pInfo->set_session_id(it->session_id());
                pInfo->set_session_type(it->session_type());
                pInfo->set_unread_cnt(it->unread_cnt());
                pInfo->set_latest_msg_id(it->latest_msg_id());
                pInfo->set_latest_msg_data(it->latest_msg_data());
                pInfo->set_latest_msg_type(it->latest_msg_type());
                pInfo->set_latest_msg_from_user_id(it->latest_msg_from_user_id());
            }
            
            
            log("userId=%d, unreadCnt=%u, totalCount=%u", nUserId, msgResp.unreadinfo_list_size(), nTotalCnt);
            msgResp.set_attach_data(msg.attach_data());
            pPduResp->SetPBMsg(&msgResp);
            pPduResp->SetSeqNum(pPdu->GetSeqNum());
            pPduResp->SetServiceId(IM::BaseDefine::SID_MSG);
            pPduResp->SetCommandId(IM::BaseDefine::CID_MSG_UNREAD_CNT_RESPONSE);
            CProxyConn::AddResponsePdu(conn_uuid, pPduResp);
        }
        else
        {
            log("parse pb failed");
        }
    }

    void clearUnreadMsgCounter(CImPdu* pPdu, uint32_t conn_uuid)
    {
    	bool bRet = false;
        IM::Message::IMMsgDataReadAck msg;
		IM::Message::IMMsgDataReadNotify msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            uint32_t nUserId = msg.user_id();
            uint32_t nFromId = msg.session_id();
			uint32_t nMsgId = msg.msg_id();
            IM::BaseDefine::SessionType nSessionType = msg.session_type();
			//清除redis数据
            CUserModel::getInstance()->clearUserCounter(nUserId, nFromId, nSessionType);
			//当是个人信息的时候，将信息修改为已读
			if(nSessionType == IM::BaseDefine::SESSION_TYPE_SINGLE)
			{
				//修改数据库中表数据
				uint32_t nRelateId = CRelationModel::getInstance()->getRelationId(nFromId, nUserId, true);
	            if(nRelateId != INVALID_VALUE)
	            {
	            	uint32_t nNow = (uint32_t)time(NULL);
	                bRet = CMessageModel::getInstance()->updateMessageToRead(nRelateId, nFromId, nUserId, nNow, nMsgId);
					if(bRet){
						
					}else{
						log("updateMessageToRead is fail. fromId=%u, toId=%u, nRelateId=%u", nFromId, nUserId, nRelateId);
					}
	            }
	            else{
	                log("relateId is invalid. fromId=%u, toId=%u, nRelateId=%u", nFromId, nUserId, nRelateId);
	            }
			}
			//当时群组消息的时候，需要处理redis中保留的消息已读未读数据
			//只有当时群组消息的已读时，才需要反馈消息
			if(nSessionType == IM::BaseDefine::SESSION_TYPE_GROUP)
			{
				CImPdu* pPduResp = new CImPdu;
				CGroupMessageModel::getInstance()->groupMessageRead(nUserId, nFromId, nMsgId);

				uint32_t nMsgReadCount = 0;
				uint32_t nMsgUnReadCount = 0;
				CGroupMessageModel::getInstance()->getReadAndUnReadByMsgId(nUserId, nFromId, nMsgReadCount, nMsgUnReadCount);

				msgResp.set_user_id(nUserId);
				msgResp.set_session_id(nFromId);
				msgResp.set_msg_id(nMsgId);
				msgResp.set_session_type(nSessionType);
				msgResp.set_msg_read_count(nMsgReadCount);
				msgResp.set_msg_unread_count(nMsgUnReadCount);

				list<uint32_t> lsUserIds;
        		CGroupModel::getInstance()->getGroupUser(nFromId, lsUserIds);
				for(auto itUserId=lsUserIds.begin(); itUserId!=lsUserIds.end(); ++itUserId)
		        {
		            msgResp.add_group_member_list(*itUserId);
		        }

				pPduResp->SetPBMsg(&msgResp);
            	pPduResp->SetSeqNum(pPdu->GetSeqNum());
            	pPduResp->SetServiceId(IM::BaseDefine::SID_MSG);
            	pPduResp->SetCommandId(IM::BaseDefine::CID_GROUP_MSG_READ_RESPONSE);
            	CProxyConn::AddResponsePdu(conn_uuid, pPduResp);
			}
            log("userId=%u, peerId=%u, type=%u", nFromId, nUserId, nSessionType);
        }
        else
        {
            log("parse pb failed");
        }
    }
        
    void setDevicesToken(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Login::IMDeviceTokenReq msg;
        IM::Login::IMDeviceTokenRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            uint32_t nUserId = msg.user_id();
            string strToken = msg.device_token();
            CImPdu* pPduResp = new CImPdu;
            CacheManager* pCacheManager = CacheManager::getInstance();
            CacheConn* pCacheConn = pCacheManager->GetCacheConn("token");
            if (pCacheConn)
            {
                IM::BaseDefine::ClientType nClientType = msg.client_type();
                string strValue;
                if(IM::BaseDefine::CLIENT_TYPE_IOS == nClientType)
                {
                    strValue = "ios:"+strToken;
                }
                else if(IM::BaseDefine::CLIENT_TYPE_ANDROID == nClientType)
                {
                    strValue = "android:"+strToken;
                }
                else
                {
                    strValue = strToken;
                }
                
                string strOldValue = pCacheConn->get("device_"+int2string(nUserId));
                
                if(!strOldValue.empty())
                {
                    size_t nPos = strOldValue.find(":");
                    if(nPos!=string::npos)
                    {
                        string strOldToken = strOldValue.substr(nPos + 1);
                        string strReply = pCacheConn->get("device_"+strOldToken);
                        if (!strReply.empty()) {
                            string strNewValue("");
                            pCacheConn->set("device_"+strOldToken, strNewValue);
                        }
                    }
                }
                
                pCacheConn->set("device_"+int2string(nUserId), strValue);
                string strNewValue = int2string(nUserId);
                pCacheConn->set("device_"+strToken, strNewValue);
            
                log("setDeviceToken. userId=%u, deviceToken=%s", nUserId, strToken.c_str());
                pCacheManager->RelCacheConn(pCacheConn);
            }
            else
            {
                log("no cache connection for token");
            }
            
            log("setDeviceToken. userId=%u, deviceToken=%s", nUserId, strToken.c_str());
            msgResp.set_attach_data(msg.attach_data());
            msgResp.set_user_id(nUserId);
            pPduResp->SetPBMsg(&msgResp);
            pPduResp->SetSeqNum(pPdu->GetSeqNum());
            pPduResp->SetServiceId(IM::BaseDefine::SID_LOGIN);
            pPduResp->SetCommandId(IM::BaseDefine::CID_LOGIN_RES_DEVICETOKEN);
            CProxyConn::AddResponsePdu(conn_uuid, pPduResp);
        }
        else
        {
            log("parse pb failed");
        }
    }


    void getDevicesToken(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Server::IMGetDeviceTokenReq msg;
        IM::Server::IMGetDeviceTokenRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            CacheManager* pCacheManager = CacheManager::getInstance();
            CacheConn* pCacheConn = pCacheManager->GetCacheConn("token");
            CImPdu* pPduResp = new CImPdu;
            uint32_t nCnt = msg.user_id_size();
            
            // 对于ios，不推送
            // 对于android，由客户端处理
            bool is_check_shield_status = false;
            time_t now = time(NULL);
            struct tm* _tm = localtime(&now);
            if (_tm->tm_hour >= 22 || _tm->tm_hour <=7 ) {
                    is_check_shield_status = true;
                }
            if (pCacheConn)
            {
                vector<string> vecTokens;
                for (uint32_t i=0; i<nCnt; ++i) {
                    string strKey = "device_"+int2string(msg.user_id(i));
                    vecTokens.push_back(strKey);
                }
                map<string, string> mapTokens;
                bool bRet = pCacheConn->mget(vecTokens, mapTokens);
                pCacheManager->RelCacheConn(pCacheConn);
                
                if(bRet)
                {
                    for (auto it=mapTokens.begin(); it!=mapTokens.end(); ++it) {
                        string strKey = it->first;
                        size_t nPos = strKey.find("device_");
                        if( nPos != string::npos)
                        {
                            string strUserId = strKey.substr(nPos + strlen("device_"));
                            uint32_t nUserId = string2int(strUserId);
                            string strValue = it->second;
                            nPos = strValue.find(":");
                            if(nPos!=string::npos)
                            {
                                string strType = strValue.substr(0, nPos);
                                string strToken = strValue.substr(nPos + 1);
                                IM::BaseDefine::ClientType nClientType = IM::BaseDefine::ClientType(0);
                                if(strType == "ios")
                                {
                                    // 过滤出已经设置勿打扰并且为晚上22：00～07：00
                                    uint32_t shield_status = 0;
                                    if (is_check_shield_status) {
                                        CUserModel::getInstance()->getPushShield(nUserId, &shield_status);
                                    }
                                    
                                    if (shield_status == 1) {
                                        // 对IOS处理
                                        continue;
                                    } else {
                                        nClientType = IM::BaseDefine::CLIENT_TYPE_IOS;
                                    }
                                    
                                    // nClientType = IM::BaseDefine::CLIENT_TYPE_IOS;
                                    // end
                                }
                                else if(strType == "android")
                                {
                                    nClientType = IM::BaseDefine::CLIENT_TYPE_ANDROID;
                                }
                                if(IM::BaseDefine::ClientType_IsValid(nClientType))
                                {
                                    IM::BaseDefine::UserTokenInfo* pToken = msgResp.add_user_token_info();
                                    pToken->set_user_id(nUserId);
                                    pToken->set_token(strToken);
                                    pToken->set_user_type(nClientType);
                                    uint32_t nTotalCnt = 0;
                                    CMessageModel::getInstance()->getUnReadCntAll(nUserId, nTotalCnt);
                                    CGroupMessageModel::getInstance()->getUnReadCntAll(nUserId, nTotalCnt);
                                    pToken->set_push_count(nTotalCnt);
                                    pToken->set_push_type(1);
                                }
                                else
                                {
                                    log("invalid clientType.clientType=%u", nClientType);
                                }
                            }
                            else
                            {
                                log("invalid value. value=%s", strValue.c_str());
                            }
                            
                        }
                        else
                        {
                            log("invalid key.key=%s", strKey.c_str());
                        }
                    }
                }
                else
                {
                    log("mget failed!");
                }
            }
            else
            {
                log("no cache connection for token");
            }
            
            log("req devices token.reqCnt=%u, resCnt=%u", nCnt, msgResp.user_token_info_size());
            
            msgResp.set_attach_data(msg.attach_data());
            pPduResp->SetPBMsg(&msgResp);
            pPduResp->SetSeqNum(pPdu->GetSeqNum());
            pPduResp->SetServiceId(IM::BaseDefine::SID_OTHER);
            pPduResp->SetCommandId(IM::BaseDefine::CID_OTHER_GET_DEVICE_TOKEN_RSP);
            CProxyConn::AddResponsePdu(conn_uuid, pPduResp);
        }
        else
        {
            log("parse pb failed");
        }
    }
};


