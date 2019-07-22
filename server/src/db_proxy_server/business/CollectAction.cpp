/*================================================================
 *   Copyright (C) 2019 All rights reserved.
 *
 *   文件名称：CollectAction.cpp
 *   创 建 者：rendi
 *   邮    箱：
 *   创建日期：2019年7月3日
 *   描    述：
 *
 ================================================================*/

#include "../ProxyConn.h"
#include "CollectAction.h"
#include "CollectModel.h"
#include "IM.Group.pb.h"
#include "IM.BaseDefine.pb.h"
#include "IM.Message.pb.h"
#include "public_define.h"
#include "IM.Server.pb.h"
#include "RelationModel.h"


namespace DB_PROXY {
    void addCollect(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Message::IMMsgData msg;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
        	uint32_t nUserId = msg.user_id();
            uint32_t nFromId = msg.from_user_id();
            uint32_t nToId = msg.to_session_id();
            uint32_t nCreateTime = msg.create_time();
            IM::BaseDefine::MsgType nMsgType = msg.msg_type();
            uint32_t nMsgLen = msg.msg_data().length();
			uint32_t nMsgId = msg.msg_id();

			bool bRet =false;
            
            uint32_t nNow = (uint32_t)time(NULL);
            if (IM::BaseDefine::MsgType_IsValid(nMsgType))
            {
                if(nMsgLen != 0)
                {
                    CImPdu* pPduResp = new CImPdu;

                    uint32_t nSessionId = INVALID_VALUE;

                    CCollectModel* pCollectModel = CCollectModel::getInstance();
					
					if(nMsgType == IM::BaseDefine::MSG_TYPE_GROUP_TEXT || nMsgType == IM::BaseDefine::MSG_TYPE_GROUP_AUDIO){
						//由于群组消息，发送者是用户，接受者是群组，因此在记录收藏的时候做调转
						bRet = pCollectModel->hasCollect(nUserId,nMsgId,nToId,nFromId);
						if(!bRet){
							bRet = pCollectModel->addCollect(nUserId, -1, nToId, nFromId, nToId, nMsgType, nCreateTime, nMsgId, (string&)msg.msg_data());
						}
					}
					if(nMsgType== IM::BaseDefine::MSG_TYPE_SINGLE_TEXT || nMsgType == IM::BaseDefine::MSG_TYPE_SINGLE_AUDIO){
						bRet = pCollectModel->hasCollect(nUserId,nMsgId,nFromId,nToId);
						if(!bRet){
							uint32_t nRelateId = CRelationModel::getInstance()->getRelationId(nFromId, nToId, true);
							bRet = pCollectModel->addCollect(nUserId, nRelateId, nFromId, nToId, -1, nMsgType, nCreateTime, nMsgId, (string&)msg.msg_data());
						}
					}
					
                    log("fromId=%u, toId=%u, type=%u, msgId=%u, sessionId=%u", nFromId, nToId, nMsgType, nMsgId, nSessionId);

                    pPduResp->SetPBMsg(&msg);
                    pPduResp->SetSeqNum(pPdu->GetSeqNum());
                    pPduResp->SetServiceId(IM::BaseDefine::SID_MSG);
                    pPduResp->SetCommandId(IM::BaseDefine::CID_COLLECT_DATA);
                    CProxyConn::AddResponsePdu(conn_uuid, pPduResp);
                }
                else
                {
                    log("msgLen error. fromId=%u, toId=%u, msgType=%u", nFromId, nToId, nMsgType);
                }
            }
            else
            {
                log("invalid msgType.fromId=%u, toId=%u, msgType=%u", nFromId, nToId, nMsgType);
            }
        }
        else
        {
            log("parse pb failed");
        }
    }

	void getCollect(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Message::IMGetMsgListReq msg;
  		if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            uint32_t nUserId = msg.user_id();
            uint32_t nMsgId = msg.msg_id_begin();
			uint32_t nPeerId = msg.session_id();
            uint32_t nMsgCnt = msg.msg_cnt();
            IM::BaseDefine::SessionType nSessionType = msg.session_type();
            if(IM::BaseDefine::SessionType_IsValid(nSessionType))
            {
                CImPdu* pPduResp = new CImPdu;
                IM::Message::IMGetMsgListRsp msgResp;

                list<IM::BaseDefine::MsgInfo> lsMsg;

                CCollectModel::getInstance()->getCollect(nUserId, nMsgId, nMsgCnt, lsMsg);
                

                msgResp.set_user_id(nUserId);
                msgResp.set_msg_id_begin(nMsgId);
                msgResp.set_session_type(nSessionType);
				msgResp.set_session_id(nPeerId);
                for(auto it=lsMsg.begin(); it!=lsMsg.end();++it)
                {
                    IM::BaseDefine::MsgInfo* pMsg = msgResp.add_msg_list();
        //            *pMsg = *it;
                    pMsg->set_msg_id(it->msg_id());
                    pMsg->set_from_session_id(it->from_session_id());
                    pMsg->set_create_time(it->create_time());
                    pMsg->set_msg_type(it->msg_type());
                    pMsg->set_msg_data(it->msg_data());
//                    log("userId=%u, peerId=%u, msgId=%u", nUserId, nPeerId, it->msg_id());
                }

                log("userId=%u, msgId=%u, msgCnt=%u, count=%u", nUserId, nMsgId, nMsgCnt, msgResp.msg_list_size());
                msgResp.set_attach_data(msg.attach_data());
                pPduResp->SetPBMsg(&msgResp);
                pPduResp->SetSeqNum(pPdu->GetSeqNum());
                pPduResp->SetServiceId(IM::BaseDefine::SID_MSG);
                pPduResp->SetCommandId(IM::BaseDefine::CID_COLLECT_LIST_RESPONSE);
                CProxyConn::AddResponsePdu(conn_uuid, pPduResp);
            }
            else
            {
                log("invalid sessionType. userId=%u, msgId=%u, msgCnt=%u, sessionType=%u",
                    nUserId, nMsgId, nMsgCnt, nSessionType);
            }
        }
        else
        {
            log("parse pb failed");
        }
    }
}


