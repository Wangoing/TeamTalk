/*================================================================
 *   Copyright (C) 2014 All rights reserved.
 *
 *   文件名称：GroupAction.cpp
 *   创 建 者：Zhang Yuanhao
 *   邮    箱：bluefoxah@gmail.com
 *   创建日期：2014年12月15日
 *   描    述：
 *
 ================================================================*/

#include "../ProxyConn.h"
#include "GroupAction.h"
#include "GroupModel.h"
#include "IM.Group.pb.h"
#include "IM.BaseDefine.pb.h"
#include "public_define.h"
#include "IM.Server.pb.h"
#include "SessionModel.h"
#include "MessageModel.h"
#include "GroupMessageModel.h"



namespace DB_PROXY {
    
    /**
     *  创建群组
     *
     *  @param pPdu      收到的packet包指针
     *  @param conn_uuid 该包过来的socket 描述符
     */
    void createGroup(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Group::IMGroupCreateReq msg;
        IM::Group::IMGroupCreateRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            CImPdu* pPduRes = new CImPdu;
			bool bRet = false;
            
            uint32_t nUserId = msg.user_id();
            string strGroupName = msg.group_name();
            IM::BaseDefine::GroupType nGroupType = msg.group_type();
            if(IM::BaseDefine::GroupType_IsValid(nGroupType))
            {
                string strGroupAvatar = msg.group_avatar();
                set<uint32_t> setMember;
                uint32_t nMemberCnt = msg.member_id_list_size();
				if(nMemberCnt > 499){
					bRet = true;
					//返回500代表创建群的成员数量超过500
					msgResp.set_result_code(500);
				}
				//判断一个人创建的群组是否大于200个，如果超过200，报错
				list<uint32_t> lsGroupId;
				CGroupModel::getInstance()->getUserGroupIds(nUserId, lsGroupId,0);
				if(lsGroupId.size() > 199){
					bRet = true;
					//返回200代表个人创建群的数量超过200
					msgResp.set_result_code(200);
				}

                for(uint32_t i=0; i<nMemberCnt; ++i)
                {
                    uint32_t nUserId = msg.member_id_list(i);
                    setMember.insert(nUserId);
                }
                log("createGroup.%d create %s, userCnt=%u", nUserId, strGroupName.c_str(), setMember.size());
				
				if(!bRet){
	                uint32_t nGroupId = CGroupModel::getInstance()->createGroup(nUserId, strGroupName, strGroupAvatar, nGroupType, setMember);
	                msgResp.set_user_id(nUserId);
	                msgResp.set_group_name(strGroupName);
	                for(auto it=setMember.begin(); it!=setMember.end();++it)
	                {
	                    msgResp.add_user_id_list(*it);
	                }
	                if(nGroupId != INVALID_VALUE)
	                {
	                    msgResp.set_result_code(0);
	                    msgResp.set_group_id(nGroupId);
	                }
	                else
	                {
	                    msgResp.set_result_code(1);
	                }
				}
                
                
                log("createGroup.%d create %s, userCnt=%u, result:%d", nUserId, strGroupName.c_str(), setMember.size(), msgResp.result_code());
                
                msgResp.set_attach_data(msg.attach_data());
                pPduRes->SetPBMsg(&msgResp);
                pPduRes->SetSeqNum(pPdu->GetSeqNum());
                pPduRes->SetServiceId(IM::BaseDefine::SID_GROUP);
                pPduRes->SetCommandId(IM::BaseDefine::CID_GROUP_CREATE_RESPONSE);
                CProxyConn::AddResponsePdu(conn_uuid, pPduRes);
            }
            else
            {
                log("invalid group type.userId=%u, groupType=%u, groupName=%s", nUserId, nGroupType, strGroupName.c_str());
            }
        }
        else
        {
            log("parse pb failed");
        }
    }

	 /**
     *  修改群组
     *
     *  @param pPdu      收到的packet包指针
     *  @param conn_uuid 该包过来的socket 描述符
     */
    void changeGroup(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Group::IMGroupChangeReq msg;
        IM::Group::IMGroupChangeRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            CImPdu* pPduRes = new CImPdu;
			bool bRet = false;
            
            uint32_t nUserId = msg.user_id();
			uint32_t nGroupId = msg.group_id();
            string strGroupName = msg.group_name();
            string strGroupAvatar = msg.group_avatar();
			uint32_t nVersion = msg.version();
			IM::BaseDefine::GroupJoinType nGroupJoinType = msg.group_join_type();
			IM::BaseDefine::GroupJoinCheckType nGroupJoinCheckType = msg.group_join_check_type();
			IM::BaseDefine::GroupUpdateType nGroupUpdateType = msg.group_update_type();
			string strGroupNotice = msg.group_notice();
            
            log("changeGroup.userid=[%d] groupName=[%s] groupId=[%d]", nUserId, strGroupName.c_str(), nGroupId);
			
			IM::BaseDefine::GroupInfo cGroupInfo;
            bRet = CGroupModel::getInstance()->getGroupInfoByGroupId(nGroupId, cGroupInfo);
			if(bRet)
			{
				//如果版本号不一致，返回数据过期错误，并将新数据返回
				if(nVersion != cGroupInfo.version())
				{
					msgResp.set_result_code(9);
					msgResp.set_user_id(nUserId);
		            msgResp.set_group_name(cGroupInfo.group_name());
		            msgResp.set_group_avatar(cGroupInfo.group_avatar());
					msgResp.set_group_id(cGroupInfo.group_id());
					msgResp.set_group_join_type(cGroupInfo.group_join_type());
					msgResp.set_group_join_check_type(cGroupInfo.group_join_check_type());
					msgResp.set_group_update_type(cGroupInfo.group_update_type());
					msgResp.set_version(cGroupInfo.version());
					msgResp.set_group_notice(cGroupInfo.group_notice());
				}else{
					//修改群组信息
					bRet = CGroupModel::getInstance()->updateGroupInfo(nGroupId, nGroupJoinType, nGroupJoinCheckType, nGroupUpdateType, strGroupName, strGroupNotice);
					if(bRet){
						//修改后，在获取一次，主要是获取版本号
						CGroupModel::getInstance()->getGroupInfoByGroupId(nGroupId, cGroupInfo);
						msgResp.set_user_id(nUserId);
			            msgResp.set_group_name(cGroupInfo.group_name());
			            msgResp.set_group_avatar(cGroupInfo.group_avatar());
						msgResp.set_group_id(cGroupInfo.group_id());
						msgResp.set_group_join_type(cGroupInfo.group_join_type());
						msgResp.set_group_join_check_type(cGroupInfo.group_join_check_type());
						msgResp.set_group_update_type(cGroupInfo.group_update_type());
						msgResp.set_version(cGroupInfo.version());
						msgResp.set_group_notice(cGroupInfo.group_notice());
						msgResp.set_result_code(0);
					}else{
						msgResp.set_result_code(1);
						msgResp.set_user_id(nUserId);
			            msgResp.set_group_name(cGroupInfo.group_name());
			            msgResp.set_group_avatar(cGroupInfo.group_avatar());
						msgResp.set_group_id(cGroupInfo.group_id());
						msgResp.set_group_join_type(cGroupInfo.group_join_type());
						msgResp.set_group_join_check_type(cGroupInfo.group_join_check_type());
						msgResp.set_group_update_type(cGroupInfo.group_update_type());
						msgResp.set_group_notice(cGroupInfo.group_notice());
						msgResp.set_version(cGroupInfo.version());
					}
				}
			}else{
				msgResp.set_user_id(nUserId);
	            msgResp.set_group_name(strGroupName);
	            msgResp.set_group_avatar(strGroupAvatar);
				msgResp.set_group_id(nGroupId);
				msgResp.set_group_join_type(nGroupJoinType);
				msgResp.set_group_join_check_type(nGroupJoinCheckType);
				msgResp.set_group_update_type(nGroupUpdateType);
				msgResp.set_version(nVersion);
				msgResp.set_group_notice(strGroupNotice);
				msgResp.set_result_code(1);
			}
			
            
            log("changeGroup.userid=[%d] groupName=[%s] groupId=[%d] result:%d", nUserId, strGroupName.c_str(), nGroupId, msgResp.result_code());
            
            msgResp.set_attach_data(msg.attach_data());
            pPduRes->SetPBMsg(&msgResp);
            pPduRes->SetSeqNum(pPdu->GetSeqNum());
            pPduRes->SetServiceId(IM::BaseDefine::SID_GROUP);
            pPduRes->SetCommandId(IM::BaseDefine::CID_GROUP_CHANGE_RESPONSE);
            CProxyConn::AddResponsePdu(conn_uuid, pPduRes);
        }
        else
        {
            log("changeGroup parse pb failed");
        }
    }
    
    /**
     *  获取正式群列表
     *
     *  @param pPdu      收到的packet包指针
     *  @param conn_uuid 该包过来的socket 描述符
     */
    void getNormalGroupList(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Group::IMNormalGroupListReq msg;
        IM::Group::IMNormalGroupListRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            CImPdu* pPduRes = new CImPdu;
            
            uint32_t nUserId = msg.user_id();
            
            list<IM::BaseDefine::GroupVersionInfo> lsGroup;
            CGroupModel::getInstance()->getUserGroup(nUserId, lsGroup, IM::BaseDefine::GROUP_TYPE_NORMAL);
            msgResp.set_user_id(nUserId);
            for(auto it=lsGroup.begin(); it!=lsGroup.end(); ++it)
            {
                IM::BaseDefine::GroupVersionInfo* pGroupVersion = msgResp.add_group_version_list();
                pGroupVersion->set_group_id(it->group_id());
                pGroupVersion->set_version(it->version());
            }
            
            //log("getNormalGroupList. userId=%u, count=%d", nUserId, msgResp.group_version_list_size());
            
            msgResp.set_attach_data(msg.attach_data());
            pPduRes->SetPBMsg(&msgResp);
            pPduRes->SetSeqNum(pPdu->GetSeqNum());
            pPduRes->SetServiceId(IM::BaseDefine::SID_GROUP);
            pPduRes->SetCommandId(IM::BaseDefine::CID_GROUP_NORMAL_LIST_RESPONSE);
            CProxyConn::AddResponsePdu(conn_uuid, pPduRes);
        }
        else
        {
            log("parse pb failed");
        }
    }
    
    /**
     *  获取群信息
     *
     *  @param pPdu      收到的packet包指针
     *  @param conn_uuid 该包过来的socket 描述符
     */
    void getGroupInfo(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Group::IMGroupInfoListReq msg;
        IM::Group::IMGroupInfoListRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            CImPdu* pPduRes = new CImPdu;
            uint32_t nUserId = msg.user_id();
            uint32_t nGroupCnt = msg.group_version_list_size();
            
            map<uint32_t, IM::BaseDefine::GroupVersionInfo> mapGroupId;
            for(uint32_t i=0; i<nGroupCnt; ++i)
            {
                IM::BaseDefine::GroupVersionInfo groupInfo = msg.group_version_list(i);
                if(CGroupModel::getInstance()->isValidateGroupId(groupInfo.group_id()))
                {
                    mapGroupId[groupInfo.group_id()] = groupInfo;
                }
            }
            list<IM::BaseDefine::GroupInfo> lsGroupInfo;
            CGroupModel::getInstance()->getGroupInfo(mapGroupId, lsGroupInfo);
            
            msgResp.set_user_id(nUserId);
            for(auto it=lsGroupInfo.begin(); it!=lsGroupInfo.end(); ++it)
            {
                IM::BaseDefine::GroupInfo* pGroupInfo = msgResp.add_group_info_list();
    //            *pGroupInfo = *it;
                pGroupInfo->set_group_id(it->group_id());
                pGroupInfo->set_version(it->version());
                pGroupInfo->set_group_name(it->group_name());
                pGroupInfo->set_group_avatar(it->group_avatar());
                pGroupInfo->set_group_creator_id(it->group_creator_id());
                pGroupInfo->set_group_type(it->group_type());
                pGroupInfo->set_shield_status(it->shield_status());
				pGroupInfo->set_group_join_type(it->group_join_type());
				pGroupInfo->set_group_join_check_type(it->group_join_check_type());
				pGroupInfo->set_group_update_type(it->group_update_type());
				pGroupInfo->set_group_notice(it->group_notice());
                uint32_t nGroupMemberCnt = it->group_member_list_size();
                for (uint32_t i=0; i<nGroupMemberCnt; ++i) {
                    uint32_t userId = it->group_member_list(i);
                    pGroupInfo->add_group_member_list(userId);
                }
            }
            
            //log("userId=%u, requestCount=%u", nUserId, nGroupCnt);
            
            msgResp.set_attach_data(msg.attach_data());
            pPduRes->SetPBMsg(&msgResp);
            pPduRes->SetSeqNum(pPdu->GetSeqNum());
            pPduRes->SetServiceId(IM::BaseDefine::SID_GROUP);
            pPduRes->SetCommandId(IM::BaseDefine::CID_GROUP_INFO_RESPONSE);
            CProxyConn::AddResponsePdu(conn_uuid, pPduRes);
        }
        else
        {
            log("parse pb failed");
        }
    }
    /**
     *  修改群成员，增加或删除
     *
     *  @param pPdu      收到的packet包指针
     *  @param conn_uuid 该包过来的socket 描述符
     */
    void modifyMember(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Group::IMGroupChangeMemberReq msg;
        IM::Group::IMGroupChangeMemberRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            uint32_t nUserId = msg.user_id();
            uint32_t nGroupId = msg.group_id();
            IM::BaseDefine::GroupModifyType nType = msg.change_type();
            if (IM::BaseDefine::GroupModifyType_IsValid(nType) &&
                CGroupModel::getInstance()->isValidateGroupId(nGroupId)) {
                
                CImPdu* pPduRes = new CImPdu;
				//需要判断群组的配置，如果是添加成员需要审批则需要做特殊处理
				IM::BaseDefine::GroupInfo cGroupInfo;
            	CGroupModel::getInstance()->getGroupInfoByGroupId(nGroupId, cGroupInfo);
				IM::BaseDefine::GroupJoinCheckType nGroupJoinCheckType = cGroupInfo.group_join_check_type();
				//如果需要审批，则发送审批消息
				if(nGroupJoinCheckType == IM::BaseDefine::GROUP_JOIN_CHECK_BY_ADMIN){
					//返回特殊的result_code，让msg_server知道需要发送群消息
					msgResp.set_result_code(399);
					//创建群消息
					CGroupModel* pGroupModel = CGroupModel::getInstance();
					CGroupMessageModel* pGroupMsgModel = CGroupMessageModel::getInstance();
					uint32_t nSessionId = INVALID_VALUE;
					uint32_t nMsgId = INVALID_VALUE;
                    if (pGroupModel->isValidateGroupId(nGroupId) && pGroupModel->isInGroup(nUserId, nGroupId))
                    {
                        nSessionId = CSessionModel::getInstance()->getSessionId(nUserId, nGroupId, IM::BaseDefine::SESSION_TYPE_GROUP, false);
                        if (INVALID_VALUE == nSessionId) {
                            nSessionId = CSessionModel::getInstance()->addSession(nUserId, nGroupId, IM::BaseDefine::SESSION_TYPE_GROUP);
                        }
                        if(nSessionId != INVALID_VALUE)
                        {
                            nMsgId = pGroupMsgModel->getMsgId(nGroupId);
                            if (nMsgId != INVALID_VALUE) {
								uint32_t nNow = (uint32_t)time(NULL);
								string strClause;
								uint32_t nCnt = msg.member_id_list_size();
								for(uint32_t i=0; i<nCnt; ++i)
				                {
				                    uint32_t nUserId = msg.member_id_list(i);
									if(i == 0){
										strClause = int2string(nUserId);
									}else{
				                    	strClause = strClause + "," + int2string(nUserId);
									}
				                }
                                pGroupMsgModel->sendMessage(nUserId, nGroupId, IM::BaseDefine::MSG_TYPE_GROUP_MEM_ADD, nNow, nMsgId, strClause);
                                CSessionModel::getInstance()->updateSession(nSessionId, nNow);
                            }
                        }
                    }
					log("need join check userId=%u, groupId=%u, nGroupJoinCheckType=%u, nMsgId=%u",nUserId, nGroupId,  nGroupJoinCheckType,nMsgId);
				}
				//如果不需要审批，则处理业务
				else if(nGroupJoinCheckType == IM::BaseDefine::GROUP_JOIN_CHECK_NONE){
					uint32_t nCnt = msg.member_id_list_size();
	                set<uint32_t> setUserId;
	                for(uint32_t i=0; i<nCnt;++i)
	                {
	                    setUserId.insert(msg.member_id_list(i));
	                }
	                list<uint32_t> lsCurUserId;
	                bool bRet = CGroupModel::getInstance()->modifyGroupMember(nUserId, nGroupId, nType, setUserId, lsCurUserId);
	                msgResp.set_result_code(bRet?0:1);
	                if(bRet)
	                {
	                    for(auto it=setUserId.begin(); it!=setUserId.end(); ++it)
	                    {
	                        msgResp.add_chg_user_id_list(*it);
	                    }
	                    
	                    for(auto it=lsCurUserId.begin(); it!=lsCurUserId.end(); ++it)
	                    {
	                        msgResp.add_cur_user_id_list(*it);
	                    }
	                }
					log("userId=%u, groupId=%u, result=%u, changeCount:%u, currentCount=%u",nUserId, nGroupId,  bRet?0:1, msgResp.chg_user_id_list_size(), msgResp.cur_user_id_list_size());
				}
				//如果不是上述两种情况，则是不允许加入，直接返回错误
				else{
					msgResp.set_result_code(1);
				}

				msgResp.set_user_id(nUserId);
	            msgResp.set_group_id(nGroupId);
	            msgResp.set_change_type(nType);
				
                msgResp.set_attach_data(msg.attach_data());
                pPduRes->SetPBMsg(&msgResp);
                pPduRes->SetSeqNum(pPdu->GetSeqNum());
                pPduRes->SetServiceId(IM::BaseDefine::SID_GROUP);
                pPduRes->SetCommandId(IM::BaseDefine::CID_GROUP_CHANGE_MEMBER_RESPONSE);
                CProxyConn::AddResponsePdu(conn_uuid, pPduRes);
            }
            else
            {
                log("invalid groupModifyType or groupId. userId=%u, groupId=%u, groupModifyType=%u", nUserId, nGroupId, nType);
            }
            
        }
        else
        {
            log("parse pb failed");
        }
    }
    
    /**
     *  设置群组信息推送，屏蔽或者取消屏蔽
     *
     *  @param pPdu      收到的packet包指针
     *  @param conn_uuid 该包过来的socket 描述符
     */
    void setGroupPush(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Group::IMGroupShieldReq msg;
        IM::Group::IMGroupShieldRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            uint32_t nUserId = msg.user_id();
            uint32_t nGroupId = msg.group_id();
            uint32_t nStatus = msg.shield_status();
            if(CGroupModel::getInstance()->isValidateGroupId(nGroupId))
            {
                
                CImPdu* pPduRes = new CImPdu;
                bool bRet = CGroupModel::getInstance()->setPush(nUserId, nGroupId, IM_GROUP_SETTING_PUSH, nStatus);
                
                msgResp.set_user_id(nUserId);
                msgResp.set_group_id(nGroupId);
                msgResp.set_result_code(bRet?0:1);
            
                log("userId=%u, groupId=%u, result=%u", nUserId, nGroupId, msgResp.result_code());
                
                msgResp.set_attach_data(msg.attach_data());
                pPduRes->SetPBMsg(&msgResp);
                pPduRes->SetSeqNum(pPdu->GetSeqNum());
                pPduRes->SetServiceId(IM::BaseDefine::SID_GROUP);
                pPduRes->SetCommandId(IM::BaseDefine::CID_GROUP_SHIELD_GROUP_RESPONSE);
                CProxyConn::AddResponsePdu(conn_uuid, pPduRes);
            }
            else
            {
                log("Invalid group.userId=%u, groupId=%u", nUserId, nGroupId);
            }
        }
        else
        {
            log("parse pb failed");
        }
    }
    
    /**
     *  获取一个群的推送设置
     *
     *  @param pPdu      收到的packet包指针
     *  @param conn_uuid 该包过来的socket 描述符
     */
    void getGroupPush(CImPdu* pPdu, uint32_t conn_uuid)
    {
        IM::Server::IMGroupGetShieldReq msg;
        IM::Server::IMGroupGetShieldRsp msgResp;
        if(msg.ParseFromArray(pPdu->GetBodyData(), pPdu->GetBodyLength()))
        {
            uint32_t nGroupId = msg.group_id();
            uint32_t nUserCnt = msg.user_id_size();
            if(CGroupModel::getInstance()->isValidateGroupId(nGroupId))
            {
                CImPdu* pPduRes = new CImPdu;
                list<uint32_t> lsUser;
                for(uint32_t i=0; i<nUserCnt; ++i)
                {
                    lsUser.push_back(msg.user_id(i));
                }
                list<IM::BaseDefine::ShieldStatus> lsPush;
                CGroupModel::getInstance()->getPush(nGroupId, lsUser, lsPush);
                
                msgResp.set_group_id(nGroupId);
                for (auto it=lsPush.begin(); it!=lsPush.end(); ++it) {
                    IM::BaseDefine::ShieldStatus* pStatus = msgResp.add_shield_status_list();
        //            *pStatus = *it;
                    pStatus->set_user_id(it->user_id());
                    pStatus->set_group_id(it->group_id());
                    pStatus->set_shield_status(it->shield_status());
                }
                
                log("groupId=%u, count=%u", nGroupId, nUserCnt);
                
                msgResp.set_attach_data(msg.attach_data());
                pPduRes->SetPBMsg(&msgResp);
                pPduRes->SetSeqNum(pPdu->GetSeqNum());
                pPduRes->SetServiceId(IM::BaseDefine::SID_OTHER);
                pPduRes->SetCommandId(IM::BaseDefine::CID_OTHER_GET_SHIELD_RSP);
                CProxyConn::AddResponsePdu(conn_uuid, pPduRes);
            }
            else
            {
                log("Invalid groupId. nGroupId=%u", nGroupId);
            }
        }
        else
        {
            log("parse pb failed");
        }
    }
}

