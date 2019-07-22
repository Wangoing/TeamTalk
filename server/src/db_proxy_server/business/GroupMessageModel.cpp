/*================================================================
 *   Copyright (C) 2014 All rights reserved.
 *
 *   文件名称：GroupMessageModel.cpp
 *   创 建 者：Zhang Yuanhao
 *   邮    箱：bluefoxah@gmail.com
 *   创建日期：2014年12月15日
 *   描    述：
 *
 ================================================================*/

#include <map>
#include <set>

#include "../DBPool.h"
#include "../CachePool.h"
#include "GroupMessageModel.h"
#include "AudioModel.h"
#include "SessionModel.h"
#include "MessageCounter.h"
#include "Common.h"
#include "GroupModel.h"

using namespace std;

extern string strAudioEnc;

CGroupMessageModel* CGroupMessageModel::m_pInstance = NULL;

/**
 *  构造函数
 */
CGroupMessageModel::CGroupMessageModel()
{

}

/**
 *  析构函数
 */
CGroupMessageModel::~CGroupMessageModel()
{

}

/**
 *  单例
 *
 *  @return 返回单例指针
 */
CGroupMessageModel* CGroupMessageModel::getInstance()
{
	if (!m_pInstance) {
		m_pInstance = new CGroupMessageModel();
	}

	return m_pInstance;
}

/**
 *  发送群消息接口
 *
 *  @param nRelateId     关系Id
 *  @param nFromId       发送者Id
 *  @param nGroupId      群组Id
 *  @param nMsgType      消息类型
 *  @param nCreateTime   消息创建时间
 *  @param nMsgId        消息Id
 *  @param strMsgContent 消息类容
 *
 *  @return 成功返回true 失败返回false
 */
bool CGroupMessageModel::sendMessage(uint32_t nFromId, uint32_t nGroupId, IM::BaseDefine::MsgType nMsgType, uint32_t nCreateTime,uint32_t nMsgId, const string& strMsgContent)
{
    bool bRet = false;
    if(CGroupModel::getInstance()->isInGroup(nFromId, nGroupId))
    {
        CDBManager* pDBManager = CDBManager::getInstance();
        CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_master");
        if (pDBConn)
        {
        	//获取群众成员的明细
        	list<uint32_t> lsUserIds;
			CGroupModel::getInstance()->getGroupUser(nGroupId,lsUserIds);
			
            string strTableName = "IMGroupMessage_" + int2string(nGroupId % 8);
            string strSql = "insert into " + strTableName + " (`groupId`, `userId`, `msgId`, `content`, `type`, `status`, `updated`, `created`, `readCount`, `unreadCount`, `isAllRead`) "\
            "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            // 必须在释放连接前delete CPrepareStatement对象，否则有可能多个线程操作mysql对象，会crash
            CPrepareStatement* pStmt = new CPrepareStatement();
            if (pStmt->Init(pDBConn->GetMysql(), strSql))
            {
                uint32_t nStatus = 0;
                uint32_t nType = nMsgType;
                uint32_t index = 0;
				uint32_t userIdSize = lsUserIds.size();
				uint32_t readCount =0;
				uint32_t isReadAll = 0;
                pStmt->SetParam(index++, nGroupId);
                pStmt->SetParam(index++, nFromId);
                pStmt->SetParam(index++, nMsgId);
                pStmt->SetParam(index++, strMsgContent);
                pStmt->SetParam(index++, nType);
                pStmt->SetParam(index++, nStatus);
                pStmt->SetParam(index++, nCreateTime);
                pStmt->SetParam(index++, nCreateTime);
				pStmt->SetParam(index++, readCount);
				pStmt->SetParam(index++, userIdSize);
				pStmt->SetParam(index++, isReadAll);
                
                bool bRet = pStmt->ExecuteUpdate();
                if (bRet)
                {
                    CGroupModel::getInstance()->updateGroupChat(nGroupId);
                    incMessageCount(nFromId, nGroupId);
                    clearMessageCount(nFromId, nGroupId);
					incMessageReadCount(nFromId,nGroupId,nMsgId,lsUserIds);
                } else {
                    log("insert message failed: %s", strSql.c_str());
                }
            }
            delete pStmt;
            pDBManager->RelDBConn(pDBConn);
        }
        else
        {
            log("no db connection for teamtalk_master");
        }
    }
    else
    {
        log("not in the group.fromId=%u, groupId=%u", nFromId, nGroupId);
    }
    return bRet;
}

/**
 *  修改群消息状态接口
 *
 *  @param nFromId       发送者Id
 *  @param nGroupId      群组Id
 *  @param nUpdateTime   消息修改时间
 *  @param nMsgId        消息Id
 *  @param nStatus		 消息状态 1-删除 2-撤销
 *
 *  @return 成功返回true 失败返回false
 */
bool CGroupMessageModel::updateMessageStatus(uint32_t nFromId, uint32_t nGroupId, uint32_t nUpdateTime,uint32_t nMsgId, uint32_t nStatus)
{
    bool bRet = false;
    if(CGroupModel::getInstance()->isInGroup(nFromId, nGroupId))
    {
        CDBManager* pDBManager = CDBManager::getInstance();
        CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_master");
        if (pDBConn)
        {
            string strTableName = "IMGroupMessage_" + int2string(nGroupId % 8);
            string strSql = "update " + strTableName + " set status=?,updated=? where groupId=? and userId=? and msgId=?";
            
            // 必须在释放连接前delete CPrepareStatement对象，否则有可能多个线程操作mysql对象，会crash
            CPrepareStatement* pStmt = new CPrepareStatement();
            if (pStmt->Init(pDBConn->GetMysql(), strSql))
            {
                uint32_t index = 0;
				pStmt->SetParam(index++, nStatus);
				pStmt->SetParam(index++, nUpdateTime);
                pStmt->SetParam(index++, nGroupId);
                pStmt->SetParam(index++, nFromId);
                pStmt->SetParam(index++, nMsgId);
                
                bool bRet = pStmt->ExecuteUpdate();
                if (!bRet)
                {
                    log("update message failed: %s", strSql.c_str());
                }
            }
            delete pStmt;
            pDBManager->RelDBConn(pDBConn);
        }
        else
        {
            log("no db connection for teamtalk_master");
        }
    }
    else
    {
        log("not in the group.fromId=%u, groupId=%u", nFromId, nGroupId);
    }
    return bRet;
}

/**
 *  发送群组语音信息
 *
 *  @param nRelateId   关系Id
 *  @param nFromId     发送者Id
 *  @param nGroupId    群组Id
 *  @param nMsgType    消息类型
 *  @param nCreateTime 消息创建时间
 *  @param nMsgId      消息Id
 *  @param pMsgContent 指向语音类容的指针
 *  @param nMsgLen     语音消息长度
 *
 *  @return 成功返回true，失败返回false
 */
bool CGroupMessageModel::sendAudioMessage(uint32_t nFromId, uint32_t nGroupId, IM::BaseDefine::MsgType nMsgType, uint32_t nCreateTime, uint32_t nMsgId, const char* pMsgContent, uint32_t nMsgLen)
{
	if (nMsgLen <= 4) {
		return false;
	}

    if(!CGroupModel::getInstance()->isInGroup(nFromId, nGroupId))
    {
        log("not in the group.fromId=%u, groupId=%u", nFromId, nGroupId);
        return false;
    }
    
	CAudioModel* pAudioModel = CAudioModel::getInstance();
	int nAudioId = pAudioModel->saveAudioInfo(nFromId, nGroupId, nCreateTime, pMsgContent, nMsgLen);

	bool bRet = true;
	if (nAudioId != -1) {
		string strMsg = int2string(nAudioId);
        bRet = sendMessage(nFromId, nGroupId, nMsgType, nCreateTime, nMsgId, strMsg);
	} else {
		bRet = false;
	}

	return bRet;
}

/**
 *  清除群组消息计数
 *
 *  @param nUserId  用户Id
 *  @param nGroupId 群组Id
 *
 *  @return 成功返回true，失败返回false
 */
bool CGroupMessageModel::clearMessageCount(uint32_t nUserId, uint32_t nGroupId)
{
    bool bRet = false;
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if (pCacheConn)
    {
        string strGroupKey = int2string(nGroupId) + GROUP_TOTAL_MSG_COUNTER_REDIS_KEY_SUFFIX;
        map<string, string> mapGroupCount;
        bool bRet = pCacheConn->hgetAll(strGroupKey, mapGroupCount);
        pCacheManager->RelCacheConn(pCacheConn);
        if(bRet)
        {
            string strUserKey = int2string(nUserId) + "_" + int2string(nGroupId) + GROUP_USER_MSG_COUNTER_REDIS_KEY_SUFFIX;
            string strReply = pCacheConn->hmset(strUserKey, mapGroupCount);
            if(strReply.empty())
            {
                log("hmset %s failed !", strUserKey.c_str());
            }
            else
            {
                bRet = true;
            }
        }
        else
        {
            log("hgetAll %s failed !", strGroupKey.c_str());
        }
    }
    else
    {
        log("no cache connection for unread");
    }
    return bRet;
}

/**
 *  增加群消息计数
 *
 *  @param nUserId  用户Id
 *  @param nGroupId 群组Id
 *
 *  @return 成功返回true，失败返回false
 */
bool CGroupMessageModel::incMessageCount(uint32_t nUserId, uint32_t nGroupId)
{
    bool bRet = false;
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if (pCacheConn)
    {
        string strGroupKey = int2string(nGroupId) + GROUP_TOTAL_MSG_COUNTER_REDIS_KEY_SUFFIX;
        pCacheConn->hincrBy(strGroupKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD, 1);
        map<string, string> mapGroupCount;
        bool bRet = pCacheConn->hgetAll(strGroupKey, mapGroupCount);
        if(bRet)
        {
            string strUserKey = int2string(nUserId) + "_" + int2string(nGroupId) + GROUP_USER_MSG_COUNTER_REDIS_KEY_SUFFIX;
            string strReply = pCacheConn->hmset(strUserKey, mapGroupCount);
            if(!strReply.empty())
            {
                bRet = true;
            }
            else
            {
                log("hmset %s failed !", strUserKey.c_str());
            }
        }
        else
        {
            log("hgetAll %s failed!", strGroupKey.c_str());
        }
        pCacheManager->RelCacheConn(pCacheConn);
    }
    else
    {
        log("no cache connection for unread");
    }
    return bRet;
}

/**
 *  获取群的某条消息已读未读数量
 *
 *  @param nUserId   用户Id
 *  @param nReadCnt 已读,引用
 *  @param nUnReadCnt 未读已读,引用
 */
void CGroupMessageModel::getReadAndUnReadByMsgId(uint32_t nMsgId, uint32_t nGroupId, uint32_t &nReadCnt,  uint32_t &nUnReadCnt)
{
	string strGroupMsgReadKey = int2string(nGroupId) + "_" + int2string(nMsgId) + "_" + "ReadCount";
	string strGroupMsgUnReadKey = int2string(nGroupId) + "_" + int2string(nMsgId) + "_" + "UnReadCount";

	
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if (pCacheConn)
    {
        string strReadCnt = pCacheConn->hget(strGroupMsgReadKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD);
		string strUnReadCnt = pCacheConn->hget(strGroupMsgUnReadKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD);
        
        
        nReadCnt = ( strReadCnt.empty() ? 0 : ((uint32_t)atoi(strReadCnt.c_str())) );
		nUnReadCnt = ( strUnReadCnt.empty() ? 0 : ((uint32_t)atoi(strUnReadCnt.c_str())) );
        
        pCacheManager->RelCacheConn(pCacheConn);
    }
    else
    {
        log("no cache connection for unread");
    }
}


/**
 *  增加群消息已读未读计数
 *
 *  @param nUserId  用户Id
 *  @param nGroupId 群组Id
 *  @param nMsgId 	消息Id
 *  @param lsUserId 群成员Id
 *
 *  @return 成功返回true，失败返回false
 */
bool CGroupMessageModel::incMessageReadCount(uint32_t nUserId, uint32_t nGroupId, uint32_t nMsgId, list<uint32_t> &lsUserId)
{
    bool bRet = false;
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if (pCacheConn)
    {
        string strGroupMsgReadKey = int2string(nGroupId) + "_" + int2string(nMsgId) + "_" + "ReadCount";
		string strGroupMsgUnReadKey = int2string(nGroupId) + "_" + int2string(nMsgId) + "_" + "UnReadCount";
		string strGroupMsgUnReadUserListKey = int2string(nGroupId) + "_" + int2string(nMsgId) + "_" + "UnReadUser";
		string strUserGroupMsgUnReadKey = int2string(nGroupId) + "_" + int2string(nUserId) + "_UserUnReadMsg";
		//群组消息的已读数量
        pCacheConn->hincrBy(strGroupMsgReadKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD, 0);
		//群组消息的未读数量
		pCacheConn->hincrBy(strGroupMsgUnReadKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD, lsUserId.size() - 1);
		//将群组消息的待读用户list存入redis
		pCacheConn->sAddInt(strGroupMsgUnReadUserListKey, lsUserId);
		//因为是讲全部的用户都加入到了未读，在此去掉自己
		pCacheConn->sRemInt(strGroupMsgUnReadUserListKey, nUserId);
		//将群里用户待读的msg_id存入redis
		for(auto it=lsUserId.begin(); it!=lsUserId.end(); ++it)
	    {
	        uint32_t nGroupMemId=*it;
	        strUserGroupMsgUnReadKey = int2string(nGroupId) + "_" + int2string(nGroupMemId) + "_UserUnReadMsg";
	        pCacheConn->sAdd(strUserGroupMsgUnReadKey, nMsgId);
	    }
		
		long len = pCacheConn->sCard(strGroupMsgUnReadUserListKey);
		long len1 = pCacheConn->sCard(strUserGroupMsgUnReadKey);
		log("incMessageReadCount strGroupMsgUnReadUserListKey[%s] userIds len = [%u] , strUserGroupMsgUnReadKey[%s] len = [%u] ",
			strGroupMsgUnReadUserListKey.c_str(), len, strUserGroupMsgUnReadKey.c_str(), len1);
        pCacheManager->RelCacheConn(pCacheConn);
		bRet = true;
    }
    else
    {
        log("no cache connection for unread");
    }
    return bRet;
}

/**
 *  用户读取群消息已读未读计数
 *
 *  @param nUserId  用户Id
 *  @param nGroupId 群组Id
 *  @param nMsgId 	消息Id
 *
 *  @return 成功返回true，失败返回false
 */
bool CGroupMessageModel::groupMessageRead(uint32_t nUserId, uint32_t nGroupId, uint32_t nLastMsgId)
{
    bool bRet = false;
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if (pCacheConn)
    {
    	string strUserGroupMsgUnReadKey = int2string(nGroupId) + "_" + int2string(nUserId) + "_UserUnReadMsg";
    	//因为进入一个群之后，无论有多少未读消息，都只传递最后一个消息的id，所以，需要先取出用户针对该群所有的未读消息，循环处理每一条消息
    	list<uint32_t> lsMsgId;
		pCacheConn->smembersInt(strUserGroupMsgUnReadKey, lsMsgId);
		uint32_t lsMsgIdCnt = lsMsgId.size();
		log("lsMsgIdCnt=%d, strUserGroupMsgUnReadKey=%s", lsMsgIdCnt, strUserGroupMsgUnReadKey.c_str());
		//如果redis中有值，再进行处理
		if(lsMsgIdCnt > 0)
		{
			for(auto it=lsMsgId.begin(); it!=lsMsgId.end(); ++it){
				uint32_t nUnReadMsgId = *it;
				log("nUnReadMsgId=%u, nLastMsgId=%d",nUnReadMsgId, nLastMsgId);
				//如果未读的消息id小于最后一条消息id，证明这条消息时间更早，需要做已读处理s
				if(nUnReadMsgId < nLastMsgId)
				{
					string strGroupMsgReadKey = int2string(nGroupId) + "_" + int2string(nUnReadMsgId) + "_" + "ReadCount";
					string strGroupMsgUnReadKey = int2string(nGroupId) + "_" + int2string(nUnReadMsgId) + "_" + "UnReadCount";
					string strGroupMsgUnReadUserListKey = int2string(nGroupId) + "_" + int2string(nUnReadMsgId) + "_" + "UnReadUser";
					long len = pCacheConn->sCard(strGroupMsgUnReadUserListKey);
					
					//直接更新这条消息id对应的用户列表，如果更新到了证明这个用户的确未读这条消息，如果未更新到，证明之前已经读取了，不做处理
					long lRet = pCacheConn->sRemInt(strGroupMsgUnReadUserListKey, nUserId);
					log("groupMessageRead strGroupMsgUnReadUserListKey[%s] len = [%u] sRemInt result=[%u] nUserId=[%d]", 
						strGroupMsgUnReadUserListKey.c_str(), len, lRet, nUserId);
					//返回值为1，证明set中有此值，返回值为0，证明set中无此值
					if(lRet != 0)
					{
						//群组消息的已读数量 + 1
				        pCacheConn->hincrBy(strGroupMsgReadKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD, 1);
						//群组消息的未读数量 - 1
						pCacheConn->hincrBy(strGroupMsgUnReadKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD,  -1);					
					}
					//将某一用户待读的msg_id从redis中清除
					pCacheConn->sRemInt(strUserGroupMsgUnReadKey, nUnReadMsgId);	
				}
			}
		}
		
		
		long len1 = pCacheConn->sCard(strUserGroupMsgUnReadKey);
		log("groupMessageRead strUserGroupMsgUnReadKey[%s] len = [%u] ", strUserGroupMsgUnReadKey.c_str(), len1);
        pCacheManager->RelCacheConn(pCacheConn);
		bRet = true;
    }
    else
    {
        log("no cache connection for unread");
    }
    return bRet;
}



/**
 *  获取群组消息列表
 *
 *  @param nUserId  用户Id
 *  @param nGroupId 群组Id
 *  @param nMsgId   开始的msgId(最新的msgId)
 *  @param nMsgCnt  获取的长度
 *  @param lsMsg    消息列表
 */
void CGroupMessageModel::getMessage(uint32_t nUserId, uint32_t nGroupId, uint32_t nMsgId, uint32_t nMsgCnt, list<IM::BaseDefine::MsgInfo>& lsMsg)
{
    //根据 count 和 lastId 获取信息
    string strTableName = "IMGroupMessage_" + int2string(nGroupId % 8);
    
    CDBManager* pDBManager = CDBManager::getInstance();
    CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_slave");
    if (pDBConn)
    {
        uint32_t nUpdated = CGroupModel::getInstance()->getUserJoinTime(nGroupId, nUserId);
        //如果nMsgId 为0 表示客户端想拉取最新的nMsgCnt条消息
        string strSql;
        if(nMsgId == 0)
        {
            strSql = "select * from " + strTableName + " where groupId = " + int2string(nGroupId) + " and status = 0 and created>="+ int2string(nUpdated) + " order by created desc, id desc limit " + int2string(nMsgCnt);
        }else {
            strSql = "select * from " + strTableName + " where groupId = " + int2string(nGroupId) + " and msgId<=" + int2string(nMsgId) + " and status = 0 and created>="+ int2string(nUpdated) + " order by created desc, id desc limit " + int2string(nMsgCnt);
        }
        
        CResultSet* pResultSet = pDBConn->ExecuteQuery(strSql.c_str());
        if (pResultSet)
        {
            map<uint32_t, IM::BaseDefine::MsgInfo> mapAudioMsg;
            while(pResultSet->Next())
            {
                IM::BaseDefine::MsgInfo msg; 
                msg.set_msg_id(pResultSet->GetInt("msgId"));
                msg.set_from_session_id(pResultSet->GetInt("userId"));
                msg.set_create_time(pResultSet->GetInt("created"));
                IM::BaseDefine::MsgType nMsgType = IM::BaseDefine::MsgType(pResultSet->GetInt("type"));
                if(IM::BaseDefine::MsgType_IsValid(nMsgType))
                {
                    msg.set_msg_type(nMsgType);
                    msg.set_msg_data(pResultSet->GetString("content"));
					uint32_t nMsgRead = 0;
					uint32_t nMsgUnRead = 0;
					getReadAndUnReadByMsgId(pResultSet->GetInt("msgId"), nGroupId, nMsgRead, nMsgUnRead);
					msg.set_read_count(nMsgRead);
					msg.set_unread_count(nMsgUnRead);
                    lsMsg.push_back(msg);
                }
                else
                {
                    log("invalid msgType. userId=%u, groupId=%u, msgType=%u", nUserId, nGroupId, nMsgType);
                }
            }
            delete pResultSet;
        }
        else
        {
            log("no result set for sql: %s", strSql.c_str());
        }
        pDBManager->RelDBConn(pDBConn);
        if (!lsMsg.empty()) {
            CAudioModel::getInstance()->readAudios(lsMsg);
        }
    }
    else
    {
        log("no db connection for teamtalk_slave");
    }
}

/**
 *  获取用户群未读消息计数
 *
 *  @param nUserId       用户Id
 *  @param nTotalCnt     总条数
 *  @param lsUnreadCount 每个会话的未读信息包含了条数，最后一个消息的Id，最后一个消息的类型，最后一个消息的类容
 */
void CGroupMessageModel::getUnreadMsgCount(uint32_t nUserId, uint32_t &nTotalCnt, list<IM::BaseDefine::UnreadInfo>& lsUnreadCount)
{
    list<uint32_t> lsGroupId;
    CGroupModel::getInstance()->getUserGroupIds(nUserId, lsGroupId, 0);
    uint32_t nCount = 0;
    
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if (pCacheConn)
    {
        for(auto it=lsGroupId.begin(); it!=lsGroupId.end(); ++it)
        {
            uint32_t nGroupId = *it;
            string strGroupKey = int2string(nGroupId) + GROUP_TOTAL_MSG_COUNTER_REDIS_KEY_SUFFIX;
            string strGroupCnt = pCacheConn->hget(strGroupKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD);
            if(strGroupCnt.empty())
            {
//                log("hget %s : count failed !", strGroupKey.c_str());
                continue;
            }
            uint32_t nGroupCnt = (uint32_t)(atoi(strGroupCnt.c_str()));
            
            string strUserKey = int2string(nUserId) + "_" + int2string(nGroupId) + GROUP_USER_MSG_COUNTER_REDIS_KEY_SUFFIX;
            string strUserCnt = pCacheConn->hget(strUserKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD);
            
            uint32_t nUserCnt = ( strUserCnt.empty() ? 0 : ((uint32_t)atoi(strUserCnt.c_str())) );
            if(nGroupCnt >= nUserCnt) {
                nCount = nGroupCnt - nUserCnt;
            }
            if(nCount > 0)
            {
                IM::BaseDefine::UnreadInfo cUnreadInfo;
                cUnreadInfo.set_session_id(nGroupId);
                cUnreadInfo.set_session_type(IM::BaseDefine::SESSION_TYPE_GROUP);
                cUnreadInfo.set_unread_cnt(nCount);
                nTotalCnt += nCount;
                string strMsgData;
                uint32_t nMsgId;
                IM::BaseDefine::MsgType nType;
                uint32_t nFromId;
                getLastMsg(nGroupId, nMsgId, strMsgData, nType, nFromId);
                if(IM::BaseDefine::MsgType_IsValid(nType))
                {
                    cUnreadInfo.set_latest_msg_id(nMsgId);
                    cUnreadInfo.set_latest_msg_data(strMsgData);
                    cUnreadInfo.set_latest_msg_type(nType);
                    cUnreadInfo.set_latest_msg_from_user_id(nFromId);
                    lsUnreadCount.push_back(cUnreadInfo);
                }
                else
                {
                    log("invalid msgType. userId=%u, groupId=%u, msgType=%u, msgId=%u", nUserId, nGroupId, nType, nMsgId);
                }
            }
        }
        pCacheManager->RelCacheConn(pCacheConn);
    }
    else
    {
        log("no cache connection for unread");
    }
}

/**
 *  获取一个群组的msgId，自增，通过redis控制
 *
 *  @param nGroupId 群Id
 *
 *  @return 返回msgId
 */
uint32_t CGroupMessageModel::getMsgId(uint32_t nGroupId)
{
    uint32_t nMsgId = 0;
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if(pCacheConn)
    {
        // TODO
        string strKey = "group_msg_id_" + int2string(nGroupId);
        nMsgId = pCacheConn->incrBy(strKey, 1);
        pCacheManager->RelCacheConn(pCacheConn);
    }
    else
    {
        log("no cache connection for unread");
    }
    return nMsgId;
}

/**
 *  获取一个群的最后一条消息
 *
 *  @param nGroupId   群Id
 *  @param nMsgId     最后一条消息的msgId,引用
 *  @param strMsgData 最后一条消息的内容,引用
 *  @param nMsgType   最后一条消息的类型,引用
 */
void CGroupMessageModel::getLastMsg(uint32_t nGroupId, uint32_t &nMsgId, string &strMsgData, IM::BaseDefine::MsgType &nMsgType, uint32_t& nFromId)
{
    string strTableName = "IMGroupMessage_" + int2string(nGroupId % 8);
    
    CDBManager* pDBManager = CDBManager::getInstance();
    CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_slave");
    if (pDBConn)
    {
        string strSql = "select msgId, type,userId, content from " + strTableName + " where groupId = " + int2string(nGroupId) + " and status = 0 order by created desc, id desc limit 1";
        
        CResultSet* pResultSet = pDBConn->ExecuteQuery(strSql.c_str());
        if (pResultSet)
        {
            while(pResultSet->Next()) {
                nMsgId = pResultSet->GetInt("msgId");
                nMsgType = IM::BaseDefine::MsgType(pResultSet->GetInt("type"));
                nFromId = pResultSet->GetInt("userId");
                if(nMsgType == IM::BaseDefine::MSG_TYPE_GROUP_AUDIO)
                {
                    // "[语音]"加密后的字符串
                    strMsgData = strAudioEnc;
                }
                else
                {
                    strMsgData = pResultSet->GetString("content");
                }
            }
            delete pResultSet;
        }
        else
        {
            log("no result set for sql: %s", strSql.c_str());
        }
        pDBManager->RelDBConn(pDBConn);
    }
    else
    {
        log("no db connection for teamtalk_slave");
    }
}

/**
 *  获取某个用户所有群的所有未读计数之和
 *
 *  @param nUserId   用户Id
 *  @param nTotalCnt 未读计数之后,引用
 */
void CGroupMessageModel::getUnReadCntAll(uint32_t nUserId, uint32_t &nTotalCnt)
{
    list<uint32_t> lsGroupId;
    CGroupModel::getInstance()->getUserGroupIds(nUserId, lsGroupId, 0);
    uint32_t nCount = 0;
    
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if (pCacheConn)
    {
        for(auto it=lsGroupId.begin(); it!=lsGroupId.end(); ++it)
        {
            uint32_t nGroupId = *it;
            string strGroupKey = int2string(nGroupId) + GROUP_TOTAL_MSG_COUNTER_REDIS_KEY_SUFFIX;
            string strGroupCnt = pCacheConn->hget(strGroupKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD);
            if(strGroupCnt.empty())
            {
//                log("hget %s : count failed !", strGroupKey.c_str());
                continue;
            }
            uint32_t nGroupCnt = (uint32_t)(atoi(strGroupCnt.c_str()));
            
            string strUserKey = int2string(nUserId) + "_" + int2string(nGroupId) + GROUP_USER_MSG_COUNTER_REDIS_KEY_SUFFIX;
            string strUserCnt = pCacheConn->hget(strUserKey, GROUP_COUNTER_SUBKEY_COUNTER_FIELD);
            
            uint32_t nUserCnt = ( strUserCnt.empty() ? 0 : ((uint32_t)atoi(strUserCnt.c_str())) );
            if(nGroupCnt >= nUserCnt) {
                nCount = nGroupCnt - nUserCnt;
            }
            if(nCount > 0)
            {
                nTotalCnt += nCount;
            }
        }
        pCacheManager->RelCacheConn(pCacheConn);
    }
    else
    {
        log("no cache connection for unread");
    }
}

void CGroupMessageModel::getMsgByMsgId(uint32_t nUserId, uint32_t nGroupId, const list<uint32_t> &lsMsgId, list<IM::BaseDefine::MsgInfo> &lsMsg)
{
    if(!lsMsgId.empty())
    {
        if (CGroupModel::getInstance()->isInGroup(nUserId, nGroupId))
        {
            CDBManager* pDBManager = CDBManager::getInstance();
            CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_slave");
            if (pDBConn)
            {
                string strTableName = "IMGroupMessage_" + int2string(nGroupId % 8);
                uint32_t nUpdated = CGroupModel::getInstance()->getUserJoinTime(nGroupId, nUserId);
                string strClause ;
                bool bFirst = true;
                for(auto it= lsMsgId.begin(); it!=lsMsgId.end();++it)
                {
                    if (bFirst) {
                        bFirst = false;
                        strClause = int2string(*it);
                    }
                    else
                    {
                        strClause += ("," + int2string(*it));
                    }
                }
                
                string strSql = "select * from " + strTableName + " where groupId=" + int2string(nGroupId) + " and msgId in (" + strClause + ") and status=0 and created >= " + int2string(nUpdated) + " order by created desc, id desc limit 100";
                CResultSet* pResultSet = pDBConn->ExecuteQuery(strSql.c_str());
                if (pResultSet)
                {
                    while (pResultSet->Next())
                    {
                        IM::BaseDefine::MsgInfo msg;
                        msg.set_msg_id(pResultSet->GetInt("msgId"));
                        msg.set_from_session_id(pResultSet->GetInt("userId"));
                        msg.set_create_time(pResultSet->GetInt("created"));
                        IM::BaseDefine::MsgType nMsgType = IM::BaseDefine::MsgType(pResultSet->GetInt("type"));
                        if(IM::BaseDefine::MsgType_IsValid(nMsgType))
                        {
                            msg.set_msg_type(nMsgType);
                            msg.set_msg_data(pResultSet->GetString("content"));
                            lsMsg.push_back(msg);
                        }
                        else
                        {
                            log("invalid msgType. userId=%u, groupId=%u, msgType=%u", nUserId, nGroupId, nMsgType);
                        }
                    }
                    delete pResultSet;
                }
                else
                {
                    log("no result set for sql:%s", strSql.c_str());
                }
                pDBManager->RelDBConn(pDBConn);
                if(!lsMsg.empty())
                {
                    CAudioModel::getInstance()->readAudios(lsMsg);
                }
            }
            else
            {
                log("no db connection for teamtalk_slave");
            }
        }
        else
        {
            log("%u is not in group:%u", nUserId, nGroupId);
        }
    }
    else
    {
        log("msgId is empty.");
    }
}

bool CGroupMessageModel::resetMsgId(uint32_t nGroupId)
{
    bool bRet = false;
    CacheManager* pCacheManager = CacheManager::getInstance();
    CacheConn* pCacheConn = pCacheManager->GetCacheConn("unread");
    if(pCacheConn)
    {
        string strKey = "group_msg_id_" + int2string(nGroupId);
        string strValue = "0";
        string strReply = pCacheConn->set(strKey, strValue);
        if(strReply == strValue)
        {
            bRet = true;
        }
        pCacheManager->RelCacheConn(pCacheConn);
    }
    return bRet;
}
