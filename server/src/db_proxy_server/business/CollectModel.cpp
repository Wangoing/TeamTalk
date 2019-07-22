/*================================================================
 *   Copyright (C) 2019 All rights reserved.
 *
 *   文件名称：CollectModel.cpp
 *   创 建 者：rendi
 *   邮    箱：
 *   创建日期：2019年7月3日
 *   描    述：
 *
 ================================================================*/

#include <map>
#include <set>

#include "../DBPool.h"
#include "../CachePool.h"
#include "CollectModel.h"
#include "AudioModel.h"
#include "SessionModel.h"
#include "RelationModel.h"

using namespace std;

CCollectModel* CCollectModel::m_pInstance = NULL;
extern string strAudioEnc;

CCollectModel::CCollectModel()
{

}

CCollectModel::~CCollectModel()
{

}

CCollectModel* CCollectModel::getInstance()
{
	if (!m_pInstance) {
		m_pInstance = new CCollectModel();
	}

	return m_pInstance;
}

/*
 * IMCollect 表
 * AddFriendShip()
 * if nFromId or nToId is ShopEmployee
 * GetShopId
 * Insert into IMCollect
 */
bool CCollectModel::addCollect(uint32_t nUserId,uint32_t nRelateId, uint32_t nFromId, uint32_t nToId, uint32_t nGroupId, IM::BaseDefine::MsgType nMsgType, uint32_t nCreateTime, uint32_t nMsgId, string& strMsgContent)
{
    bool bRet =false;
    if (nFromId == 0 || nToId == 0) {
        log("invalied userId.%u->%u", nFromId, nToId);
        return bRet;
    }

	uint32_t cur_time = time(NULL);

	CDBManager* pDBManager = CDBManager::getInstance();
	CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_master");
	if (pDBConn)
    {
        string strTableName = "IMUserCollect";
        string strSql = "insert into " + strTableName + " (`userId`, `relateId`, `fromId`, `toId`, `groupId`, `msgId`, `content`, `status`, `type`, `msgTime`, `created`, `updated`) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        // 必须在释放连接前delete CPrepareStatement对象，否则有可能多个线程操作mysql对象，会crash
        CPrepareStatement* pStmt = new CPrepareStatement();
        if (pStmt->Init(pDBConn->GetMysql(), strSql))
        {
            uint32_t nStatus = 0;
            uint32_t nType = nMsgType;
            uint32_t index = 0;
			pStmt->SetParam(index++, nUserId);
            pStmt->SetParam(index++, nRelateId);
            pStmt->SetParam(index++, nFromId);
            pStmt->SetParam(index++, nToId);
			pStmt->SetParam(index++, nGroupId);
            pStmt->SetParam(index++, nMsgId);
            pStmt->SetParam(index++, strMsgContent);
            pStmt->SetParam(index++, nStatus);
            pStmt->SetParam(index++, nType);
			pStmt->SetParam(index++, nCreateTime);
            pStmt->SetParam(index++, cur_time);
            pStmt->SetParam(index++, cur_time);
            bRet = pStmt->ExecuteUpdate();
        }
        delete pStmt;
        pDBManager->RelDBConn(pDBConn);
        if (bRet)
        {
			log("insert collect success: %s", strSql.c_str());
        }
        else
        {
            log("insert collect failed: %s", strSql.c_str());
        }
	}
    else
    {
        log("no db connection for teamtalk_master");
    }
	return bRet;
}

bool CCollectModel::hasCollect(uint32_t nUserId, uint32_t nMsgId,
                               uint32_t nFromId, uint32_t nToId)
{
	bool bRet =false;

    CDBManager* pDBManager = CDBManager::getInstance();
    CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_slave");
    if (pDBConn)
    {
        string strTableName = "IMUserCollect";
        string strSql;
        strSql = "select * from " + strTableName + " where userId= " + int2string(nUserId) + " and fromId = "+ int2string(nFromId)+" and msgId =" + int2string(nMsgId)+ " and toId= " + int2string(nToId);
        CResultSet* pResultSet = pDBConn->ExecuteQuery(strSql.c_str());
		log("strSql is [%s]",strSql.c_str());
        if (pResultSet)
        {
            while (pResultSet->Next())
            {
                bRet = true;
            }
            delete pResultSet;
        }
        else
        {
            log("no result set: %s", strSql.c_str());
        }
    }
    else
    {
        log("no db connection for teamtalk_slave");
    }

	return bRet;
}


void CCollectModel::getCollect(uint32_t nUserId, uint32_t nMsgId,
                               uint32_t nMsgCnt, list<IM::BaseDefine::MsgInfo>& lsMsg)
{
    CDBManager* pDBManager = CDBManager::getInstance();
    CDBConn* pDBConn = pDBManager->GetDBConn("teamtalk_slave");
    if (pDBConn)
    {
        string strTableName = "IMUserCollect";
        string strSql ;
        if (nMsgId == 0) {
            strSql = "select * from " + strTableName + " where userId= " + int2string(nUserId) + " and status = 0 order by created desc, id desc limit " + int2string(nMsgCnt);
        }
        else
        {
            strSql = "select * from " + strTableName + " where userId= " + int2string(nUserId) + " and status = 0 and id <=" + int2string(nMsgId)+ " order by created desc, id desc limit " + int2string(nMsgCnt);
        }
		log("strSql=[%s]",strSql.c_str());
        CResultSet* pResultSet = pDBConn->ExecuteQuery(strSql.c_str());
        if (pResultSet)
        {
            while (pResultSet->Next())
            {
                IM::BaseDefine::MsgInfo cMsg;
                cMsg.set_msg_id(pResultSet->GetInt("id"));
                cMsg.set_from_session_id(pResultSet->GetInt("fromId"));
                cMsg.set_create_time(pResultSet->GetInt("msgTime"));
                IM::BaseDefine::MsgType nMsgType = IM::BaseDefine::MsgType(pResultSet->GetInt("type"));
                if(IM::BaseDefine::MsgType_IsValid(nMsgType))
                {
                    cMsg.set_msg_type(nMsgType);
                    cMsg.set_msg_data(pResultSet->GetString("content"));
                    lsMsg.push_back(cMsg);
                }
                else
                {
                    log("invalid msgType. userId=%u, msgId=%u, msgCnt=%u, msgType=%u", nUserId, nMsgId, nMsgCnt, nMsgType);
                }
            }
            delete pResultSet;
        }
        else
        {
            log("no result set: %s", strSql.c_str());
        }
        pDBManager->RelDBConn(pDBConn);
        if (!lsMsg.empty())
        {
            //CAudioModel::getInstance()->readAudios(lsMsg);
        }
    }
    else
    {
        log("no db connection for teamtalk_slave");
    }
}

