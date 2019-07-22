/*================================================================
 *   Copyright (C) 2019 All rights reserved.
 *
 *   文件名称：CollectModel.h
 *   创 建 者：rendi
 *   邮    箱：
 *   创建日期：2019年7月3日
 *   描    述：收藏实体模板
 *
 ================================================================*/

#ifndef MESSAGE_MODEL_H_
#define MESSAGE_MODEL_H_

#include <list>
#include <string>

#include "util.h"
#include "ImPduBase.h"
#include "AudioModel.h"
#include "IM.BaseDefine.pb.h"
using namespace std;

class CCollectModel {
public:
	virtual ~CCollectModel();
	static CCollectModel* getInstance();

    bool addCollect(uint32_t nUserId,uint32_t nRelateId, uint32_t nFromId, uint32_t nToId, uint32_t nGroupId,
		IM::BaseDefine::MsgType nMsgType, uint32_t nCreateTime, uint32_t nMsgId, string& strMsgContent);
	void getCollect(uint32_t nUserId,   uint32_t nMsgId, uint32_t nMsgCnt,
                    list<IM::BaseDefine::MsgInfo>& lsMsg);
	bool hasCollect(uint32_t nUserId, uint32_t nMsgId,
                               uint32_t nFromId, uint32_t nToId);
private:
	CCollectModel();
private:
	static CCollectModel*	m_pInstance;
};



#endif /* MESSAGE_MODEL_H_ */

