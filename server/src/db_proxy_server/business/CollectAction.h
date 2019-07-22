/*================================================================
 *   Copyright (C) 2019 All rights reserved.
 *
 *   文件名称：CollectAction.h
 *   创 建 者：rendi
 *   邮    箱：
 *   创建日期：2019年7月3日
 *   描    述：
 *
 ================================================================*/

#ifndef COLLECTACTION_H_
#define COLLECTACTION_H_

#include "ImPduBase.h"

namespace DB_PROXY {
    
    void addCollect(CImPdu* pPdu, uint32_t conn_uuid);
	void getCollect(CImPdu* pPdu, uint32_t conn_uuid);

};



#endif /* COLLECTACTION_H_ */

