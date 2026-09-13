/**
  ******************************************************************************
  * @file    mpsc_pbuf.h
  * @author  lx
  * @date    2026-09-12
  * @brief   多生产者、单消费者（Multi-Producer, Single-Consumer）环形包缓冲区
  ******************************************************************************
  * @attention 消费者需要严格按照Claim->处理->Free->下一次Claim的顺序交替进行，不允许在
  *            上一个Claim还没有Free时，调用Claim申请新的数据包。
  * @details 多生产者、单消费者包缓冲区允许分配可变长度的连续空间来存储数据包。
 *           当空间分配后，用户可以填充数据（被占用的 2 位除外），数据包就绪后提交。
 *           允许在提交前一个数据包之前分配新的数据包，且允许乱序提交。
 *           如果缓冲区已满且无法分配数据包，则返回空指针，除非选择了覆盖模式。
 *           在覆盖模式下，最旧的条目会被丢弃，直到分配成功。
 *           可能出现待丢弃的候选包正在被占用的情况，此时跳过该包，被占用的包
 *           在释放时会被转换为跳过包。
 *           读取数据包分两步进行：首先声明数据包，声明返回缓冲区内数据包的指针；
 *           数据包不再使用时释放。声明与释放必须严格交替进行。
**/

#ifndef _MPSC_PBUF_H_
#define _MPSC_PBUF_H_

/* Includes ------------------------------------------------------------------*/
#include "mpsc_pbuf_internal.h"

/** 
 * @brief 获取数据包长度的回调原型。
 * @note 该回调在持有互斥锁的临界区内被调用，必须快速返回且无副作用；
 *       返回值必须与分配该数据包时使用的字数一致。
 * @param pPacket 用户数据包。
 * @return 数据包的大小，以 32 位字为单位。
**/
typedef uint32_t (*MpscPbufGetWlen_Cb_T)(const MPSC_PBUF_GENERIC_T * pPacket);

/** @brief MPSC 包缓冲区结构体。 */
typedef struct mpsc_pbuf_buffer_t {
    /** 临时写索引。 */
    uint32_t tmpWrIdx;

    /** 写索引。 */
    uint32_t wrIdx;

    /** 临时读索引。 */
    uint32_t tmpRdIdx;

    /** 读索引。 */
    uint32_t rdIdx;

    /** 标志。 */
    uint32_t flags;

    /** 互斥锁。 */
    void(*takeMutex)(void);
    void(*giveMutex)(void);

    /** 获取数据包长度的回调。 */
    MpscPbufGetWlen_Cb_T getWlen;

    /* 缓冲区。 */
    uint32_t * pBuf;

    /* 缓冲区大小，以 32 位字为单位。 */
    uint32_t size;
} MPSC_PBUF_BUFFER_T;

/** 
 * @brief 初始化包缓冲区。
 * @param pBuffer 缓冲区。
 * @param overwriteMode 若为 true，为覆盖模式，缓冲区满时丢弃最旧数据包，若为 false，为非覆盖模式，缓冲区满时返回空指针。
 * @param getWlen 获取数据包长度的回调。
 * @param pBuf 用户提供的缓冲区存储。
 * @param size 缓冲区大小，以 32 位字为单位。
 * @param takeMutex 获取互斥锁函数，可设为 NULL。
 * @param giveMutex 释放互斥锁函数，可设为 NULL。
 */
void MpscPbufInit(MPSC_PBUF_BUFFER_T * pBuffer, bool overwriteMode, MpscPbufGetWlen_Cb_T getWlen, uint32_t * pBuf, uint32_t size, void (*takeMutex)(void), void (*giveMutex)(void));

/** 
 * @brief 分配数据包。
 * @param pBuffer 缓冲区。
 * @param wlen 要分配的字数。
 * @return 指向已分配空间的指针，若无法分配则返回空指针。
 */
MPSC_PBUF_GENERIC_T * MpscPbufAlloc(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t wlen);

/** 
 * @brief 提交数据包。
 * @note 提交时内部会设置 valid 位，调用者无需提前设置。
 * @param pBuffer 缓冲区。
 * @param pPacket 由 @ref MpscPbufAlloc 分配的数据包指针。
 */
void MpscPbufCommit(MPSC_PBUF_BUFFER_T * pBuffer, MPSC_PBUF_GENERIC_T * pPacket);

/** 
 * @brief 声明第一个待处理的数据包。
 * @note 必须与 @ref MpscPbufFree 严格交替使用：上一个声明的数据包释放之前，不允许再次声明新的数据包。
 * @param pBuffer 缓冲区。
 * @return 指向已声明数据包的指针，若无可用数据包则返回空指针。
 */
const MPSC_PBUF_GENERIC_T * MpscPbufClaim(MPSC_PBUF_BUFFER_T * pBuffer);

/** 
 * @brief 释放数据包。
 * @note 必须与 @ref MpscPbufClaim 严格交替使用。
 * @param pBuffer 缓冲区。
 * @param pPacket 由 @ref MpscPbufClaim 声明的数据包指针。
 */
void MpscPbufFree(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_GENERIC_T * pPacket);

/** 
 * @brief 检查是否有待处理的消息。
 * @param pBuffer 缓冲区。
 * @retval true 有待处理的消息。
 * @retval false 没有待处理的消息。
 */
bool MpscPbufIsPending(MPSC_PBUF_BUFFER_T * pBuffer);

#endif /* _MPSC_PBUF_H_ */