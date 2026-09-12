/**
  ******************************************************************************
  * @file    mpsc_pbuf.h
  * @author  lx
  * @date    2026-09-12
  * @brief   多生产者、单消费者（Multi-Producer, Single-Consumer）环形包缓冲区
  ******************************************************************************
  *
  ******************************************************************************
***/

#ifndef _MPSC_PBUF_H_
#define _MPSC_PBUF_H_

/* Includes ------------------------------------------------------------------*/
#include "mpsc_packet.h"
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/**
 * @brief 多生产者、单消费者包缓冲区 API
 * @defgroup mpsc_buf MPSC（多生产者、单消费者）包缓冲区 API
 * @ingroup datastructure_apis
 * @{
**/

/*
 * 多生产者、单消费者包缓冲区允许分配可变长度的连续空间来存储数据包。
 * 当空间分配后，用户可以填充数据（前 2 位除外），数据包就绪后提交。
 * 允许在提交前一个数据包之前分配新的数据包。
 *
 * 如果缓冲区已满且无法分配数据包，则返回空指针，除非选择了覆盖模式。
 * 在覆盖模式下，最旧的条目会被丢弃（通知用户），直到分配成功。
 * 可能出现待丢弃的候选包正在被占用的情况，此时跳过该包，丢弃下一个包，
 * 被占用的包在释放时标记为无效。
 *
 * 读取数据包分两步进行：首先声明数据包，声明返回缓冲区内数据包的指针；
 * 数据包不再使用时释放。
**/

/**@defgroup MPSC_PBUF_FLAGS MPSC 包缓冲区标志
 * @{
**/

/** @brief 指示缓冲区大小为 2 的幂的标志。
 *
 * 当缓冲区大小为 2 的幂时，应用优化。
**/
#define MPSC_PBUF_SIZE_POW2 (1U << 0)

/** @brief 指示缓冲区满时策略的标志。
 *
 * 若设置此标志，当从满缓冲区分配时，最旧的数据包将被丢弃。
 * 若未设置此标志，分配将返回空指针。
**/
#define MPSC_PBUF_MODE_OVERWRITE (1U << 1)

/** @brief 指示缓冲区当前已满的标志。 */
#define MPSC_PBUF_FULL (1U << 3)

/**@} */

/* 前向声明 */
struct mpsc_pbuf_buffer_t;

/** @brief 获取数据包长度的回调原型。
 *
 * @param pPacket 用户数据包。
 *
 * @return 数据包的大小，以 32 位字为单位。
**/
typedef uint32_t (*MpscPbufGetWlen_Cb_T)(const MPSC_PBUF_GENERIC_T * pPacket);

/** @brief 数据包被丢弃时的回调。
 *
 * @param pBuffer 包缓冲区。
 *
 * @param pPacket 正在被丢弃的数据包。
 */
typedef void (*MpscPbufNotifyDrop_Cb_T)(const struct mpsc_pbuf_buffer_t * pBuffer,
    const MPSC_PBUF_GENERIC_T * pPacket);

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

    /** 数据包被丢弃时调用的用户回调。
     *
     * 如不需要可设为 NULL。
     */
    MpscPbufNotifyDrop_Cb_T notifyDrop;

    /** 获取数据包长度的回调。 */
    MpscPbufGetWlen_Cb_T getWlen;

    /* 缓冲区。 */
    uint32_t * pBuf;

    /* 缓冲区大小，以 32 位字为单位。 */
    uint32_t size;
} MPSC_PBUF_BUFFER_T;

/** @brief MPSC 包缓冲区配置结构体。 */
typedef struct mpsc_pbuf_buffer_config_t {
    /* 用于存储数据包的内存指针。 */
    uint32_t * pBuf;

    /* 缓冲区大小，以 32 位字为单位。 */
    uint32_t size;

    /* 回调函数。 */
    MpscPbufNotifyDrop_Cb_T notifyDrop;
    MpscPbufGetWlen_Cb_T getWlen;

    /* 配置标志。 */
    uint32_t flags;
} MPSC_PBUF_BUFFER_CONFIG_T;

/** @brief 初始化包缓冲区。
 *
 * @param pBuffer 缓冲区。
 *
 * @param pConfig 配置。
 */
void MpscPbufInit(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_BUFFER_CONFIG_T * pConfig, void (*takeMutex)(void), void (*giveMutex)(void));

/** @brief 分配数据包。
 *
 * 若缓冲区配置为覆盖模式，当没有空间分配新缓冲区时，最旧的数据包将被丢弃。
 * 否则分配失败，返回空指针。
 *
 * @param pBuffer 缓冲区。
 *
 * @param wlen 要分配的字数。
 *
 * @return 指向已分配空间的指针，若无法分配则返回空指针。
 */
MPSC_PBUF_GENERIC_T * MpscPbufAlloc(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t wlen);

/** @brief 提交数据包。
 *
 * @param pBuffer 缓冲区。
 *
 * @param pPacket 由 @ref MpscPbufAlloc 分配的数据包指针。
 */
void MpscPbufCommit(MPSC_PBUF_BUFFER_T * pBuffer, MPSC_PBUF_GENERIC_T * pPacket);

/** @brief 将单字数据包放入缓冲区。
 *
 * 该函数针对可放入单个字的数据包进行了优化。
 * 注意：该字的 2 位由缓冲区使用。
 *
 * @param pBuffer 缓冲区。
 *
 * @param word 数据包内容，包含有效位已设置的 MPSC_PBUF_HDR
 * 以及剩余位上的数据。
 */
void MpscPbufPutWord(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_GENERIC_T word);

/** @brief 放入由一个字和一个指针组成的数据包。
 *
 * 该函数针对由一个字和一个指针组成的数据包进行了优化。
 * 注意：第一个字的 2 位由缓冲区使用。
 *
 * @param pBuffer 缓冲区。
 *
 * @param word 数据包的第一个字，包含有效位已设置的 MPSC_PBUF_HDR
 * 以及剩余位上的数据。
 *
 * @param pData 用户数据。
 */
void MpscPbufPutWordExt(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_GENERIC_T word, const void * pData);

/** @brief 将数据包放入缓冲区。
 *
 * 将数据复制到缓冲区中。
 * 注意：第一个字的 2 位由缓冲区使用。
 *
 * @param pBuffer 缓冲区。
 *
 * @param pData 数据的第一个字必须包含有效位已设置的 MPSC_PBUF_HDR。
 *
 * @param wlen 数据包大小，以字为单位。
 */
void MpscPbufPutData(MPSC_PBUF_BUFFER_T * pBuffer, const uint32_t * pData, uint32_t wlen);

/** @brief 声明第一个待处理的数据包。
 *
 * @param pBuffer 缓冲区。
 *
 * @return 指向已声明数据包的指针，若无可用数据包则返回空指针。
 */
const MPSC_PBUF_GENERIC_T * MpscPbufClaim(MPSC_PBUF_BUFFER_T * pBuffer);

/** @brief 释放数据包。
 *
 * @param pBuffer 缓冲区。
 *
 * @param pPacket 数据包。
 */
void MpscPbufFree(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_GENERIC_T * pPacket);

/** @brief 检查是否有待处理的消息。
 *
 * @param pBuffer 缓冲区。
 *
 * @retval true 有待处理的消息。
 * @retval false 没有待处理的消息。
 */
bool MpscPbufIsPending(MPSC_PBUF_BUFFER_T * pBuffer);

/**
 * @}
 */

#endif /* _MPSC_PBUF_H_ */