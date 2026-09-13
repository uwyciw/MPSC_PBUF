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

#ifndef _MPSC_PBUF_INTERNAL_H_
#define _MPSC_PBUF_INTERNAL_H_

/* Includes ------------------------------------------------------------------*/
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/**@defgroup MPSC_PBUF_FLAGS MPSC 包缓冲区标志
 * @{
**/

/** 
 * @brief 指示缓冲区满时策略的标志。
 * 若设置此标志，当从满缓冲区分配时，最旧的数据包将被丢弃。
 * 若未设置此标志，分配将返回空指针。
**/
#define MPSC_PBUF_MODE_OVERWRITE (1U << 0)

/** @brief 指示缓冲区当前已满的标志。 */
#define MPSC_PBUF_FULL (1U << 1)

/**
 * @brief 多生产者、单消费者包头
 * @defgroup mpsc_packet MPSC（多生产者、单消费者）包头
 * @ingroup mpsc_buf
 * @{
 */

/** @brief 第一个字中由缓冲区使用的位数。 */
#define MPSC_PBUF_HDR_BITS 2

/** 
 * @brief 必须添加到每个数据包第一个字的头部。
 * 这些字段由包缓冲区控制，除非特别说明，否则不得使用。
 * 字段必须添加在包头结构体的顶部。
 */
#define MPSC_PBUF_HDR \
	uint32_t valid: 1; \
	uint32_t busy: 1

/** @brief 通用包头。 */
typedef struct mpsc_pbuf_hdr_t {
	MPSC_PBUF_HDR;
	uint32_t data: 32 - MPSC_PBUF_HDR_BITS;
} MPSC_PBUF_HDR_T;

/** @brief 包缓冲区内部使用的跳过包。 */
typedef struct mpsc_pbuf_skip_t {
	MPSC_PBUF_HDR;
	uint32_t len: 32 - MPSC_PBUF_HDR_BITS;
} MPSC_PBUF_SKIP_T;

/** @brief 通用包头联合体。 */
typedef union mpsc_pbuf_generic_t {
	MPSC_PBUF_HDR_T hdr;
	MPSC_PBUF_SKIP_T skip;
	uint32_t raw;
} MPSC_PBUF_GENERIC_T;

#endif /* _MPSC_PBUF_INTERNAL_H_ */