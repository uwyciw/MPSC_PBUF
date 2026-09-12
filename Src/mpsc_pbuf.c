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

/* Includes ------------------------------------------------------------------*/
#include "mpsc_pbuf.h"

void MpscPbufInit(MPSC_PBUF_BUFFER_T * pBuffer, bool overwriteMode, MpscPbufGetWlen_Cb_T getWlen, uint32_t * pBuf, uint32_t size, void (*takeMutex)(void), void (*giveMutex)(void))
{
    memset(pBuffer, 0, offsetof(MPSC_PBUF_BUFFER_T, pBuf));
    pBuffer->getWlen = getWlen;
    pBuffer->pBuf = pBuf;
    pBuffer->size = size;
    pBuffer->flags = overwriteMode ? MPSC_PBUF_MODE_OVERWRITE : 0;
    pBuffer->takeMutex = takeMutex;
    pBuffer->giveMutex = giveMutex;
}

/* 计算可用空闲空间或到缓冲区末尾的空闲空间。
 *
 * @param pBuffer 缓冲区。
 * @param[out] pRes 写入空闲空间值的目标地址。
 *
 * @retval true 当空间计算到缓冲区末尾（回绕后可能还有更多可用空间）。
 * @retval false 当结果为总空闲空间。
 */
static inline bool FreeSpace(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t * pRes)
{
    if (pBuffer->flags & MPSC_PBUF_FULL) {
        *pRes = 0;
        return false;
    }

    if (pBuffer->rdIdx > pBuffer->tmpWrIdx) {
        *pRes = pBuffer->rdIdx - pBuffer->tmpWrIdx;
        return false;
    }
    *pRes = pBuffer->size - pBuffer->tmpWrIdx;

    return true;
}

/* 获取有效数据量。
 *
 * @param pBuffer 缓冲区。
 * @param[out] pRes 写入可用空间值的目标地址。
 *
 * @retval true 当空间计算到缓冲区末尾（回绕后可能还有更多可用空间）。
 * @retval false 当结果为总空闲空间。
 */
static inline bool Available(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t * pRes)
{
    if (pBuffer->flags & MPSC_PBUF_FULL || pBuffer->tmpRdIdx > pBuffer->wrIdx) {
        *pRes = pBuffer->size - pBuffer->tmpRdIdx;
        return true;
    }

    *pRes = (pBuffer->wrIdx - pBuffer->tmpRdIdx);

    return false;
}

static inline bool IsValid(MPSC_PBUF_GENERIC_T * pItem)
{
    return (bool)pItem->hdr.valid;
}

static inline bool IsInvalid(MPSC_PBUF_GENERIC_T * pItem)
{
    return !pItem->hdr.valid && !pItem->hdr.busy;
}

static inline uint32_t IdxInc(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t idx, uint32_t val)
{
    uint32_t i = idx + val;
    return (i >= pBuffer->size) ? i - pBuffer->size : i;
}

static inline uint32_t GetSkip(MPSC_PBUF_GENERIC_T * pItem)
{
    if (pItem->hdr.busy && !pItem->hdr.valid) {
        return pItem->skip.len;
    }

    return 0;
}

static inline void TmpWrIdxInc(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t wlen)
{
    pBuffer->tmpWrIdx = IdxInc(pBuffer, pBuffer->tmpWrIdx, wlen);
    if (pBuffer->tmpWrIdx == pBuffer->rdIdx) {
        pBuffer->flags |= MPSC_PBUF_FULL;
    }
}

static void RdIdxInc(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t wlen)
{
    pBuffer->rdIdx = IdxInc(pBuffer, pBuffer->rdIdx, wlen);
    pBuffer->flags &= ~MPSC_PBUF_FULL;
}

static void AddSkipItem(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t wlen)
{
    MPSC_PBUF_GENERIC_T skip = {
        .skip = { .valid = 0, .busy = 1, .len = wlen }
    };

    pBuffer->pBuf[pBuffer->tmpWrIdx] = skip.raw;
    TmpWrIdxInc(pBuffer, wlen);
    pBuffer->wrIdx = IdxInc(pBuffer, pBuffer->wrIdx, wlen);
}

/**
 * @brief 丢弃 rdIdx 处的数据包以释放空间。
 *
 * 依次处理三种情况：
 *   1. 遇到跳过包（skip item）：直接推进 rdIdx 释放空间。
 *   2. 覆盖模式下遇到有效且空闲的数据包：标记为无效并推进索引，
 *      同时通过 @p pTmpWrIdxShift 告知调用者丢弃造成的 tmpWrIdx 偏移，
 *      调用者需在释放互斥锁后调用 PostDropAction 完成收尾。
 *   3. 覆盖模式下遇到有效但正忙（busy）的数据包：无法丢弃，改为
 *      添加跳过包占位，将所有索引前移至该忙包之后继续寻找可丢弃项。
 *
 * @note 调用时必须已持有互斥锁。
 *
 * @param pBuffer   缓冲区。
 * @param freeWlen  到缓冲区末尾的空闲字数（回绕场景下用于填充）。
 * @param pTmpWrIdxShift 输出参数：丢弃操作导致的 tmpWrIdx 偏移量，
 *                       仅在真正丢弃了有效数据包时非零。
 *
 * @retval true  已释放空间，调用者应重试分配（或后续处理忙包）。
 * @retval false 无包可丢弃（非覆盖模式或遇到无效包），分配应中止。
 */
static bool DropItemLocked(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t freeWlen, uint32_t * pTmpWrIdxShift)
{
    MPSC_PBUF_GENERIC_T * pItem;
    uint32_t skipWlen;

    pItem = (MPSC_PBUF_GENERIC_T *)&pBuffer->pBuf[pBuffer->rdIdx];
    skipWlen = GetSkip(pItem);
    *pTmpWrIdxShift = 0;

    if (skipWlen) {
        /* 找到跳过包，可以丢弃以释放空间 */
        RdIdxInc(pBuffer, skipWlen);
        pBuffer->tmpRdIdx = pBuffer->rdIdx;
        return true;
    }

    /* 其他丢弃选项仅在覆盖模式下可用。 */
    if (!(pBuffer->flags & MPSC_PBUF_MODE_OVERWRITE)) {
        return false;
    }

    uint32_t rdWlen = pBuffer->getWlen(pItem);

    /* 若数据包正忙则需跳过。 */
    if (!IsValid(pItem)) {
        return false;
    } else if (pItem->hdr.busy) {
        bool isRet = true;

        /* 在被占用的包之前添加跳过包。 */
        if (freeWlen) {
            AddSkipItem(pBuffer, freeWlen);
        }
        /* 将所有索引前移至被占用包之后。 */
        pBuffer->wrIdx = IdxInc(pBuffer, pBuffer->wrIdx, rdWlen);

        /* 若分配回绕缓冲区后发现已被跳过的忙包，
         * 再次跳过并指示没有数据包被丢弃。
         */
        if (pBuffer->rdIdx == pBuffer->tmpRdIdx) {
            pBuffer->tmpRdIdx = IdxInc(pBuffer, pBuffer->tmpRdIdx, rdWlen);
            isRet = false;
        }

        pBuffer->tmpWrIdx = pBuffer->tmpRdIdx;
        pBuffer->rdIdx = pBuffer->tmpRdIdx;
        pBuffer->flags |= MPSC_PBUF_FULL;
        return isRet;
    } else {
        /* 准备丢弃数据包。 */
        RdIdxInc(pBuffer, rdWlen);
        pBuffer->tmpRdIdx = pBuffer->rdIdx;
        /* 临时前移 tmp_wr 索引，确保数据包不会被重复丢弃，
         * 内容不会被覆盖。
         */
        if (freeWlen) {
            /* 将空闲位置标记为无效，防止读取不完整数据。 */
            MPSC_PBUF_GENERIC_T invalid = {
                .hdr = {
                    .valid = 0,
                    .busy = 0
                }
            };

            pBuffer->pBuf[pBuffer->tmpWrIdx] = invalid.raw;
        }

        *pTmpWrIdxShift = rdWlen + freeWlen;
        pBuffer->tmpWrIdx = IdxInc(pBuffer, pBuffer->tmpWrIdx, *pTmpWrIdxShift);
        pBuffer->flags |= MPSC_PBUF_FULL;
        pItem->hdr.valid = 0;
    }

    return true;
}

static void PostDropAction(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t prevTmpWrIdx, uint32_t tmpWrIdxShift)
{
    uint32_t cmpTmpWrIdx = IdxInc(pBuffer, prevTmpWrIdx, tmpWrIdxShift);

    if (cmpTmpWrIdx == pBuffer->tmpWrIdx) {
        /* 操作未被其他分配中断。 */
        pBuffer->tmpWrIdx = prevTmpWrIdx;
        pBuffer->flags &= ~MPSC_PBUF_FULL;
        return;
    }

    /* 操作被中断，将该区域标记为待跳过。 */
    MPSC_PBUF_GENERIC_T skip = {
        .skip = {
            .valid = 0,
            .busy = 1,
            .len = tmpWrIdxShift
        }
    };

    pBuffer->pBuf[prevTmpWrIdx] = skip.raw;
    pBuffer->wrIdx = IdxInc(pBuffer,
        pBuffer->wrIdx,
        tmpWrIdxShift);
}

void MpscPbufPutWord(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_GENERIC_T item)
{
    bool isCont;
    uint32_t freeWlen;
    uint32_t tmpWrIdxShift = 0;
    uint32_t tmpWrIdxVal = 0;

    do {
        if (pBuffer->takeMutex) pBuffer->takeMutex();

        if (tmpWrIdxShift) {
            PostDropAction(pBuffer, tmpWrIdxVal, tmpWrIdxShift);
            tmpWrIdxShift = 0;
        }

        (void)FreeSpace(pBuffer, &freeWlen);

        if (freeWlen) {
            pBuffer->pBuf[pBuffer->tmpWrIdx] = item.raw;
            TmpWrIdxInc(pBuffer, 1);
            isCont = false;
            pBuffer->wrIdx = IdxInc(pBuffer, pBuffer->wrIdx, 1);
        } else {
            tmpWrIdxVal = pBuffer->tmpWrIdx;
            isCont = DropItemLocked(pBuffer, freeWlen, &tmpWrIdxShift);
        }

        if (pBuffer->giveMutex) pBuffer->giveMutex();
    } while (isCont);
}

MPSC_PBUF_GENERIC_T * MpscPbufAlloc(MPSC_PBUF_BUFFER_T * pBuffer, uint32_t wlen)
{
    MPSC_PBUF_GENERIC_T * pItem = NULL;
    bool isCont = true;
    uint32_t freeWlen;
    uint32_t tmpWrIdxShift = 0;
    uint32_t tmpWrIdxVal = 0;

    if (wlen > (pBuffer->size)) {
        return NULL;
    }

    do {
        bool isWrap;

        if (pBuffer->takeMutex) pBuffer->takeMutex();
        if (tmpWrIdxShift) {
            PostDropAction(pBuffer, tmpWrIdxVal, tmpWrIdxShift);
            tmpWrIdxShift = 0;
        }

        isWrap = FreeSpace(pBuffer, &freeWlen);

        if (freeWlen >= wlen) {
            pItem = (MPSC_PBUF_GENERIC_T *)&pBuffer->pBuf[pBuffer->tmpWrIdx];
            pItem->hdr.valid = 0;
            pItem->hdr.busy = 0;
            TmpWrIdxInc(pBuffer, wlen);
            isCont = false;
        } else if (isWrap) {
            AddSkipItem(pBuffer, freeWlen);
            isCont = true;
        } else if (isCont) {
            tmpWrIdxVal = pBuffer->tmpWrIdx;
            isCont = DropItemLocked(pBuffer, freeWlen, &tmpWrIdxShift);
        }
        if (pBuffer->giveMutex) pBuffer->giveMutex();
    } while (isCont);

    return pItem;
}

void MpscPbufCommit(MPSC_PBUF_BUFFER_T * pBuffer, MPSC_PBUF_GENERIC_T * pItem)
{
    uint32_t wlen = pBuffer->getWlen(pItem);

    if (pBuffer->takeMutex) pBuffer->takeMutex();

    pItem->hdr.valid = 1;
    pBuffer->wrIdx = IdxInc(pBuffer, pBuffer->wrIdx, wlen);

    if (pBuffer->giveMutex) pBuffer->giveMutex();
}

void MpscPbufPutWordExt(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_GENERIC_T item, const void * pData)
{
    static const uint32_t l =
        (uint32_t)(sizeof(item) + sizeof(pData)) / sizeof(uint32_t);
    bool isCont;
    uint32_t tmpWrIdxShift = 0;
    uint32_t tmpWrIdxVal = 0;

    do {
        uint32_t freeWlen;
        bool isWrap;

        if (pBuffer->takeMutex) pBuffer->takeMutex();

        if (tmpWrIdxShift) {
            PostDropAction(pBuffer, tmpWrIdxVal, tmpWrIdxShift);
            tmpWrIdxShift = 0;
        }

        isWrap = FreeSpace(pBuffer, &freeWlen);

        if (freeWlen >= l) {
            pBuffer->pBuf[pBuffer->tmpWrIdx] = item.raw;
            void ** pp =
                (void **)&pBuffer->pBuf[pBuffer->tmpWrIdx + 1];

            *pp = (void *)pData;
            TmpWrIdxInc(pBuffer, l);
            pBuffer->wrIdx = IdxInc(pBuffer, pBuffer->wrIdx, l);
            isCont = false;
        } else if (isWrap) {
            AddSkipItem(pBuffer, freeWlen);
            isCont = true;
        } else {
            tmpWrIdxVal = pBuffer->tmpWrIdx;
            isCont = DropItemLocked(pBuffer, freeWlen, &tmpWrIdxShift);
        }

        if (pBuffer->giveMutex) pBuffer->giveMutex();
    } while (isCont);
}

void MpscPbufPutData(MPSC_PBUF_BUFFER_T * pBuffer, const uint32_t * pData, uint32_t wlen)
{
    bool isCont;
    uint32_t tmpWrIdxShift = 0;
    uint32_t tmpWrIdxVal = 0;

    do {
        uint32_t freeWlen;
        bool isWrap;

        if (pBuffer->takeMutex) pBuffer->takeMutex();

        if (tmpWrIdxShift) {
            PostDropAction(pBuffer, tmpWrIdxVal, tmpWrIdxShift);
            tmpWrIdxShift = 0;
        }

        isWrap = FreeSpace(pBuffer, &freeWlen);

        if (freeWlen >= wlen) {
            memcpy(&pBuffer->pBuf[pBuffer->tmpWrIdx], pData,
                wlen * sizeof(uint32_t));
            pBuffer->wrIdx = IdxInc(pBuffer, pBuffer->wrIdx, wlen);
            TmpWrIdxInc(pBuffer, wlen);
            isCont = false;
        } else if (isWrap) {
            AddSkipItem(pBuffer, freeWlen);
            isCont = true;
        } else {
            tmpWrIdxVal = pBuffer->tmpWrIdx;
            isCont = DropItemLocked(pBuffer, freeWlen, &tmpWrIdxShift);
        }

        if (pBuffer->giveMutex) pBuffer->giveMutex();
    } while (isCont);
}

const MPSC_PBUF_GENERIC_T * MpscPbufClaim(MPSC_PBUF_BUFFER_T * pBuffer)
{
    MPSC_PBUF_GENERIC_T * pItem;
    bool isCont;

    do {
        uint32_t availableWlen;

        isCont = false;

        if (pBuffer->takeMutex) pBuffer->takeMutex();

        (void)Available(pBuffer, &availableWlen);
        pItem = (MPSC_PBUF_GENERIC_T *)
            &pBuffer->pBuf[pBuffer->tmpRdIdx];

        if (!availableWlen || IsInvalid(pItem)) {
            pItem = NULL;
        } else {
            uint32_t skipWlen = GetSkip(pItem);

            if (skipWlen || !IsValid(pItem)) {
                uint32_t inc =
                    skipWlen ? skipWlen : pBuffer->getWlen(pItem);

                pBuffer->tmpRdIdx =
                    IdxInc(pBuffer, pBuffer->tmpRdIdx, inc);
                RdIdxInc(pBuffer, inc);
                isCont = true;
            } else {
                pItem->hdr.busy = 1;
                pBuffer->tmpRdIdx =
                    IdxInc(pBuffer, pBuffer->tmpRdIdx,
                        pBuffer->getWlen(pItem));
            }
        }

        if (pBuffer->giveMutex) pBuffer->giveMutex();

    } while (isCont);

    return pItem;
}

void MpscPbufFree(MPSC_PBUF_BUFFER_T * pBuffer, const MPSC_PBUF_GENERIC_T * pItem)
{
    uint32_t wlen = pBuffer->getWlen(pItem);

    if (pBuffer->takeMutex) pBuffer->takeMutex();

    MPSC_PBUF_GENERIC_T * pWitem = (MPSC_PBUF_GENERIC_T *)pItem;

    pWitem->hdr.valid = 0;
    if (!(pBuffer->flags & MPSC_PBUF_MODE_OVERWRITE) ||
        ((uint32_t *)pItem == &pBuffer->pBuf[pBuffer->rdIdx])) {
        pWitem->hdr.busy = 0;
        if (pBuffer->rdIdx == pBuffer->tmpRdIdx) {
            /* 在声明和释放之间可能添加了很多新数据包，
             * 导致 rd_idx 再次指向被声明的项。此时 tmp_rd_idx
             * 指向同一位置。在这种情况下，同时递增 tmp_rd_idx，
             * 将释放的缓冲区标记为唯一的空闲空间。
             */
            pBuffer->tmpRdIdx = IdxInc(pBuffer, pBuffer->tmpRdIdx, wlen);
        }
        RdIdxInc(pBuffer, wlen);
    } else {
        pWitem->skip.len = wlen;
    }

    if (pBuffer->giveMutex) pBuffer->giveMutex();
}

bool MpscPbufIsPending(MPSC_PBUF_BUFFER_T * pBuffer)
{
    uint32_t availableWlen;

    if (pBuffer->takeMutex) pBuffer->takeMutex();
    (void)Available(pBuffer, &availableWlen);
    if (pBuffer->giveMutex) pBuffer->giveMutex();

    return availableWlen ? true : false;
}