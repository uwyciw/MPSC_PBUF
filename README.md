# MPSC Packet Buffer

多生产者、单消费者（Multi-Producer, Single-Consumer）环形包缓冲区。以可变长度数据包为单位进行 FIFO 存储，适用于多线程/多中断上下文下的数据传输场景。

## 功能特色

- **多生产者并发安全**：多个生产者可同时分配空间，内部使用互斥锁保护临界区
- **单消费者模型**：消费者独占读取，无需额外同步
- **可变长度数据包**：每个包长度独立，分配时保证连续空间
- **零拷贝消费**：消费者通过 Claim 拿到指针直接操作，释放时回收
- **两步式生产**：Alloc → 填充 → Commit，生产者可以在 Commit 之前继续分配新包
- **覆盖模式**：满时自动丢弃最旧包（支持正在消费的包安全跳过），并通知用户
- **无覆盖模式**：满时返回 NULL，由上层决定重试或丢弃
- **快路径优化 API**：`PutWord` / `PutWordExt` 针对单字 / 单字+指针的小包做了值传递优化
- **零外部依赖**：仅使用 C11 标准库（`<stdint.h>` `<stdbool.h>` `<stddef.h>` `<string.h>`），通过函数指针注入互斥锁，可适配任意 RTOS 或裸机
- **2 的幂优化**：缓冲区大小为 2^n 时自动用位与替代取模运算

## 文件结构

```
MPSC_PBUF/
├── Inc/
│   ├── mpsc_packet.h   包头定义（valid / busy 标志位 + 跳过包结构）
│   └── mpsc_pbuf.h     缓冲区结构体与 API 声明
├── Src/
│   └── mpsc_pbuf.c     API 实现
└── LICENSE
```

## 实现原理

### 包头结构

每个数据包的第一个 32 位字的高 2 位由缓冲区内部使用：

```
| 位 31..2 | 位 1  | 位 0  |
|----------|-------|-------|
| 用户数据  | busy  | valid |
```

| valid | busy | 含义 |
|-------|------|------|
| 0     | 0    | 空闲，可被分配 |
| 1     | 0    | 有效数据包，等待消费 |
| 1     | 1    | 已被消费者声明（Claim），正在使用 |
| 0     | 1    | 内部跳过包（填充 / 丢弃标记） |

用户自定义包头需要在结构体**顶部**嵌入 `MPSC_PBUF_HDR` 宏：

```c
typedef struct {
    MPSC_PBUF_HDR;          // valid + busy，必须放在首位
    uint32_t length: 30;    // 用户可自由使用剩余 30 位
} my_packet_t;
```

### 四索引模型

缓冲区维护两对读写索引，分别服务于生产者和消费者：

| 索引 | 角色 | 含义 |
|------|------|------|
| `tmpWrIdx` | 生产者前沿 | 所有已分配但未 Commit 的包都占用到此位置 |
| `wrIdx`    | 生产者提交点 | 已 Commit 的数据包末尾 |
| `tmpRdIdx` | 消费者前沿 | 已 Claim 但未 Free 的包的下一个位置 |
| `rdIdx`    | 消费者回收点 | 实际已释放空间的位置 |

核心判断逻辑：
- **空闲空间** = `rdIdx` 到 `tmpWrIdx` 之间（含回绕）
- **可读取数据** = `tmpRdIdx` 到 `wrIdx` 之间（含回绕）
- **满**：`tmpWrIdx` 追上 `rdIdx` 时置 `MPSC_PBUF_FULL` 标志
- **区分满/空**：牺牲一个字的容量，实际可用 = `size - 1`

### 跳过包（Skip Packet）

环形缓冲区回绕时，末尾可能剩下一小块空间不够放下一个包。这时会写入一个跳过包（`valid=0, busy=1`，`len` 字段记录跳过长度），直接把写索引跳到缓冲区开头，避免碎片。

### 覆盖模式下的忙包保护

开启 `MPSC_PBUF_MODE_OVERWRITE` 后，缓冲区满时会尝试丢弃最旧包腾出空间。但如果最旧包正被消费者持有（`busy=1`），不会强行覆盖，而是：

1. 在忙包之前剩余的空闲位置填入跳过包
2. 所有写/读索引直接前移到忙包之后
3. 忙包被 Free 时检测到自己已被跳过，自动转换为跳过包而不是正常回收

这样就保证了消费者的指针始终有效。

### 互斥锁注入

核心写入路径（PutWord / Alloc / Commit / PutWordExt / PutData）和读取路径（Claim / Free / IsPending）的临界区都通过 `takeMutex` / `giveMutex` 函数指针保护。传入 `NULL` 时跳过锁操作，适合裸机或单线程场景。

典型适配：

```c
static void MutexTake(void)   { pthread_mutex_lock(&g_mutex); }
static void MutexGive(void)  { pthread_mutex_unlock(&g_mutex); }

MpscPbufInit(&buf, &cfg, MutexTake, MutexGive);
```

## API 参考

### 初始化

```c
void MpscPbufInit(MPSC_PBUF_BUFFER_T * pBuffer,
                  const MPSC_PBUF_BUFFER_CONFIG_T * pConfig,
                  void (*takeMutex)(void),
                  void (*giveMutex)(void));
```

`pConfig` 字段：

| 字段 | 说明 |
|------|------|
| `pBuf` | 用户提供的 `uint32_t` 数组，作为缓冲区存储 |
| `size` | 数组长度（32 位字数），实际可用 `size - 1` |
| `getWlen` | 回调：给定数据包指针返回其字数 |
| `notifyDrop` | 回调：数据包因覆盖被丢弃时调用（可为 NULL） |
| `flags` | `0` 或 `MPSC_PBUF_MODE_OVERWRITE` |

### 生产（两步法）

```c
// 第一步：分配空间，返回缓冲区内部指针（valid/busy 已清零）
MPSC_PBUF_GENERIC_T * pkt = MpscPbufAlloc(&buf, wlen);
if (!pkt) { /* 满了 */ }

// 第二步：填充数据，设置 valid 位，提交
((my_packet_t *)pkt)->hdr.valid = 1;
MpscPbufCommit(&buf, pkt);
```

### 生产（单步快路径）

```c
// 单字小包（第一个字里必须预先设置好 valid=1）
MPSC_PBUF_GENERIC_T word = { .hdr.valid = 1, .data = 0x1234 };
MpscPbufPutWord(&buf, word);

// 单字 + 外部指针
MpscPbufPutWordExt(&buf, word, (void *)external_data);

// 多字拷贝
uint32_t raw[4] = { 0x80000001, 0xDEADBEEF, 0xCAFEBABE, 0x12345678 };
MpscPbufPutData(&buf, raw, 4);
```

### 消费

```c
// 声明：获取第一个待处理的包（自动标记 busy=1）
const MPSC_PBUF_GENERIC_T * pkt = MpscPbufClaim(&buf);
if (pkt) {
    // 处理数据……
    // 释放：标记 valid=0，busy=0，空间回收
    MpscPbufFree(&buf, pkt);
}
```

### 其他

```c
bool MpscPbufIsPending(&buf);   // 是否有可读取的包
```

## 完整使用示例

```c
#include "Inc/mpsc_pbuf.h"

#define BUF_WORDS 128

static uint32_t g_buf[BUF_WORDS];

typedef struct {
    MPSC_PBUF_HDR;
    uint32_t id:  22;
    uint32_t len: 10;
} msg_hdr_t;

typedef struct {
    msg_hdr_t hdr;
    uint32_t payload[8];
} msg_t;

static uint32_t GetMsgWlen(const MPSC_PBUF_GENERIC_T * pkt)
{
    return sizeof(msg_t) / sizeof(uint32_t);
}

int main(void)
{
    MPSC_PBUF_BUFFER_T pbuf;
    MPSC_PBUF_BUFFER_CONFIG_T cfg = {
        .pBuf     = g_buf,
        .size     = BUF_WORDS,
        .getWlen  = GetMsgWlen,
        .notifyDrop = NULL,
        .flags    = MPSC_PBUF_MODE_OVERWRITE,
    };

    // 无互斥锁 = 单线程/裸机场景
    MpscPbufInit(&pbuf, &cfg, NULL, NULL);

    // 生产
    msg_t * m = (msg_t *)MpscPbufAlloc(&pbuf, GetMsgWlen(NULL));
    m->hdr.id  = 42;
    m->hdr.len = 8;
    for (int i = 0; i < 8; i++) m->payload[i] = i * 0x10001;
    m->hdr.valid = 1;
    MpscPbufCommit(&pbuf, (MPSC_PBUF_GENERIC_T *)m);

    // 消费
    const msg_t * r = (const msg_t *)MpscPbufClaim(&pbuf);
    if (r) {
        // 处理 r->payload ……
        MpscPbufFree(&pbuf, (const MPSC_PBUF_GENERIC_T *)r);
    }

    return 0;
}
```

## 编译

仅依赖 C11 标准库：

```bash
# GCC / Clang
gcc -Wall -Wextra -std=c11 -IInc -c Src/mpsc_pbuf.c

# MSVC
cl /W4 /std:c11 /IInc /c Src\mpsc_pbuf.c
```

## 迁移自 Zephyr

本项目基于 Zephyr RTOS `sys/mpsc_pbuf` 改写，已移除所有 Zephyr 依赖：
