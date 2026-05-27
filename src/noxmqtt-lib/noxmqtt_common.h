/*****************************************************************************
* Copyright (c) [2024] - [2026], Argenox Technologies LLC
* All rights reserved.
* SPDX-License-Identifier: GPL-2.0-only OR LicenseRef-Argenox-Commercial
*
*
* This file is part of the NoxMQTT Library.
*
* Licensed under the GNU General Public License v2.0 or later,
* or alternatively under a commercial license from
* Argenox Technologies LLC.
*
* See the LICENSE file in the project root for full details.
* CONTACT: info@argenox.com
*
*
* File:    noxmqtt_common.h
* Summary: Common
*
*****************************************************************************/

#ifndef _NOXMQTT_COMMON_H_
#define _NOXMQTT_COMMON_H_

#ifdef __cplusplus
extern "C" {
#endif

#define ARRAY_LEN(X)  (sizeof(X)  / sizeof(X[0]))
#define MEMZERO(X)     memset(X, 0, sizeof(X))
#define MEMZERO_S(X)   memset(&X, 0, sizeof(X))

    /* GNU GCC Packing */
#ifdef __GNUC__
#define PACK_STRUCT( __Declaration__ ) __Declaration__ __attribute__((__packed__))
#endif

/* IAR Packing */
#ifdef ICCARM
#define PACK_STRUCT( __Declaration__ ) __Declaration__
#endif

/* Visual Studio Packing */
#ifdef _MSC_VER
#define PACK_STRUCT( __Declaration__ ) __pragma( pack(push, 1) ) __Declaration__ __pragma( pack(pop))
#endif

#ifndef _COMPILER_PACK
#if defined(ICCARM) || defined(__GNUC__) || defined(__ICCARM__)
#define _COMPILER_PACK  __attribute__((packed))
#else 
#define _COMPILER_PACK
#endif
#endif


#define MSB(x) ((x & 0xFF00) >> 8)
#define LSB(x) (x & 0x00FF)

#ifdef __cplusplus
}
#endif

#endif
