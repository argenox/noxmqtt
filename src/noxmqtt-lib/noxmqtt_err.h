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
* File:    noxmqtt_err.h
* Summary: NoxMQTT Error Codes
*
*****************************************************************************/

#ifndef _NOXMQTT_ERR_H_
#define _NOXMQTT_ERR_H_

#ifdef __cplusplus
extern "C" {
#endif

#define ERROR_BASE 0x00000000UL


typedef enum
{
    NOXMQTT_SUCCESS                   = ERROR_BASE,
    NOXMQTT_RC_ERROR                  = ERROR_BASE + 1,
    NOXMQTT_RC_ERROR_INTERNAL         = ERROR_BASE + 2,
    NOXMQTT_RC_ERROR_NOT_INIT         = ERROR_BASE + 3, /* Library object not initialized */
    NOXMQTT_RC_ERROR_BAD_CLIENT_IDENT = ERROR_BASE + 4, /* Device ID not specified specified or length / characters of ID wrong */
    NOXMQTT_RC_ERROR_NULL             = ERROR_BASE + 5,
    NOXMQTT_RC_ERROR_OVERFLOW         = ERROR_BASE + 6,
    NOXMQTT_RC_ERROR_BAD_PACKET       = ERROR_BASE + 7,
    NOXMQTT_RC_ERROR_TRANSPORT        = ERROR_BASE + 8,
    NOXMQTT_RC_ERROR_TIMEOUT          = ERROR_BASE + 9,
    NOXMQTT_RC_ERROR_NOT_SUPPORTED    = ERROR_BASE + 10,
    NOXMQTT_RC_ERROR_NO_MEMORY        = ERROR_BASE + 11,
    NOXMQTT_RC_ERROR_NOT_CONNECTED    = ERROR_BASE + 12,

} noxmqtt_rc_t;



#ifdef __cplusplus
}
#endif

#endif
