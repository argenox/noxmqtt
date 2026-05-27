/*****************************************************************************
* Copyright (c) [2024] Argenox Technologies LLC
* All rights reserved.
*
*
*
* NOTICE:  All information contained herein, source code, binaries and
* derived works is, and remains the property of Argenox and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Argenox and its suppliers and may be covered
* by U.S. and Foreign Patents, patents in process, and are protected by
* trade secret or copyright law.
*
* Licensing of this software can be found in LICENSE
*
* THIS SOFTWARE IS PROVIDED BY ARGENOX "AS IS" AND
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL ARGENOX LLC BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*
* CONTACT: info@argenox.com
*
* File:    commandline.h
* Summary: Command Line Parser Defintions
*
*/

#ifndef _COMMANDLINE_H_
#define _COMMANDLINE_H_

#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

#include "noxmqtt_err.h"

#define DESC_MAX_NAME_LEN 64

typedef void (*term_cmd_handler_t)(char * buf, uint16_t len);

typedef struct
{
    char name[DESC_MAX_NAME_LEN];
    term_cmd_handler_t handler;
} commandline_item_t;



#ifdef __cplusplus
}
#endif

#endif /* _COMMANDLINE_H_ */