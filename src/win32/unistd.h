/*
 * Copied from StackOverflow:
 *  http://stackoverflow.com/a/826027
 */

#ifndef _UNISTD_H
#define _UNISTD_H    1

#ifdef __cplusplus
extern "C" {
#endif

/* This file intended to serve as a drop-in replacement for
 *  unistd.h on Windows
 *  Please add functionality as neeeded
 */

#include <stdlib.h>
#include <io.h>
#include <getopt.h> /* getopt at: https://gist.github.com/ashelly/7776712*/
#include <process.h> /* for getpid() and the exec..() family */

#define srandom srand
#define random rand

/* Values for the second argument to access.
   These may be OR'd together.  */
#define R_OK    4       /* Test for read permission.  */
#define W_OK    2       /* Test for write permission.  */
//#define   X_OK    1       /* execute permission - unsupported in windows*/
#define F_OK    0       /* Test for existence.  */

#define access _access
#define ftruncate _chsize
#define unlink _unlink

#define ssize_t int

#define STDIN_FILENO 0
#define STDOUT_FILENO 1
#define STDERR_FILENO 2
/* should be in some equivalent to <sys/types.h> */
typedef __int8            int8_t;
typedef __int16           int16_t;
typedef __int32           int32_t;
typedef __int64           int64_t;
typedef unsigned __int8   uint8_t;
typedef unsigned __int16  uint16_t;
typedef unsigned __int32  uint32_t;
typedef unsigned __int64  uint64_t;


#ifdef __cplusplus
}
#endif


#endif /* unistd.h  */
