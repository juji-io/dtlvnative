#ifndef DTLV_CRC32C_H
#define DTLV_CRC32C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint32_t dtlv_crc32c(const void *data, size_t length);

#ifdef __cplusplus
}
#endif

#endif /* DTLV_CRC32C_H */
