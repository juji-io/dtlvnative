#ifndef DTLV_BYTES_H
#define DTLV_BYTES_H

#include <stdint.h>

static inline uint16_t dtlv_to_be16(uint16_t value) {
  return (uint16_t)(((value & 0x00FFu) << 8) | ((value & 0xFF00u) >> 8));
}

static inline uint16_t dtlv_from_be16(uint16_t value) {
  return dtlv_to_be16(value);
}

static inline uint32_t dtlv_to_be32(uint32_t value) {
  return ((value & 0x000000FFu) << 24) |
         ((value & 0x0000FF00u) << 8) |
         ((value & 0x00FF0000u) >> 8) |
         ((value & 0xFF000000u) >> 24);
}

static inline uint32_t dtlv_from_be32(uint32_t value) {
  return dtlv_to_be32(value);
}

static inline uint64_t dtlv_to_be64(uint64_t value) {
  return ((uint64_t)dtlv_to_be32((uint32_t)(value & 0xFFFFFFFFULL)) << 32) |
         (uint64_t)dtlv_to_be32((uint32_t)(value >> 32));
}

static inline uint64_t dtlv_from_be64(uint64_t value) {
  return dtlv_to_be64(value);
}

#endif /* DTLV_BYTES_H */
