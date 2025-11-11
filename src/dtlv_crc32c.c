#include "dtlv_crc32c.h"

static uint32_t dtlv_crc32c_table[256];
static int dtlv_crc32c_ready = 0;

static void dtlv_crc32c_init(void) {
  if (dtlv_crc32c_ready) return;
  const uint32_t poly = 0x82f63b78u;
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t crc = i;
    for (int j = 0; j < 8; ++j) {
      if (crc & 1) {
        crc = (crc >> 1) ^ poly;
      } else {
        crc >>= 1;
      }
    }
    dtlv_crc32c_table[i] = crc;
  }
  dtlv_crc32c_ready = 1;
}

uint32_t dtlv_crc32c(const void *data, size_t length) {
  dtlv_crc32c_init();
  uint32_t crc = 0xFFFFFFFFu;
  const uint8_t *bytes = (const uint8_t *)data;
  for (size_t i = 0; i < length; ++i) {
    uint32_t idx = (crc ^ bytes[i]) & 0xFFu;
    crc = dtlv_crc32c_table[idx] ^ (crc >> 8);
  }
  return ~crc;
}
