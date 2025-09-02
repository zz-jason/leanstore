#ifndef LEANSTORE_COMMON_UTILS_H
#define LEANSTORE_COMMON_UTILS_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// NOLINTBEGIN

/// Start metrics exposer
void lean_metrics_exposer_start(int32_t port);

/// Stop metrics exposer
void lean_metrics_exposer_stop();

/// NOLINTEND

#ifdef __cplusplus
}
#endif

#endif