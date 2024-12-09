#ifndef _APP_BLE_H
#define _APP_BLE_H

/* STD APIs */
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/* ESP APIs */
#include "esp_log.h"
#include "nvs_flash.h"
#include "sdkconfig.h"
#include "esp_random.h"
/* FreeRTOS APIs */
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

/* NimBLE stack APIs */
#include "host/ble_hs.h"
#include "host/ble_uuid.h"
#include "host/util/util.h"
#include "nimble/ble.h"
#include "nimble/nimble_port.h"
#include "nimble/nimble_port_freertos.h"


/* NimBLE GAP APIs */
#include "host/ble_gap.h"
#include "services/gap/ble_svc_gap.h"

/* NimBLE GATT APIs */
#include "host/ble_gatt.h"
#include "services/gatt/ble_svc_gatt.h"

#include "bsp.h"

/* Defines */
#define BLE_GAP_APPEARANCE_GENERIC_TAG 0x0200
#define BLE_GAP_URI_PREFIX_HTTPS 0x17
#define BLE_GAP_LE_ROLE_PERIPHERAL 0x00

void app_ble_init();
void send_heart_rate_indication(void);
void send_heart_rate_notify(void);

esp_err_t app_ble_send_indicate(uint8_t *data, int len);
#endif