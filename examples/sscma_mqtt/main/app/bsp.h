#ifndef _BSP_H
#define _BSP_H

#include "esp_log.h"
#include "nvs_flash.h"
#include "esp_event.h"
#include "cJSON.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include <stdatomic.h>
#include "mbcontroller.h"       // for mbcontroller defines and api

#include "esp_system.h"
#include "esp_heap_caps.h"


#include "sscma_client_io.h"
#include "sscma_client_ops.h"
#include "sscma_client_commands.h"
#include "mqtt_client.h"
#define NVS_NAMESPACE "storage"
#define NVS_KEY_SSID "wifi_ssid"
#define NVS_KEY_PASSWORD "wifi_password"

#define NVS_KEY_MQTT_USERNAME "mqtt_username"
#define NVS_KEY_MQTT_PASSWORD "mqtt_password"
#define NVS_KEY_MQTT_ADDRESS "mqtt_address"
#define NVS_KEY_MQTT_PORT "mqtt_port"
#define NVS_KEY_MQTT_PORT1 "mqtt_port1"
#define NVS_KEY_MQTT_CLIENT_ID "mqtt_client_id"
#define NVS_KEY_MQTT_USE_SSL "mqtt_use_ssl"
#define NVS_KEY_MQTT_USE_SSL1 "mqtt_use_ssl1"

#define MQTT_CONNECTED_BIT BIT2


#pragma once

/** Major version number (X.x.x) */
#define SSCMA_CLIENT_MONITOR_VERSION_MAJOR 0
/** Minor version number (x.X.x) */
#define SSCMA_CLIENT_MONITOR_VERSION_MINOR 0
/** Patch version number (x.x.X) */
#define SSCMA_CLIENT_MONITOR_VERSION_PATCH 1

/**
 * Macro to convert version number into an integer
 *
 * To be used in comparisons, such as SSCMA_CLIENT_MONITOR_VERSION >= SSCMA_CLIENT_MONITOR_VERSION_VAL(4, 0, 0)
 */
#define SSCMA_CLIENT_MONITOR_VERSION_VAL(major, minor, patch) ((major << 16) | (minor << 8) | (patch))

/**
 * Current version, as an integer
 *
 * To be used in comparisons, such as SSCMA_CLIENT_MONITOR_VERSION >= SSCMA_CLIENT_MONITOR_VERSION_VAL(4, 0, 0)
 */
#define SSCMA_CLIENT_MONITOR_VERSION SSCMA_CLIENT_MONITOR_VERSION_VAL(SSCMA_CLIENT_MONITOR_VERSION_MAJOR, SSCMA_CLIENT_MONITOR_VERSION_MINOR, SSCMA_CLIENT_MONITOR_VERSION_PATCH)

// 定义事件标志
#define PRODUCER_1_READY  (1 << 0)
#define PRODUCER_2_READY  (1 << 1)
#define PRODUCER_3_READY  (1 << 2)


extern sscma_client_io_handle_t io;
extern sscma_client_handle_t client;
extern esp_mqtt_client_handle_t mqtt_client;

extern bool himax_config;
extern bool mqtt_connect;
extern bool wifi_connect;

extern SemaphoreHandle_t xSemaphore;
extern SemaphoreHandle_t msg_mutex;

extern EventGroupHandle_t event_group;


extern char wifi_ssid[100];
extern char wifi_passwd[100];

extern char mqtt_username[100];
extern char mqtt_password[100];
extern char mqtt_address[100];
extern char mqtt_clientid[100];

extern char mqtt_tx_topic[128];
extern char mqtt_rx_topic[128];
extern char mqtt_client_id[64];

extern char pub_msg[25 * 1024];

extern void print_memory_status(char *tag);

#endif