#ifndef _BSP_H
#define _BSP_H

#include "esp_log.h"
#include "nvs_flash.h"
#include "esp_event.h"
#include "cJSON.h"
#include <stdatomic.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include <stdatomic.h>
#include "mbcontroller.h"       // for mbcontroller defines and api

#include "esp_system.h"
#include "esp_heap_caps.h"


#include "sscma_client_io.h"
#include "sscma_client_ops.h"
#include "sscma_client_commands.h"

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

ESP_EVENT_DECLARE_BASE(PARAM_CHANGE_EVENT_BASE);

extern portMUX_TYPE param_lock;

extern sscma_client_io_handle_t io;
extern sscma_client_handle_t client;
extern esp_event_loop_handle_t change_param_event_loop;
extern QueueHandle_t ble_msg_queue;
extern SemaphoreHandle_t xSemaphore;
extern SemaphoreHandle_t img_memory_lock;
extern atomic_int img_memory_lock_owner;
extern int img_get_cnt;
extern bool model_change_flag;

typedef enum{
   Prefab_Models = 1,
   Human_Body_Detection, 
   Meter_Identification, 
   People_Counting
}modle_type;

typedef enum{
  Baud4800 = 0,
  Baud9600 = 1,
  Baud14400 = 2,
  Baud19200 = 3,
  Baud38400 = 4,
  Baud57600 = 5,
  Baud115200 = 6,
}Baud_rate_type;


typedef struct
{
   uint16_t  modbus_address;
   uint32_t modbus_baud;
}modbus_parm;

typedef struct 
{
   modle_type current_modle;
   char *model_name;
   char *classes[SSCMA_CLIENT_MODEL_MAX_CLASSES];
   int confidence[SSCMA_CLIENT_MODEL_MAX_CLASSES];
   int classes_count;
   bool save_pic_flage;
}modle_param;


typedef struct 
{
   modbus_parm modbus_p;
   modle_param modle_p;
}A1102_param;


extern A1102_param g_a1102_param;

typedef struct {
    uint8_t *msg;
    int size;
} ble_msg_t;



typedef enum {
   PARAM_CHANGE_MODBUS,
   PARAM_CHANGE_BAUD_RATE,      
   PARAM_CHANGE_SLAVE_ID,       
   PARAM_CHANGE_CONFIDENCE,     
   PARAM_CHANGE_MODEL_TYPE,
   PARAM_PREVIEW,
   PARAM_RESET,
} param_change_event_id_t;



typedef struct {
    char class_name[50];  
    int confidence;       
} ClassInfo;


typedef struct {
    int class_count;      
    ClassInfo *classes;   
} ClassData;


typedef struct {
   int param_id;             
   int new_baud_rate;      
   int new_slave_id;       
   int new_model_type;     
   ClassData class_data;
   bool save_pic_flage;
} param_change_event_data_t;



const char* model_to_string(modle_type model);

void bsp_init(void);
#endif