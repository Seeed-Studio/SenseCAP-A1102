#ifndef _APP_NVS_H
#define _APP_NVS_H

#include "bsp.h"


#define NVS_KEY_ADDR "modbus_addr"
#define NVS_KEY_BAUD "modbus_baud"
#define NVS_KEY_BLE_SN "BLE_SN"
#define NVS_KEY_DEVICE_SN "SN"

esp_err_t write_modbus_address(uint16_t modbus_address);
esp_err_t write_modbus_baud(uint32_t modbus_baud);
char* get_SN(bool type);
esp_err_t set_sn(char *sn_number);
void app_nvs_init();

#endif