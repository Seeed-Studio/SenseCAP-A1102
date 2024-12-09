#ifndef MAIN_H
#define MAIN_H

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "driver/uart.h"
#include "driver/i2c.h"
#include "driver/gpio.h"
#include "driver/spi_master.h"
#include "esp_timer.h"
#include "esp_check.h"
#include "esp_spiffs.h"
#include "esp_system.h"
#include "lwip/err.h"
#include "lwip/sys.h"

#include "mbcontroller.h"

#include "app_nvs.h"
#include "app_modbus.h"

#include "app_himax.h"
#include "app_ble.h"
#include "at_cmd.h"


#define TAG "SenseCAP A1102"


#endif // !MAIN_H