#ifndef _APP_WIFI_H
#define _APP_WIFI_H

#include"bsp.h"

#include <net/if.h>
#include "esp_wifi.h"
#include "esp_netif.h"

#define WIFI_CONNECTED_BIT BIT0
#define WIFI_FAIL_BIT      BIT1

void wifi_init_sta();


#endif

