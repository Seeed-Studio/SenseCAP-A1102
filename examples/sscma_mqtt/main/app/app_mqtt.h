#ifndef _APP_MQTT_H
#define _APP_MQTT_H

#include"bsp.h"


#include "mqtt_client.h"


extern esp_mqtt_client_handle_t mqtt_client;

extern char mqtt_tx_topic[128];
extern char mqtt_rx_topic[128];
extern char mqtt_client_id[64];

void mqtt_initialize();

#endif