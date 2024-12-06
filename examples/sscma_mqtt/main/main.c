#include "app_himax.h"
#include "app_mqtt.h"
#include "app_nvs.h"
#include "app_wifi.h"
#include "bsp.h"
static const char *TAG = "main";

void app_main(void)
{
    xSemaphore = xSemaphoreCreateBinary();
    xSemaphoreGive(xSemaphore);

    msg_mutex = xSemaphoreCreateMutex();
    
    event_group = xEventGroupCreate();

    if (event_group == NULL) {
        printf("Failed to create event group\n");
        return;
    }

    //vTaskDelay(pdMS_TO_TICKS(5000));
    app_nvs_init();
    himax_sscam_init();

    while (himax_config != true)
    {
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
    
    ESP_LOGI(TAG, "Init network");
    wifi_init_sta();
    mqtt_initialize();

    
    while (1)
    {
        // print_memory_status("main");
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}