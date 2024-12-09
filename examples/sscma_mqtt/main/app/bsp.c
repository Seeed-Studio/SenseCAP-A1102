#include "bsp.h"

// 创建事件组句柄
EventGroupHandle_t event_group;

sscma_client_io_handle_t io = NULL;
sscma_client_handle_t client = NULL;

esp_mqtt_client_handle_t mqtt_client;

bool himax_config = false;
bool mqtt_connect = false;
bool wifi_connect = false;
SemaphoreHandle_t xSemaphore;
SemaphoreHandle_t msg_mutex;


char wifi_ssid[100];
char wifi_passwd[100];

char mqtt_username[100];
char mqtt_password[100];
char mqtt_address[100];
char mqtt_clientid[100];

char mqtt_tx_topic[128];
char mqtt_rx_topic[128];
char mqtt_client_id[64];

char pub_msg[25 * 1024];


void print_memory_status(char *tag)
{
    size_t free_mem = heap_caps_get_free_size(MALLOC_CAP_8BIT);       // 当前可用的堆内存
    size_t min_free_mem = heap_caps_get_minimum_free_size(MALLOC_CAP_8BIT); // 启动后最小堆内存
    size_t largest_block = heap_caps_get_largest_free_block(MALLOC_CAP_8BIT); // 最大连续空闲块
    

    ESP_LOGI(tag, "Free memory: %d bytes", free_mem);
    ESP_LOGI(tag, "Minimum free memory since startup: %d bytes", min_free_mem);
    ESP_LOGI(tag, "Largest free block: %d bytes", largest_block);
    
}
