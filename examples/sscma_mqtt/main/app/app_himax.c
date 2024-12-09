#include "app_himax.h"
#include "app_wifi.h"
// #include "app_mqtt.h"
#include "cJSON.h"
#include "cJSON_Utils.h"
#include "esp_task_wdt.h"
#include "esp_timer.h"
#include "esp_heap_trace.h"

static const char *TAG = "app_himax";
int connect_time = 0;



void on_log(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    if(mqtt_connect == true){
        int msg_id = esp_mqtt_client_publish(mqtt_client, mqtt_tx_topic, reply->data, reply->len, 0, 0);
        if (msg_id < 0)
        {
            // ESP_LOGE(TAG, "Failed to publish: %d", msg_id);
        }
        else
        {
            // ESP_LOGI(TAG, "Publish success: %d", strlen(data));
        }

    }

    char tag[10] = "on_log";

    print_memory_status(tag);
}


void on_response(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{

    if(mqtt_connect == true){
        int msg_id = esp_mqtt_client_publish(mqtt_client, mqtt_tx_topic, reply->data, reply->len, 0, 0);
        if (msg_id < 0)
        {
            // ESP_LOGE(TAG, "Failed to publish: %d", msg_id);
        }
        else
        {
            // ESP_LOGI(TAG, "Publish success: %d", strlen(data));
        }

    }

    char tag[] = "on_response";

    print_memory_status(tag);
}


void on_event(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{

    if(mqtt_connect == true){
        int msg_id = esp_mqtt_client_publish(mqtt_client, mqtt_tx_topic, reply->data, reply->len, 0, 0);
        if (msg_id < 0)
        {
            // ESP_LOGE(TAG, "Failed to publish: %d", msg_id);
        }
        else
        {
            // ESP_LOGI(TAG, "Publish success: %d", strlen(data));
        }

    }

    char tag[] = "on_event";
     print_memory_status(tag);

}



void on_connect(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    connect_time++;
    if(connect_time > 1){
        ESP_LOGW(TAG, "connect_time = %d will reboot",connect_time);
        esp_restart();
    }

    ESP_LOGW(TAG, "on connect");



    sscma_client_info_t *info = NULL;
    if (sscma_client_get_info(client, &info, true) == ESP_OK)
    {
        printf("ID: %s\n", (info->id != NULL) ? info->id : "NULL");
        printf("Name: %s\n", (info->name != NULL) ? info->name : "NULL");
        printf("Hardware Version: %s\n", (info->hw_ver != NULL) ? info->hw_ver : "NULL");
        printf("Software Version: %s\n", (info->sw_ver != NULL) ? info->sw_ver : "NULL");
        printf("Firmware Version: %s\n", (info->fw_ver != NULL) ? info->fw_ver : "NULL");
    }
    else
    {
        printf("get info failed\n");
    }

    sscma_client_model_t *model = NULL;
    if (sscma_client_get_model(client, &model, true) == ESP_OK)
    {
        printf("ID: %d\n", model->id ? model->id : -1);
        printf("UUID: %s\n", model->uuid ? model->uuid : "N/A");
        
        if(model->uuid != NULL){
            printf("atoi(model->uuid) * 1000 = : %d\n", atoi(model->uuid) * 1000);
            printf("1000000000 + atoi(model->uuid) * 1000 + 140 : %d\n", (1000000000 + atoi(model->uuid) * 1000 + 140));
        }else{
            printf("model->uuid == NULL\r\n");
        }

        

        printf("Name: %s\n", model->name ? model->name : "N/A");
        printf("Version: %s\n", model->ver ? model->ver : "N/A");
        printf("URL: %s\n", model->url ? model->url : "N/A");
        printf("Checksum: %s\n", model->checksum ? model->checksum : "N/A");
        printf("Classes:\n");
        if (model->classes[0] != NULL)
        {
            for (int i = 0; model->classes[i] != NULL; i++)
            {
                printf("  - %s\n", model->classes[i]);
            }
        }
        else
        {
            printf("  N/A\n");
        }
    }

    sscma_client_wifi_t wifi;
    memset(&wifi, 0, sizeof(wifi));
    if (get_wifi_config(client, &wifi) == ESP_OK)
    {
        ESP_LOGI(TAG, "SSID: %s", wifi.ssid == NULL ? "NULL" : wifi.ssid);
        ESP_LOGI(TAG, "Password: %s", wifi.password == NULL ? "NULL" : wifi.password);

        nvs_handle_t my_handle;
        esp_err_t ret = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &my_handle);

        if (ret != ESP_OK) {
            ESP_LOGE(TAG, "Error (%s) opening NVS handle!", esp_err_to_name(ret));
            } else {
            ESP_LOGI(TAG, "NVS handle opened successfully");

            const char *ssid = wifi.ssid ? wifi.ssid : "null";
            const char *password = wifi.password ? wifi.password : "null";

            memset(wifi_ssid,0,sizeof(wifi_ssid));
            memcpy(wifi_ssid,ssid,strlen(ssid));
            memset(wifi_passwd,0,sizeof(wifi_passwd));
            memcpy(wifi_passwd,password,strlen(password));

            ret = nvs_set_str(my_handle, NVS_KEY_SSID, ssid);
            ESP_ERROR_CHECK(ret);

            ret = nvs_set_str(my_handle, NVS_KEY_PASSWORD, password);
            ESP_ERROR_CHECK(ret);

            ret = nvs_commit(my_handle);
            ESP_ERROR_CHECK(ret);

            nvs_close(my_handle);
        }
        free(wifi.ssid);
        free(wifi.password);
    }

    sscma_client_mqtt_t mqtt;
    memset(&mqtt, 0, sizeof(mqtt));
    if (get_mqtt_config(client, &mqtt) == ESP_OK)
    {
        ESP_LOGI(TAG, "00Username: %s", mqtt.username == NULL ? "NULL" : mqtt.username);
        ESP_LOGI(TAG, "Password: %s", mqtt.password == NULL ? "NULL" : mqtt.password);
        ESP_LOGI(TAG, "Address: %s", mqtt.address == NULL ? "NULL" : mqtt.address);
        ESP_LOGI(TAG, "Client ID: %s", mqtt.client_id == NULL ? "NULL" : mqtt.client_id);
        ESP_LOGI(TAG, "Port1: %d", mqtt.port1 == 0 ? 0 : mqtt.port1);
        ESP_LOGI(TAG, "use_ssl1: %d", mqtt.use_ssl1);

        nvs_handle_t my_handle;
        esp_err_t ret = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &my_handle);

        if (ret != ESP_OK) {
            ESP_LOGE(TAG, "Error (%s) opening NVS handle!", esp_err_to_name(ret));
        } else {
            ESP_LOGI(TAG, "NVS handle opened successfully");

            const char *username = mqtt.username ? mqtt.username : "null";
            const char *password = mqtt.password ? mqtt.password : "null";
            const char *address = mqtt.address ? mqtt.address : "null";
            const char *client_id = mqtt.client_id ? mqtt.client_id : "null";
            const int *port1 = mqtt.port1 ? mqtt.port1 : "null";
            const int *use_ssl1 = mqtt.use_ssl1;

            memset(mqtt_username,0,sizeof(mqtt_username));
            memset(mqtt_password,0,sizeof(mqtt_password));
            memset(mqtt_address,0,sizeof(mqtt_address));
            memset(mqtt_clientid,0,sizeof(mqtt_clientid));


            memcpy(mqtt_username,username,strlen(username));
            memcpy(mqtt_password,password,strlen(password));
            memcpy(mqtt_address,address,strlen(address));
            memcpy(mqtt_clientid,client_id,strlen(client_id));



            ret = nvs_set_str(my_handle, NVS_KEY_MQTT_USERNAME, username);
            ESP_ERROR_CHECK(ret);
            ESP_LOGI(TAG, "NVS handle opened successfully");
            ret = nvs_set_str(my_handle, NVS_KEY_MQTT_PASSWORD, password);
            ESP_ERROR_CHECK(ret);

            ret = nvs_set_str(my_handle, NVS_KEY_MQTT_ADDRESS, address);
            ESP_ERROR_CHECK(ret);

            ret = nvs_set_i32(my_handle, NVS_KEY_MQTT_PORT1, port1);
            ESP_ERROR_CHECK(ret);

            ret = nvs_set_str(my_handle, NVS_KEY_MQTT_CLIENT_ID, client_id);
            ESP_ERROR_CHECK(ret);

            ret = nvs_set_i32(my_handle, NVS_KEY_MQTT_USE_SSL1, use_ssl1);
            ESP_ERROR_CHECK(ret);

            ret = nvs_commit(my_handle);
            ESP_ERROR_CHECK(ret);

            nvs_close(my_handle);
        }
        // if (read_config == 1)
        // {
        //     //esp_restart();
        // }
        free(mqtt.client_id);
        free(mqtt.address);
        free(mqtt.username);
        free(mqtt.password);
    }
    else
    {
        ESP_LOGE(TAG, "get mqtt config failed\n");
    }

    
    himax_config = true;

}


void himax_status(void *param) {
    
    while (1)
    {
        if(himax_config != true){
            ESP_LOGI("himax_status","himax_config != true ,continue");
            continue;
        }
        vTaskDelay(5000);
       
        sscma_client_wifi_t wifi;
        memset(&wifi, 0, sizeof(wifi));
        xSemaphoreTake(xSemaphore, portMAX_DELAY);
        if (get_wifi_config(client, &wifi) == ESP_OK)
        {

            if( strcmp(wifi_ssid,wifi.ssid) || strcmp(wifi_passwd,wifi.password)){
                ESP_LOGI("himax_status", "SSID: %s", wifi.ssid == NULL ? "NULL" : wifi.ssid);
                ESP_LOGI("himax_status", "Password: %s", wifi.password == NULL ? "NULL" : wifi.password);

                ESP_LOGI("himax_status", "SSID: %s", wifi_ssid);
                ESP_LOGI("himax_status", "Password: %s", wifi_passwd);

                ESP_LOGI("himax_status", "WIFI reboot");
                esp_restart();
            }
            free(wifi.ssid);
            free(wifi.password);
        }
        
        xSemaphoreGive(xSemaphore);

        sscma_client_mqtt_t mqtt;
        memset(&mqtt, 0, sizeof(mqtt));
        xSemaphoreTake(xSemaphore, portMAX_DELAY);
        if (get_mqtt_config(client, &mqtt) == ESP_OK)
        {
            if(strcmp(mqtt_username,mqtt.username) || strcmp(mqtt_password,mqtt.password) || strcmp(mqtt_address,mqtt.address) || strcmp(mqtt_clientid,mqtt.client_id)){
                ESP_LOGI("himax_status", "00Username: %s", mqtt.username == NULL ? "NULL" : mqtt.username);
                ESP_LOGI("himax_status", "Password: %s", mqtt.password == NULL ? "NULL" : mqtt.password);
                ESP_LOGI("himax_status", "Address: %s", mqtt.address == NULL ? "NULL" : mqtt.address);
                ESP_LOGI("himax_status", "Client ID: %s", mqtt.client_id == NULL ? "NULL" : mqtt.client_id);
                ESP_LOGI("himax_status", "Port1: %d", mqtt.port1 == 0 ? 0 : mqtt.port1);
                ESP_LOGI("himax_status", "use_ssl1: %d", mqtt.use_ssl1);
                ESP_LOGI("himax_status", "mqtt reboot");
                esp_restart();
            }
            free(mqtt.client_id);
            free(mqtt.address);
            free(mqtt.username);
            free(mqtt.password);
        }
        xSemaphoreGive(xSemaphore);

        if(wifi_connect != true){
            
            continue;
        }

        esp_netif_t *netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF"); // 获取 WiFi STA 接口
        if (netif == NULL) {
            ESP_LOGE("himax_status", "Failed to get esp_netif handle");
            
            continue;;
        }

        esp_netif_ip_info_t ip_info;
        esp_err_t ret = esp_netif_get_ip_info(netif, &ip_info);
        if (ret != ESP_OK) {
            ESP_LOGE("himax_status", "Failed to get IP information: %s", esp_err_to_name(ret));
            
            continue;;
        }

        char at[30] = "AT+WIFISTA=2\r\n";

        xSemaphoreTake(xSemaphore, portMAX_DELAY);
        sscma_client_write(client, at, strlen(at));
        xSemaphoreGive(xSemaphore);

        char at_command[128];
        // 格式化 AT 指令
        snprintf(at_command, sizeof(at_command), "AT+WIFIIN4=\"" IPSTR "\",\"" IPSTR "\",\"" IPSTR "\"\r\n",
                IP2STR(&ip_info.ip), IP2STR(&ip_info.netmask), IP2STR(&ip_info.gw));

        ESP_LOGW("himax_status","AT Command: %s", at_command);
        sscma_client_reply_t reply;
        xSemaphoreTake(xSemaphore, portMAX_DELAY);
        sscma_client_request(client, at_command, &reply, true, CMD_WAIT_DELAY);
        xSemaphoreGive(xSemaphore);

        if (reply.payload != NULL)
        {
            printf("himax_status reply is %s\r\n",reply.data);

            sscma_client_reply_clear(&reply);
        }

        if(mqtt_connect != true){
            
            continue;
        }

        char at_command2[30] = "AT+MQTTSERVERSTA=2\r\n";
        xSemaphoreTake(xSemaphore, portMAX_DELAY);
        sscma_client_write(client, at_command2, strlen(at_command2));
        xSemaphoreGive(xSemaphore);
    }
    
}



void himax_sscam_init()
{
    uart_config_t uart_config = {
        .baud_rate = 921600,
        .data_bits = UART_DATA_8_BITS,
        .parity = UART_PARITY_DISABLE,
        .stop_bits = UART_STOP_BITS_1,
        .flow_ctrl = UART_HW_FLOWCTRL_DISABLE,
        .source_clk = UART_SCLK_DEFAULT,
    };
    int intr_alloc_flags = 0;

    ESP_ERROR_CHECK(uart_driver_install(UART_NUM_1, 8 * 1024, 0, 0, NULL, intr_alloc_flags));
    ESP_ERROR_CHECK(uart_param_config(UART_NUM_1, &uart_config));
    ESP_ERROR_CHECK(uart_set_pin(UART_NUM_1, 21, 20, -1, -1));

    sscma_client_io_uart_config_t io_uart_config = {
        .user_ctx = NULL,
    };

    sscma_client_config_t sscma_client_config = SSCMA_CLIENT_CONFIG_DEFAULT();
    sscma_client_config.tx_buffer_size = 2 * 1024;
    sscma_client_config.rx_buffer_size = 24 * 1024;
    sscma_client_config.event_queue_size = 1;
    sscma_client_config.monitor_task_stack = 1024 * 10;
    sscma_client_config.process_task_stack = 4096;

    sscma_client_config.reset_gpio_num = 5;

    sscma_client_new_io_uart_bus((sscma_client_uart_bus_handle_t)1, &io_uart_config, &io);

    sscma_client_new(io, &sscma_client_config, &client);

    const sscma_client_callback_t callback = {
        .on_connect = on_connect,
        .on_event = on_event,
        .on_response = on_response,
        .on_log = on_log,
    };

    if(client == NULL){
        printf("client null\r\n");
        return;
    }

    printf("client not null\r\n");

    if (sscma_client_register_callback(client, &callback, NULL) != ESP_OK)
    {
        printf("set callback failed\n");
        abort();
    }

    sscma_client_init(client);

    vTaskDelay(2000);
    
    xTaskCreate(&himax_status, "himax_status", 1024 * 3, NULL, 2,NULL);
    
}
