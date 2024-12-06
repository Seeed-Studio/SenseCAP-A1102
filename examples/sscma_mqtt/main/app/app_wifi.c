#include "app_wifi.h"

static const char *TAG = "app_wifi";

EventGroupHandle_t s_wifi_event_group;
static int s_retry_num = 0;

static void event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START)
    {
        esp_wifi_connect();
    }
    else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED)
    {
        if (s_retry_num < CONFIG_ESP_MAXIMUM_RETRY)
        {
            esp_wifi_connect();
            s_retry_num++;
            ESP_LOGI(TAG, "retry to connect to the AP");
        }
        else
        {
            wifi_connect = false;
            xEventGroupSetBits(s_wifi_event_group, WIFI_FAIL_BIT);
           
        }
        ESP_LOGI(TAG, "connect to the AP fail");
    }
    else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP)
    {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "got ip:" IPSTR, IP2STR(&event->ip_info.ip));
        s_retry_num = 0;
        wifi_connect = true;
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);

    }
}


esp_err_t read_wifi_config(sscma_client_wifi_t *wifi)
{
    nvs_handle_t my_handle;
    esp_err_t ret = nvs_open(NVS_NAMESPACE, NVS_READONLY, &my_handle);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "Error (%s) opening NVS handle!", esp_err_to_name(ret));
        wifi->ssid = "testssid";
        wifi->password = "testpassword";
        // read_config = 1;
        return ret;
    }

    size_t required_size;
    ret = nvs_get_str(my_handle, NVS_KEY_SSID, NULL, &required_size);
    if (ret == ESP_OK) {
        wifi->ssid = malloc(required_size);
        ret = nvs_get_str(my_handle, NVS_KEY_SSID, wifi->ssid, &required_size);
    } else {
        wifi->ssid = NULL;
    }

    ret = nvs_get_str(my_handle, NVS_KEY_PASSWORD, NULL, &required_size);
    if (ret == ESP_OK) {
        wifi->password = malloc(required_size);
        ret = nvs_get_str(my_handle, NVS_KEY_PASSWORD, wifi->password, &required_size);
    } else {
        wifi->password = NULL;
    }

    nvs_close(my_handle);
    return ESP_OK;
}





void wifi_init_sta()
{
    s_wifi_event_group = xEventGroupCreate();
    

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_t *netif = esp_netif_create_default_wifi_sta();
    assert(netif);

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &event_handler, NULL));

    sscma_client_wifi_t wifi;
    memset(&wifi, 0, sizeof(wifi));
    if (read_wifi_config(&wifi) == ESP_OK)
    {
        ESP_LOGI(TAG, "readSSID: %s", wifi.ssid == NULL ? "NULL" : wifi.ssid);
        ESP_LOGI(TAG, "Password: %s", wifi.password == NULL ? "NULL" : wifi.password);
    }

    wifi_config_t wifi_config = {
        .sta = {
            .ssid = "",
            .password = ""
        },
    };
    strncpy((char *)wifi_config.sta.ssid, wifi.ssid, sizeof(wifi_config.sta.ssid) - 1);
    strncpy((char *)wifi_config.sta.password, wifi.password, sizeof(wifi_config.sta.password) - 1);

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(ESP_IF_WIFI_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_ERROR_CHECK(esp_netif_set_hostname(netif, "A1102"));//tcpip name is A1102

    ESP_LOGI(TAG, "wifi_init_sta finished.");

    EventBits_t bits = xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT | WIFI_FAIL_BIT, pdFALSE, pdFALSE, portMAX_DELAY);

    if (bits & WIFI_CONNECTED_BIT)
    {
        ESP_LOGI(TAG, "connected to ap SSID: %s", wifi.ssid);

        esp_netif_t *netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF"); // 获取 WiFi STA 接口
        if (netif == NULL) {
            ESP_LOGE("NETWORK", "Failed to get esp_netif handle");
            return;
        }

        esp_netif_ip_info_t ip_info;
        esp_err_t ret = esp_netif_get_ip_info(netif, &ip_info);
        if (ret != ESP_OK) {
            ESP_LOGE("NETWORK", "Failed to get IP information: %s", esp_err_to_name(ret));
            return;
        }

        char at[30] = "AT+WIFISTA=2\r\n";
        
        sscma_client_write(client, at, strlen(at));

        char at_command[128];
        // 格式化 AT 指令
        snprintf(at_command, sizeof(at_command), "AT+WIFIIN4=\"" IPSTR "\",\"" IPSTR "\",\"" IPSTR "\"\r\n",
                IP2STR(&ip_info.ip), IP2STR(&ip_info.netmask), IP2STR(&ip_info.gw));

        ESP_LOGW(TAG,"AT Command: %s", at_command);
        sscma_client_reply_t reply;
        sscma_client_request(client, at_command, &reply, true, CMD_WAIT_DELAY);

        if (reply.payload != NULL)
        {
            printf("reply is %s\r\n",reply.data);

            sscma_client_reply_clear(&reply);
        }
        

    }
    else if (bits & WIFI_FAIL_BIT)
    {
        ESP_LOGI(TAG, "Failed to connect to SSID: %s", wifi.ssid);
        // read_config = 1;
    }
    else
    {
        ESP_LOGE(TAG, "UNEXPECTED EVENT");
    }
    vEventGroupDelete(s_wifi_event_group);
}