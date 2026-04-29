#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "driver/uart.h"
#include "driver/gpio.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"

#include "esp_log.h"
#include "esp_check.h"
#include "esp_system.h"
#include "esp_event.h"
#include "esp_wifi.h"
#include "esp_netif.h"
#include "esp_netif.h"
#include "nvs_flash.h"

#include "mqtt_client.h"
#include "cJSON.h"

#include "sscma_client_io.h"
#include "sscma_client_ops.h"
#include "sscma_client_commands.h"

#define TAG "mqtt2uart"

/* Version sent to SSCMA chip via AT+WIFIVER */
#define WIFI_VERSION_STR "xiao_esp32c3:3.0.0"

/* NVS keys */
#define NVS_NAMESPACE          "mqtt2uart"
#define NVS_KEY_WIFI_SSID      "wifi_ssid"
#define NVS_KEY_WIFI_PASSWORD  "wifi_passwd"
#define NVS_KEY_MQTT_ADDRESS   "mqtt_addr"
#define NVS_KEY_MQTT_PORT      "mqtt_port"
#define NVS_KEY_MQTT_CLIENT_ID "mqtt_cid"
#define NVS_KEY_MQTT_USERNAME  "mqtt_user"
#define NVS_KEY_MQTT_PASSWORD  "mqtt_pass"
#define NVS_KEY_MQTT_USE_SSL   "mqtt_ssl"

/* Event bits */
#define SSCMA_CONNECTED_BIT  BIT0
#define WIFI_CONNECTED_BIT   BIT1
#define MQTT_CONNECTED_BIT   BIT3

/* Max retries */
#define WIFI_MAX_RETRIES      100
#define AT_CMD_MAX_RETRIES    10

/* UART config for SSCMA chip */
#define SSCMA_UART_NUM        UART_NUM_1
#define SSCMA_UART_TX_PIN     GPIO_NUM_21
#define SSCMA_UART_RX_PIN     GPIO_NUM_20
#define SSCMA_UART_BAUD       921600
#define SSCMA_UART_RX_BUF     (32 * 1024)
#define SSCMA_RESET_GPIO      GPIO_NUM_5

#define STR_BUF_SIZE   128
#define TOPIC_BUF_SIZE 160

/* ---------- Global state ---------- */

static sscma_client_io_handle_t s_io = NULL;
static sscma_client_handle_t s_client = NULL;
static esp_mqtt_client_handle_t s_mqtt_client = NULL;

static EventGroupHandle_t s_event_group;
static bool s_wifi_inited = false;
static bool s_netif_inited = false;

/* Configs stored as fixed buffers */
static char s_wifi_ssid[STR_BUF_SIZE] = {0};
static char s_wifi_password[STR_BUF_SIZE] = {0};
static char s_mqtt_address[STR_BUF_SIZE] = {0};
static char s_mqtt_client_id[STR_BUF_SIZE] = {0};
static char s_mqtt_username[STR_BUF_SIZE] = {0};
static char s_mqtt_password[STR_BUF_SIZE] = {0};
static int  s_mqtt_port = 0;
static int  s_mqtt_use_ssl = 0;

static char s_mqtt_tx_topic[TOPIC_BUF_SIZE];
static char s_mqtt_rx_topic[TOPIC_BUF_SIZE];

/* Forward declarations */
static void wifi_reconnect(void);
static void mqtt_reconnect(void);

/* ========== NVS helpers ========== */

static void nvs_save_wifi(const char *ssid, const char *password)
{
    nvs_handle_t h;
    if (nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h) != ESP_OK) return;
    nvs_set_str(h, NVS_KEY_WIFI_SSID, ssid ? ssid : "");
    nvs_set_str(h, NVS_KEY_WIFI_PASSWORD, password ? password : "");
    nvs_commit(h);
    nvs_close(h);
}

static void nvs_save_mqtt(const char *address, int port, const char *client_id,
                           const char *username, const char *password, int use_ssl)
{
    nvs_handle_t h;
    if (nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h) != ESP_OK) return;
    nvs_set_str(h, NVS_KEY_MQTT_ADDRESS, address ? address : "");
    nvs_set_i32(h, NVS_KEY_MQTT_PORT, port);
    nvs_set_str(h, NVS_KEY_MQTT_CLIENT_ID, client_id ? client_id : "");
    nvs_set_str(h, NVS_KEY_MQTT_USERNAME, username ? username : "");
    nvs_set_str(h, NVS_KEY_MQTT_PASSWORD, password ? password : "");
    nvs_set_i32(h, NVS_KEY_MQTT_USE_SSL, use_ssl);
    nvs_commit(h);
    nvs_close(h);
}

static bool nvs_load_wifi(void)
{
    nvs_handle_t h;
    if (nvs_open(NVS_NAMESPACE, NVS_READONLY, &h) != ESP_OK) return false;
    size_t len = STR_BUF_SIZE;
    if (nvs_get_str(h, NVS_KEY_WIFI_SSID, s_wifi_ssid, &len) != ESP_OK || strlen(s_wifi_ssid) == 0) {
        nvs_close(h);
        return false;
    }
    len = STR_BUF_SIZE;
    nvs_get_str(h, NVS_KEY_WIFI_PASSWORD, s_wifi_password, &len);
    nvs_close(h);
    ESP_LOGI(TAG, "NVS: WiFi SSID=%s", s_wifi_ssid);
    return true;
}

static bool nvs_load_mqtt(void)
{
    nvs_handle_t h;
    if (nvs_open(NVS_NAMESPACE, NVS_READONLY, &h) != ESP_OK) return false;
    size_t len = STR_BUF_SIZE;
    if (nvs_get_str(h, NVS_KEY_MQTT_ADDRESS, s_mqtt_address, &len) != ESP_OK || strlen(s_mqtt_address) == 0) {
        nvs_close(h);
        return false;
    }
    len = STR_BUF_SIZE;
    nvs_get_str(h, NVS_KEY_MQTT_CLIENT_ID, s_mqtt_client_id, &len);
    len = STR_BUF_SIZE;
    nvs_get_str(h, NVS_KEY_MQTT_USERNAME, s_mqtt_username, &len);
    len = STR_BUF_SIZE;
    nvs_get_str(h, NVS_KEY_MQTT_PASSWORD, s_mqtt_password, &len);
    int32_t val;
    nvs_get_i32(h, NVS_KEY_MQTT_PORT, &val); s_mqtt_port = val;
    nvs_get_i32(h, NVS_KEY_MQTT_USE_SSL, &val); s_mqtt_use_ssl = val;
    nvs_close(h);
    ESP_LOGI(TAG, "NVS: MQTT %s:%d", s_mqtt_address, s_mqtt_port);
    return true;
}

/* Copy SSCMA config into globals + save NVS */
static void apply_wifi_config(const sscma_client_wifi_t *wifi)
{
    memset(s_wifi_ssid, 0, sizeof(s_wifi_ssid));
    memset(s_wifi_password, 0, sizeof(s_wifi_password));
    if (wifi->ssid) strncpy(s_wifi_ssid, wifi->ssid, sizeof(s_wifi_ssid) - 1);
    if (wifi->password) strncpy(s_wifi_password, wifi->password, sizeof(s_wifi_password) - 1);
    nvs_save_wifi(s_wifi_ssid, s_wifi_password);
}

static void apply_mqtt_config(const sscma_client_mqtt_t *mqtt)
{
    memset(s_mqtt_address, 0, sizeof(s_mqtt_address));
    memset(s_mqtt_client_id, 0, sizeof(s_mqtt_client_id));
    memset(s_mqtt_username, 0, sizeof(s_mqtt_username));
    memset(s_mqtt_password, 0, sizeof(s_mqtt_password));
    if (mqtt->address) strncpy(s_mqtt_address, mqtt->address, sizeof(s_mqtt_address) - 1);
    if (mqtt->client_id) strncpy(s_mqtt_client_id, mqtt->client_id, sizeof(s_mqtt_client_id) - 1);
    if (mqtt->username) strncpy(s_mqtt_username, mqtt->username, sizeof(s_mqtt_username) - 1);
    if (mqtt->password) strncpy(s_mqtt_password, mqtt->password, sizeof(s_mqtt_password) - 1);
    s_mqtt_port = mqtt->port1;
    s_mqtt_use_ssl = mqtt->use_ssl1;
    nvs_save_mqtt(s_mqtt_address, s_mqtt_port, s_mqtt_client_id,
                  s_mqtt_username, s_mqtt_password, s_mqtt_use_ssl);
}

/* ========== SSCMA callbacks ========== */

static void publish_to_mqtt(const sscma_client_reply_t *reply)
{
    EventBits_t bits = xEventGroupGetBits(s_event_group);
    if (!(bits & MQTT_CONNECTED_BIT) || s_mqtt_client == NULL) return;

    esp_mqtt_client_publish(s_mqtt_client, s_mqtt_tx_topic, reply->data, reply->len, 0, 0);
}

static void on_connect(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    ESP_LOGI(TAG, "SSCMA connected");
    EventBits_t bits = xEventGroupGetBits(s_event_group);
    bool first_connect = !(bits & SSCMA_CONNECTED_BIT);

    /* Send version info (fire-and-forget) */
    sscma_client_write(client, "AT+WIFIVER=\"" WIFI_VERSION_STR "\"\r\n", strlen("AT+WIFIVER=\"" WIFI_VERSION_STR "\"\r\n"));
    vTaskDelay(pdMS_TO_TICKS(50));

    /* Set initial status to disconnected (fire-and-forget) */
    sscma_client_write(client, "AT+WIFISTA=0\r\n", strlen("AT+WIFISTA=0\r\n"));
    vTaskDelay(pdMS_TO_TICKS(50));
    sscma_client_write(client, "AT+MQTTSERVERSTA=0\r\n", strlen("AT+MQTTSERVERSTA=0\r\n"));
    vTaskDelay(pdMS_TO_TICKS(50));

    /* Get WiFi config */
    sscma_client_wifi_t wifi;
    memset(&wifi, 0, sizeof(wifi));
    for (int i = 0; i < AT_CMD_MAX_RETRIES; i++) {
        if (get_wifi_config(client, &wifi) == ESP_OK) break;
        vTaskDelay(pdMS_TO_TICKS(10));
        if (i == AT_CMD_MAX_RETRIES - 1) { ESP_LOGE(TAG, "Get WiFi config failed"); esp_restart(); }
    }
    ESP_LOGI(TAG, "WiFi SSID: %s", wifi.ssid ? wifi.ssid : "NULL");

    /* Get MQTT config */
    sscma_client_mqtt_t mqtt;
    memset(&mqtt, 0, sizeof(mqtt));
    for (int i = 0; i < AT_CMD_MAX_RETRIES; i++) {
        if (get_mqtt_config(client, &mqtt) == ESP_OK) break;
        vTaskDelay(pdMS_TO_TICKS(10));
        if (i == AT_CMD_MAX_RETRIES - 1) { ESP_LOGE(TAG, "Get MQTT config failed"); esp_restart(); }
    }
    ESP_LOGI(TAG, "MQTT %s:%d client=%s", mqtt.address ? mqtt.address : "NULL", mqtt.port1,
             mqtt.client_id ? mqtt.client_id : "NULL");

    /* Check if config changed vs current running config */
    bool wifi_changed = false, mqtt_changed = false;
    if (!first_connect) {
        /* Compare with current globals (not NVS) */
        if ((strlen(s_wifi_ssid) == 0) != (wifi.ssid == NULL)) wifi_changed = true;
        if (wifi.ssid && strcmp(s_wifi_ssid, wifi.ssid) != 0) wifi_changed = true;
        if ((strlen(s_wifi_password) == 0) != (wifi.password == NULL)) wifi_changed = true;
        if (wifi.password && strcmp(s_wifi_password, wifi.password) != 0) wifi_changed = true;

        if ((strlen(s_mqtt_address) == 0) != (mqtt.address == NULL)) mqtt_changed = true;
        if (mqtt.address && strcmp(s_mqtt_address, mqtt.address) != 0) mqtt_changed = true;
        if ((strlen(s_mqtt_client_id) == 0) != (mqtt.client_id == NULL)) mqtt_changed = true;
        if (mqtt.client_id && strcmp(s_mqtt_client_id, mqtt.client_id) != 0) mqtt_changed = true;
        if ((strlen(s_mqtt_username) == 0) != (mqtt.username == NULL)) mqtt_changed = true;
        if (mqtt.username && strcmp(s_mqtt_username, mqtt.username) != 0) mqtt_changed = true;
        if ((strlen(s_mqtt_password) == 0) != (mqtt.password == NULL)) mqtt_changed = true;
        if (mqtt.password && strcmp(s_mqtt_password, mqtt.password) != 0) mqtt_changed = true;
        if (s_mqtt_port != mqtt.port1) mqtt_changed = true;
        if (s_mqtt_use_ssl != mqtt.use_ssl1) mqtt_changed = true;
    }

    /* Apply new configs */
    apply_wifi_config(&wifi);
    free(wifi.ssid);
    free(wifi.password);
    apply_mqtt_config(&mqtt);
    free(mqtt.client_id);
    free(mqtt.address);
    free(mqtt.username);
    free(mqtt.password);

    xEventGroupSetBits(s_event_group, SSCMA_CONNECTED_BIT);

    /* First connect: app_main will handle WiFi/MQTT startup */
    if (first_connect) return;

    /* SSCMA restarted — restore status if already connected */
    if (bits & WIFI_CONNECTED_BIT) {
        ESP_LOGI(TAG, "Restoring WiFi status to connected");
        sscma_client_write(client, "AT+WIFISTA=2\r\n", strlen("AT+WIFISTA=2\r\n"));
        vTaskDelay(pdMS_TO_TICKS(50));
        /* Re-send IP info — read from netif since we don't cache it */
        esp_netif_t *netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
        if (netif) {
            esp_netif_ip_info_t ip_info;
            if (esp_netif_get_ip_info(netif, &ip_info) == ESP_OK) {
                char at_cmd[128];
                snprintf(at_cmd, sizeof(at_cmd),
                         "AT+WIFIIN4=\"" IPSTR "\",\"" IPSTR "\",\"" IPSTR "\"\r\n",
                         IP2STR(&ip_info.ip), IP2STR(&ip_info.netmask), IP2STR(&ip_info.gw));
                sscma_client_write(client, at_cmd, strlen(at_cmd));
                vTaskDelay(pdMS_TO_TICKS(50));
            }
        }
    }
    if (bits & MQTT_CONNECTED_BIT) {
        ESP_LOGI(TAG, "Restoring MQTT status to connected");
        sscma_client_write(client, "AT+MQTTSERVERSTA=2\r\n", strlen("AT+MQTTSERVERSTA=2\r\n"));
        vTaskDelay(pdMS_TO_TICKS(50));
    }

    /* If config changed, trigger reconnection */
    if (wifi_changed) {
        ESP_LOGW(TAG, "WiFi config changed after SSCMA restart, reconnecting...");
        if (s_mqtt_client != NULL) {
            esp_mqtt_client_stop(s_mqtt_client);
            esp_mqtt_client_destroy(s_mqtt_client);
            s_mqtt_client = NULL;
            xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
        }
        wifi_reconnect();
        EventBits_t wbits = xEventGroupWaitBits(s_event_group, WIFI_CONNECTED_BIT,
                                                 pdFALSE, pdFALSE, pdMS_TO_TICKS(15000));
        if (wbits & WIFI_CONNECTED_BIT) mqtt_reconnect();
    } else if (mqtt_changed) {
        ESP_LOGW(TAG, "MQTT config changed after SSCMA restart, reconnecting...");
        mqtt_reconnect();
    }
}

/* Event-driven config change: SSCMA pushes WIFI/MQTT events when config changes */
static void on_event(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    publish_to_mqtt(reply);

    if (reply->payload == NULL) return;

    cJSON *name = cJSON_GetObjectItem(reply->payload, "name");
    if (name == NULL || !cJSON_IsString(name)) return;

    if (strnstr(name->valuestring, EVENT_WIFI, strlen(name->valuestring)) != NULL) {
        ESP_LOGW(TAG, "WiFi config event from SSCMA");
        sscma_client_wifi_t wifi;
        memset(&wifi, 0, sizeof(wifi));
        if (get_wifi_config(client, &wifi) != ESP_OK) {
            ESP_LOGE(TAG, "Failed to re-read WiFi config");
            return;
        }
        ESP_LOGI(TAG, "New WiFi SSID: %s", wifi.ssid ? wifi.ssid : "NULL");
        apply_wifi_config(&wifi);
        free(wifi.ssid);
        free(wifi.password);

        if (s_mqtt_client != NULL) {
            esp_mqtt_client_stop(s_mqtt_client);
            esp_mqtt_client_destroy(s_mqtt_client);
            s_mqtt_client = NULL;
            xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
        }
        wifi_reconnect();
        EventBits_t bits = xEventGroupWaitBits(s_event_group, WIFI_CONNECTED_BIT,
                                                pdFALSE, pdFALSE, pdMS_TO_TICKS(15000));
        if (bits & WIFI_CONNECTED_BIT) {
            mqtt_reconnect();
        }
    }
    else if (strnstr(name->valuestring, EVENT_MQTT, strlen(name->valuestring)) != NULL) {
        ESP_LOGW(TAG, "MQTT config event from SSCMA");
        sscma_client_mqtt_t mqtt;
        memset(&mqtt, 0, sizeof(mqtt));
        if (get_mqtt_config(client, &mqtt) != ESP_OK) {
            ESP_LOGE(TAG, "Failed to re-read MQTT config");
            return;
        }
        ESP_LOGI(TAG, "New MQTT: %s:%d", mqtt.address ? mqtt.address : "NULL", mqtt.port1);
        apply_mqtt_config(&mqtt);
        free(mqtt.client_id);
        free(mqtt.address);
        free(mqtt.username);
        free(mqtt.password);
        mqtt_reconnect();
    }
}

static void on_response(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    publish_to_mqtt(reply);
}

static void on_log(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    publish_to_mqtt(reply);
}

/* ========== WiFi ========== */

static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    static int s_retry_num = 0;

    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        xEventGroupClearBits(s_event_group, WIFI_CONNECTED_BIT);
        if (s_retry_num < WIFI_MAX_RETRIES) {
            esp_wifi_connect();
            s_retry_num++;
        } else {
            ESP_LOGE(TAG, "WiFi connect timeout, restarting...");
            esp_restart();
        }
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "Got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        s_retry_num = 0;
        xEventGroupSetBits(s_event_group, WIFI_CONNECTED_BIT);

        if (s_client) {
            /* Update WiFi status and IP info (fire-and-forget) */
            sscma_client_write(s_client, "AT+WIFISTA=2\r\n", strlen("AT+WIFISTA=2\r\n"));
            vTaskDelay(pdMS_TO_TICKS(50));
            char at_cmd[128];
            snprintf(at_cmd, sizeof(at_cmd),
                     "AT+WIFIIN4=\"" IPSTR "\",\"" IPSTR "\",\"" IPSTR "\"\r\n",
                     IP2STR(&event->ip_info.ip), IP2STR(&event->ip_info.netmask),
                     IP2STR(&event->ip_info.gw));
            sscma_client_write(s_client, at_cmd, strlen(at_cmd));
        }
    }
}

static void wifi_start(void)
{
    if (!s_netif_inited) {
        ESP_ERROR_CHECK(esp_netif_init());
        ESP_ERROR_CHECK(esp_event_loop_create_default());
        esp_netif_create_default_wifi_sta();
        s_netif_inited = true;
    }

    if (!s_wifi_inited) {
        wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
        ESP_ERROR_CHECK(esp_wifi_init(&cfg));
        ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
        ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));
        s_wifi_inited = true;
    }

    wifi_config_t wifi_config = { 0 };
    strncpy((char *)wifi_config.sta.ssid, s_wifi_ssid, sizeof(wifi_config.sta.ssid) - 1);
    strncpy((char *)wifi_config.sta.password, s_wifi_password, sizeof(wifi_config.sta.password) - 1);

    ESP_LOGI(TAG, "WiFi SSID: %s", s_wifi_ssid);
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(ESP_IF_WIFI_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
}

static void wifi_reconnect(void)
{
    ESP_LOGW(TAG, "WiFi reconnecting...");
    xEventGroupClearBits(s_event_group, WIFI_CONNECTED_BIT);
    esp_wifi_disconnect();
    esp_wifi_stop();

    wifi_config_t wifi_config = { 0 };
    strncpy((char *)wifi_config.sta.ssid, s_wifi_ssid, sizeof(wifi_config.sta.ssid) - 1);
    strncpy((char *)wifi_config.sta.password, s_wifi_password, sizeof(wifi_config.sta.password) - 1);
    ESP_ERROR_CHECK(esp_wifi_set_config(ESP_IF_WIFI_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
}

/* ========== MQTT ========== */

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = event_data;

    switch (event->event_id) {
        case MQTT_EVENT_CONNECTED:
            ESP_LOGI(TAG, "MQTT connected");
            xEventGroupSetBits(s_event_group, MQTT_CONNECTED_BIT);
            {
                char discovery[256];
                snprintf(discovery, sizeof(discovery), "{\"client_id\":\"%s\"}", s_mqtt_client_id);
                esp_mqtt_client_publish(s_mqtt_client, "sscma/v0/discovery", discovery, 0, 0, 0);
            }
            esp_mqtt_client_subscribe(s_mqtt_client, s_mqtt_rx_topic, 0);
            if (s_client) {
                sscma_client_write(s_client, "AT+MQTTSERVERSTA=2\r\n", strlen("AT+MQTTSERVERSTA=2\r\n"));
            }
            break;

        case MQTT_EVENT_DISCONNECTED:
            ESP_LOGW(TAG, "MQTT disconnected");
            xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
            if (s_client) {
                sscma_client_write(s_client, "AT+MQTTSERVERSTA=0\r\n", strlen("AT+MQTTSERVERSTA=0\r\n"));
            }
            break;

        case MQTT_EVENT_DATA:
            sscma_client_write(s_client, event->data, event->data_len);
            break;

        case MQTT_EVENT_ERROR:
            ESP_LOGW(TAG, "MQTT error");
            break;

        default:
            break;
    }
}

static void mqtt_start(void)
{
    char mqtt_uri[256] = {0};
    int port = s_mqtt_port > 0 ? s_mqtt_port : 1883;

    if (strstr(s_mqtt_address, "://") != NULL) {
        snprintf(mqtt_uri, sizeof(mqtt_uri), "%s", s_mqtt_address);
    } else {
        snprintf(mqtt_uri, sizeof(mqtt_uri), "mqtt://%s", s_mqtt_address);
    }

    snprintf(s_mqtt_tx_topic, sizeof(s_mqtt_tx_topic), "sscma/v0/%s/tx", s_mqtt_client_id);
    snprintf(s_mqtt_rx_topic, sizeof(s_mqtt_rx_topic), "sscma/v0/%s/rx", s_mqtt_client_id);

    ESP_LOGI(TAG, "MQTT %s:%d", mqtt_uri, port);

    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.uri = mqtt_uri,
        .broker.address.port = port,
        .credentials.client_id = s_mqtt_client_id,
        .credentials.username = s_mqtt_username,
        .credentials.authentication.password = s_mqtt_password,
        .buffer.size = 32 * 1024,
    };

    s_mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    if (s_mqtt_client == NULL) {
        ESP_LOGE(TAG, "Failed to create MQTT client");
        return;
    }
    esp_mqtt_client_register_event(s_mqtt_client, MQTT_EVENT_ANY, mqtt_event_handler, NULL);
    esp_mqtt_client_start(s_mqtt_client);
}

static void mqtt_reconnect(void)
{
    ESP_LOGW(TAG, "MQTT reconnecting...");
    xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);

    if (s_mqtt_client != NULL) {
        esp_mqtt_client_stop(s_mqtt_client);
        esp_mqtt_client_destroy(s_mqtt_client);
        s_mqtt_client = NULL;
    }
    if (s_client) {
        sscma_client_write(s_client, "AT+MQTTSERVERSTA=0\r\n", strlen("AT+MQTTSERVERSTA=0\r\n"));
    }
    mqtt_start();
}

/* ========== SSCMA init ========== */

static void sscma_setup(void)
{
    uart_config_t uart_config = {
        .baud_rate = SSCMA_UART_BAUD,
        .data_bits = UART_DATA_8_BITS,
        .parity = UART_PARITY_DISABLE,
        .stop_bits = UART_STOP_BITS_1,
        .flow_ctrl = UART_HW_FLOWCTRL_DISABLE,
        .source_clk = UART_SCLK_DEFAULT,
    };

    int intr_alloc_flags = 0;
#if CONFIG_UART_ISR_IN_IRAM
    intr_alloc_flags = ESP_INTR_FLAG_IRAM;
#endif

    ESP_ERROR_CHECK(uart_driver_install(SSCMA_UART_NUM, SSCMA_UART_RX_BUF, 0, 0, NULL, intr_alloc_flags));
    ESP_ERROR_CHECK(uart_param_config(SSCMA_UART_NUM, &uart_config));
    ESP_ERROR_CHECK(uart_set_pin(SSCMA_UART_NUM, SSCMA_UART_TX_PIN, SSCMA_UART_RX_PIN, -1, -1));

    sscma_client_io_uart_config_t io_uart_config = { .user_ctx = NULL };

    sscma_client_config_t sscma_config = SSCMA_CLIENT_CONFIG_DEFAULT();
    sscma_config.reset_gpio_num = SSCMA_RESET_GPIO;

    ESP_ERROR_CHECK(sscma_client_new_io_uart_bus((sscma_client_uart_bus_handle_t)SSCMA_UART_NUM, &io_uart_config, &s_io));
    ESP_ERROR_CHECK(sscma_client_new(s_io, &sscma_config, &s_client));

    const sscma_client_callback_t callback = {
        .on_connect = on_connect,
        .on_event = on_event,
        .on_response = on_response,
        .on_log = on_log,
    };

    ESP_ERROR_CHECK(sscma_client_register_callback(s_client, &callback, NULL));
    ESP_ERROR_CHECK(sscma_client_init(s_client));

    ESP_LOGI(TAG, "SSCMA client initialized");
}

/* ========== Config check (fallback polling, 30s) ========== */

#define CONFIG_CHECK_INTERVAL_MS 30000

static bool wifi_config_changed(const sscma_client_wifi_t *wifi)
{
    if ((strlen(s_wifi_ssid) == 0) != (wifi->ssid == NULL)) return true;
    if (wifi->ssid && strcmp(s_wifi_ssid, wifi->ssid) != 0) return true;
    if ((strlen(s_wifi_password) == 0) != (wifi->password == NULL)) return true;
    if (wifi->password && strcmp(s_wifi_password, wifi->password) != 0) return true;
    return false;
}

static bool mqtt_config_changed(const sscma_client_mqtt_t *mqtt)
{
    if ((strlen(s_mqtt_client_id) == 0) != (mqtt->client_id == NULL)) return true;
    if (mqtt->client_id && strcmp(s_mqtt_client_id, mqtt->client_id) != 0) return true;
    if ((strlen(s_mqtt_address) == 0) != (mqtt->address == NULL)) return true;
    if (mqtt->address && strcmp(s_mqtt_address, mqtt->address) != 0) return true;
    if ((strlen(s_mqtt_username) == 0) != (mqtt->username == NULL)) return true;
    if (mqtt->username && strcmp(s_mqtt_username, mqtt->username) != 0) return true;
    if ((strlen(s_mqtt_password) == 0) != (mqtt->password == NULL)) return true;
    if (mqtt->password && strcmp(s_mqtt_password, mqtt->password) != 0) return true;
    if (s_mqtt_port != mqtt->port1) return true;
    if (s_mqtt_use_ssl != mqtt->use_ssl1) return true;
    return false;
}

static void config_check_task(void *arg)
{
    ESP_LOGI(TAG, "Config check task started (every %ds)", CONFIG_CHECK_INTERVAL_MS / 1000);

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(CONFIG_CHECK_INTERVAL_MS));

        /* Check WiFi */
        sscma_client_wifi_t wifi;
        memset(&wifi, 0, sizeof(wifi));
        if (get_wifi_config(s_client, &wifi) == ESP_OK && wifi_config_changed(&wifi)) {
            ESP_LOGW(TAG, "Fallback: WiFi config changed");
            apply_wifi_config(&wifi);
            free(wifi.ssid);
            free(wifi.password);

            if (s_mqtt_client != NULL) {
                esp_mqtt_client_stop(s_mqtt_client);
                esp_mqtt_client_destroy(s_mqtt_client);
                s_mqtt_client = NULL;
                xEventGroupClearBits(s_event_group, MQTT_CONNECTED_BIT);
            }
            wifi_reconnect();
            EventBits_t bits = xEventGroupWaitBits(s_event_group, WIFI_CONNECTED_BIT,
                                                    pdFALSE, pdFALSE, pdMS_TO_TICKS(15000));
            if (bits & WIFI_CONNECTED_BIT) mqtt_reconnect();
            continue;
        }
        free(wifi.ssid);
        free(wifi.password);

        /* Check MQTT */
        sscma_client_mqtt_t mqtt;
        memset(&mqtt, 0, sizeof(mqtt));
        if (get_mqtt_config(s_client, &mqtt) == ESP_OK && mqtt_config_changed(&mqtt)) {
            ESP_LOGW(TAG, "Fallback: MQTT config changed");
            apply_mqtt_config(&mqtt);
            free(mqtt.client_id);
            free(mqtt.address);
            free(mqtt.username);
            free(mqtt.password);
            mqtt_reconnect();
            continue;
        }
        free(mqtt.client_id);
        free(mqtt.address);
        free(mqtt.username);
        free(mqtt.password);
    }
}

/* ========== Main ========== */

void app_main(void)
{
    ESP_LOGI(TAG, "mqtt2uart v3.0.0 starting...");
    ESP_LOGI(TAG, "Free memory: %" PRIu32 " bytes", esp_get_free_heap_size());

    /* Init NVS */
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    s_event_group = xEventGroupCreate();

    /* Try NVS cache first */
    bool have_wifi = nvs_load_wifi();
    bool have_mqtt = nvs_load_mqtt();

    /* Init SSCMA (on_connect will read configs and save to NVS) */
    sscma_setup();

    if (have_wifi && have_mqtt) {
        /* Use cached config -- connect immediately */
        ESP_LOGI(TAG, "Using NVS cached config");
        wifi_start();
        EventBits_t bits = xEventGroupWaitBits(s_event_group, WIFI_CONNECTED_BIT,
                                                pdFALSE, pdFALSE, portMAX_DELAY);
        if (bits & WIFI_CONNECTED_BIT) {
            mqtt_start();
        }
    } else {
        /* No cache -- wait for SSCMA on_connect */
        ESP_LOGI(TAG, "Waiting for SSCMA...");
        xEventGroupWaitBits(s_event_group, SSCMA_CONNECTED_BIT, pdFALSE, pdFALSE, portMAX_DELAY);
        wifi_start();
        EventBits_t bits = xEventGroupWaitBits(s_event_group, WIFI_CONNECTED_BIT,
                                                pdFALSE, pdFALSE, portMAX_DELAY);
        if (!(bits & WIFI_CONNECTED_BIT)) { esp_restart(); }
        mqtt_start();
    }

    ESP_LOGI(TAG, "Proxy started");

    /* Fallback polling task -- catches config changes if SSCMA didn't push events */
    xTaskCreate(config_check_task, "cfg_check", 4 * 1024, NULL, 2, NULL);

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}
