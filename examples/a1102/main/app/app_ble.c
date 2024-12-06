#include "app_ble.h"
#include "app_nvs.h"

#define TAG "A1102 NimBLE_GATT_Server"



static volatile atomic_bool g_ble_connected = ATOMIC_VAR_INIT(false);
static volatile atomic_int g_curr_mtu = ATOMIC_VAR_INIT(23);

/* Private function declarations */
static int read_chr_access(uint16_t conn_handle, uint16_t attr_handle,
                                 struct ble_gatt_access_ctxt *ctxt, void *arg);

static int write_chr_access(uint16_t conn_handle, uint16_t attr_handle,
                                 struct ble_gatt_access_ctxt *ctxt, void *arg);



static const ble_uuid16_t adv_uuid[] = {BLE_UUID16_INIT(0x2886),BLE_UUID16_INIT(0xa886)};


static const ble_uuid128_t param__svc_uuid =  BLE_UUID128_INIT(0x55, 0xE4, 0x05, 0xD2, 0xAF, 0x9F, 0xA9, 0x8F, 0xE5, 0x4A, 0x7D, 0xFE, 0x43, 0x53, 0x53, 0x49);


static uint8_t read_param_chr_val[2] = {0};
static uint16_t read_param_chr_val_handle;

static const ble_uuid128_t read_param_chr_uuid = BLE_UUID128_INIT(0x16, 0x96, 0x24, 0x47, 0xC6, 0x23, 0x61, 0xBA, 0xD9, 0x4B, 0x4D, 0x1E, 0x43, 0x53, 0x53, 0x49);


static uint16_t read_param_chr_conn_handle = 0;
static bool read_param_chr_conn_handle_inited = false;
static bool read_param_ind_status = false;


static uint8_t write_param_chr_val[2] = {0};
static uint16_t write_param_chr_val_handle;
static const ble_uuid128_t write_param_chr_uuid = BLE_UUID128_INIT(0xB3, 0x9B, 0x72, 0x34, 0xBE, 0xEC, 0xD4, 0xA8, 0xF4, 0x43, 0x41, 0x88, 0x43, 0x53, 0x53, 0x49);


/* Private variables */
static uint8_t own_addr_type;
static uint8_t addr_val[6] = {0};


static void start_advertising(void);
/* Library function declarations */
void ble_store_config_init(void);

/* Private functions */
inline static void format_addr(char *addr_str, uint8_t addr[]) {
    sprintf(addr_str, "%02X:%02X:%02X:%02X:%02X:%02X", addr[0], addr[1],
            addr[2], addr[3], addr[4], addr[5]);
}

static void print_conn_desc(struct ble_gap_conn_desc *desc) {
    /* Local variables */
    char addr_str[12] = {0};

    /* Connection handle */
    ESP_LOGI(TAG, "connection handle: %d", desc->conn_handle);

    /* Local ID address */
    format_addr(addr_str, desc->our_id_addr.val);
    ESP_LOGI(TAG, "device id address: type=%d, value=%s",
             desc->our_id_addr.type, addr_str);

    /* Peer ID address */
    format_addr(addr_str, desc->peer_id_addr.val);
    ESP_LOGI(TAG, "peer id address: type=%d, value=%s", desc->peer_id_addr.type,
             addr_str);

    /* Connection info */
    ESP_LOGI(TAG,
             "conn_itvl=%d, conn_latency=%d, supervision_timeout=%d, "
             "encrypted=%d, authenticated=%d, bonded=%d\n",
             desc->conn_itvl, desc->conn_latency, desc->supervision_timeout,
             desc->sec_state.encrypted, desc->sec_state.authenticated,
             desc->sec_state.bonded);
}



/* Private functions */

static int read_chr_access(uint16_t conn_handle, uint16_t attr_handle,
                                 struct ble_gatt_access_ctxt *ctxt, void *arg) {
    /* Local variables */
    int rc =0;

    /* Handle access events */
    /* Note: Heart rate characteristic is read only */
    switch (ctxt->op) {

    /* Read characteristic event */
    case BLE_GATT_ACCESS_OP_READ_CHR:
        
        /* Verify connection handle */
        if (conn_handle != BLE_HS_CONN_HANDLE_NONE) {
            ESP_LOGI(TAG, "characteristic read; conn_handle=%d attr_handle=%d",
                     conn_handle, attr_handle);
        } else {
            ESP_LOGI(TAG, "characteristic read by nimble stack; attr_handle=%d",
                     attr_handle);
        }

        /* Verify attribute handle */
        if (attr_handle == read_param_chr_val_handle) {
 
            cJSON *json = cJSON_CreateObject();

            cJSON_AddNumberToObject(json, "baudrate", g_a1102_param.modbus_p.modbus_baud);
            cJSON_AddNumberToObject(json, "slaveID", g_a1102_param.modbus_p.modbus_address);
            cJSON_AddStringToObject(json, "model", model_to_string(g_a1102_param.modle_p.current_modle));
            // cJSON_AddNumberToObject(json, "confidence", g_a1102_param.modle_p.modle_confidence);

            char *json_string = cJSON_Print(json);
            printf("Generated JSON: \n%s\n", json_string);

            rc = os_mbuf_append(ctxt->om, json_string,
            strlen(json_string));

            free(json);
            free(json_string);

            return rc == 0 ? 0 : BLE_ATT_ERR_INSUFFICIENT_RES;
        }
        goto error;
    
    /* Unknown event */
    default:
        goto error;
    }

error:
    ESP_LOGE(
        TAG,
        "unexpected access operation to heart rate characteristic, opcode: %d",
        ctxt->op);
    return BLE_ATT_ERR_UNLIKELY;
}



static int write_chr_access(uint16_t conn_handle, uint16_t attr_handle,
                                 struct ble_gatt_access_ctxt *ctxt, void *arg) {
    /* Local variables */
    int rc;

    /* Handle access events */
    /* Note: Heart rate characteristic is read only */
    switch (ctxt->op) {

    case BLE_GATT_ACCESS_OP_WRITE_CHR:
        /* Verify connection handle */
        if (conn_handle != BLE_HS_CONN_HANDLE_NONE) {
            ESP_LOGI(TAG, "characteristic write; conn_handle=%d attr_handle=%d",
                     conn_handle, attr_handle);
        } else {
            ESP_LOGI(TAG,
                     "characteristic write by nimble stack; attr_handle=%d",
                     attr_handle);
        }

        /* Verify attribute handle */
        if (attr_handle == write_param_chr_val_handle) {

            size_t ble_msg_len = OS_MBUF_PKTLEN(ctxt->om);
            ble_msg_t ble_msg = {.size = ble_msg_len, .msg = malloc(ble_msg_len + 1)};  // 1 for null-terminator
            ble_msg.msg[ble_msg.size] = '\0';
            uint16_t real_len = 0;
            rc = ble_hs_mbuf_to_flat(ctxt->om, ble_msg.msg, ble_msg_len, &real_len);
            if (rc != 0) {
                free(ble_msg.msg);
                return BLE_ATT_ERR_UNLIKELY;
            }
            ESP_LOGI(TAG, "mbuf_len: %d, copied %d bytes from ble stack.", ble_msg_len, real_len);
            ESP_LOGD(TAG, "ble msg: %s", ble_msg.msg);

            if (xQueueSend(ble_msg_queue, &ble_msg, pdMS_TO_TICKS(10)) != pdPASS) {
                ESP_LOGW(TAG, "failed to send ble msg to queue, maybe at_cmd task stalled???");
            } else {
                ESP_LOGD(TAG, "ble msg enqueued");
            }

            
            } else {
                goto error;
            }
            return rc;
        
        goto error;

    /* Unknown event */
    default:
        goto error;
    }

error:
    ESP_LOGE(
        TAG,
        "unexpected access operation to heart rate characteristic, opcode: %d",
        ctxt->op);
    return BLE_ATT_ERR_UNLIKELY;
}




/* GATT services table */
static const struct ble_gatt_svc_def gatt_svr_svcs[] = {
    /* Heart rate service */
    {.type = BLE_GATT_SVC_TYPE_PRIMARY,
     .uuid = &param__svc_uuid.u,
     .characteristics =
         (struct ble_gatt_chr_def[]){
             {/* modle type characteristic */
              .uuid = &read_param_chr_uuid.u,
              .access_cb = read_chr_access,
              .flags = BLE_GATT_CHR_F_READ | BLE_GATT_CHR_F_INDICATE | BLE_GATT_CHR_F_NOTIFY,
              .val_handle = &read_param_chr_val_handle},
            {/* modle confidence characteristic */
              .uuid = &write_param_chr_uuid.u,
              .access_cb = write_chr_access,
              .flags = BLE_GATT_CHR_F_WRITE ,
              .val_handle = &write_param_chr_val_handle},
             {
                 0, /* No more characteristics in this service. */
             }}},

    {
        0, /* No more services. */
    },
};




/* Public functions */
void send_heart_rate_indication(void) {
    if (read_param_ind_status && read_param_chr_conn_handle_inited) {

        ble_gatts_indicate(read_param_chr_conn_handle,
                           read_param_chr_val_handle);

        ESP_LOGI(TAG, "read param indication sent!");
    }
}

void send_heart_rate_notify(void){
    int rc;
    rc = ble_gatts_notify(read_param_chr_conn_handle, read_param_chr_val_handle);
    if (rc != 0) {
        printf("Failed to send notification: %d\n", rc);
    } else {
        // printf("Notification sent: %s\n", data);
    }

}

int gap_init(void) {
    /* Local variables */
    int rc = 0;

    /* Call NimBLE GAP initialization API */
    ble_svc_gap_init();
    return rc;
}


int gatt_svc_init(void) {
    /* Local variables */
    int rc;

    /* 1. GATT service initialization */
    ble_svc_gatt_init();

    /* 2. Update GATT services counter */
    rc = ble_gatts_count_cfg(gatt_svr_svcs);
    if (rc != 0) {
        return rc;
    }

    /* 3. Add GATT services */
    rc = ble_gatts_add_svcs(gatt_svr_svcs);
    if (rc != 0) {
        return rc;
    }

    return 0;
}



/*
 *  GATT server subscribe event callback
 *      1. Update heart rate subscription status
 */

void gatt_svr_subscribe_cb(struct ble_gap_event *event) {
    /* Check connection handle */
    if (event->subscribe.conn_handle != BLE_HS_CONN_HANDLE_NONE) {
        ESP_LOGI(TAG, "subscribe event; conn_handle=%d attr_handle=%d",
                 event->subscribe.conn_handle, event->subscribe.attr_handle);
    } else {
        ESP_LOGI(TAG, "subscribe by nimble stack; attr_handle=%d",
                 event->subscribe.attr_handle);
    }

    /* Check attribute handle */
    if (event->subscribe.attr_handle == read_param_chr_val_handle) {
        ESP_LOGI(TAG, "subscribe modle type");
        /* Update heart rate subscription status */
        read_param_chr_conn_handle = event->subscribe.conn_handle;
        read_param_chr_conn_handle_inited = true;
        read_param_ind_status = event->subscribe.cur_indicate;
    }
}

void gatt_svr_notify_cb(struct ble_gap_event *event){
        /* Check connection handle */
    if (event->subscribe.conn_handle != BLE_HS_CONN_HANDLE_NONE) {
        ESP_LOGI(TAG, "subscribe event; conn_handle=%d attr_handle=%d",
                 event->subscribe.conn_handle, event->subscribe.attr_handle);
    } else {
        ESP_LOGI(TAG, "subscribe by nimble stack; attr_handle=%d",
                 event->subscribe.attr_handle);
    }



}


static int gap_event_handler(struct ble_gap_event *event, void *arg) {
    /* Local variables */
    int rc = 0;
    struct ble_gap_conn_desc desc;

    /* Handle different GAP event */
    switch (event->type) {

    /* Connect event */
    case BLE_GAP_EVENT_CONNECT:
        /* A new connection was established or a connection attempt failed. */
        ESP_LOGI(TAG, "connection %s; status=%d",
                 event->connect.status == 0 ? "established" : "failed",
                 event->connect.status);

        /* Connection succeeded */
        if (event->connect.status == 0) {
            /* Check connection handle */
            rc = ble_gap_conn_find(event->connect.conn_handle, &desc);
            if (rc != 0) {
                ESP_LOGE(TAG,
                         "failed to find connection by handle, error code: %d",
                         rc);
                return rc;
            }
            atomic_store(&g_ble_connected, true);
            /* Print connection descriptor */
            print_conn_desc(&desc);

            /* Try to update connection parameters */
            struct ble_gap_upd_params params = {.itvl_min = desc.conn_itvl,
                                                .itvl_max = desc.conn_itvl,
                                                .latency = 3,
                                                .supervision_timeout =
                                                    desc.supervision_timeout};
            rc = ble_gap_update_params(event->connect.conn_handle, &params);
            if (rc != 0) {
                ESP_LOGE(
                    TAG,
                    "failed to update connection parameters, error code: %d",
                    rc);
                return rc;
            }
        }
        /* Connection failed, restart advertising */
        else {
            start_advertising();
        }
        return rc;

    /* Disconnect event */
    case BLE_GAP_EVENT_DISCONNECT:
        /* A connection was terminated, print connection descriptor */
        ESP_LOGI(TAG, "disconnected from peer; reason=%d",
                 event->disconnect.reason);
        atomic_store(&g_ble_connected, false);
        /* Restart advertising */
        start_advertising();
        return rc;

    /* Connection parameters update event */
    case BLE_GAP_EVENT_CONN_UPDATE:
        /* The central has updated the connection parameters. */
        ESP_LOGI(TAG, "connection updated; status=%d",
                 event->conn_update.status);

        /* Print connection descriptor */
        rc = ble_gap_conn_find(event->conn_update.conn_handle, &desc);
        if (rc != 0) {
            ESP_LOGE(TAG, "failed to find connection by handle, error code: %d",
                     rc);
            return rc;
        }
        print_conn_desc(&desc);
        return rc;

    /* Advertising complete event */
    case BLE_GAP_EVENT_ADV_COMPLETE:
        /* Advertising completed, restart advertising */
        ESP_LOGI(TAG, "advertise complete; reason=%d",
                 event->adv_complete.reason);
        start_advertising();
        return rc;

    /* Notification sent event */
    case BLE_GAP_EVENT_NOTIFY_TX:
        if ((event->notify_tx.status != 0) &&
            (event->notify_tx.status != BLE_HS_EDONE)) {
            /* Print notification info on error */
            ESP_LOGI(TAG,
                     "notify event; conn_handle=%d attr_handle=%d "
                     "status=%d is_indication=%d",
                     event->notify_tx.conn_handle, event->notify_tx.attr_handle,
                     event->notify_tx.status, event->notify_tx.indication);
        }
        return rc;

    /* Subscribe event */
    case BLE_GAP_EVENT_SUBSCRIBE:
        /* Print subscription info to log */
        ESP_LOGI(TAG,
                 "subscribe event; conn_handle=%d attr_handle=%d "
                 "reason=%d prevn=%d curn=%d previ=%d curi=%d",
                 event->subscribe.conn_handle, event->subscribe.attr_handle,
                 event->subscribe.reason, event->subscribe.prev_notify,
                 event->subscribe.cur_notify, event->subscribe.prev_indicate,
                 event->subscribe.cur_indicate);

        /* GATT subscribe event callback */
        gatt_svr_subscribe_cb(event);
        return rc;

    /* MTU update event */
    case BLE_GAP_EVENT_MTU:
        /* Print MTU update info to log */
        ESP_LOGI(TAG, "mtu update event; conn_handle=%d cid=%d mtu=%d",
                 event->mtu.conn_handle, event->mtu.channel_id,
                 event->mtu.value);
        if (event->mtu.value < 20) {
            ESP_LOGW(TAG, "mtu become less than 20??? really?");
        }

        atomic_store(&g_curr_mtu, event->mtu.value);

        return rc;
    }

    return rc;
}                                   

int app_ble_get_current_mtu(void)
{
    int mtu = atomic_load(&g_curr_mtu);
    return mtu;
}

static void start_advertising(void) {


    char s_name[40];
    /* Local variables */
    int rc = 0;
    const char *name;
    struct ble_hs_adv_fields adv_fields = {0};
    struct ble_hs_adv_fields rsp_fields = {0};
    struct ble_gap_adv_params adv_params = {0};


    char *sn = get_SN(1);
    if (sn != NULL)
    {
        strcpy(s_name, sn);
        ESP_LOGI(TAG, "SN name =  %s,len = %d",s_name,strlen(s_name));
        strcat(s_name, "-A1102");
        ESP_LOGI(TAG, "NEW  name =  %s,len = %d",s_name,strlen(s_name));
        free(sn);
    }else{
        strcpy(s_name, "Default");
        ESP_LOGI(TAG, "SN name =  %s,len = %d",s_name,strlen(s_name));
        strcat(s_name, "-A1102");
        ESP_LOGI(TAG, "NEW  name =  %s,len = %d",s_name,strlen(s_name));
    }
    

    /* Set GAP device name */
    rc = ble_svc_gap_device_name_set(s_name);
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to set device name to %s, error code: %d",
                 sn, rc);
        return;
    }

    /* Set advertising flags */
    adv_fields.flags = BLE_HS_ADV_F_DISC_GEN | BLE_HS_ADV_F_BREDR_UNSUP;


    adv_fields.uuids16 = adv_uuid;
    adv_fields.num_uuids16 = 2;
    adv_fields.uuids16_is_complete = 1; 



    /* Set advertiement fields */
    rc = ble_gap_adv_set_fields(&adv_fields);
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to set advertising data, error code: %d", rc);
        return;
    }

    /* Set device name */
    // name = ble_svc_gap_device_name();
    rsp_fields.name = (uint8_t *)s_name;
    rsp_fields.name_len = strlen(s_name);
    rsp_fields.name_is_complete = 1;

    

    /* Set scan response fields */
    rc = ble_gap_adv_rsp_set_fields(&rsp_fields);
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to set scan response data, error code: %d", rc);
        return;
    }

    /* Set non-connetable and general discoverable mode to be a beacon */
    adv_params.conn_mode = BLE_GAP_CONN_MODE_UND;
    adv_params.disc_mode = BLE_GAP_DISC_MODE_GEN;

    /* Set advertising interval */
    adv_params.itvl_min = BLE_GAP_ADV_ITVL_MS(500);
    adv_params.itvl_max = BLE_GAP_ADV_ITVL_MS(510);

    /* Start advertising */
    rc = ble_gap_adv_start(own_addr_type, NULL, BLE_HS_FOREVER, &adv_params,
                           gap_event_handler, NULL);
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to start advertising, error code: %d", rc);
        return;
    }
    ESP_LOGI(TAG, "advertising started!");
}



static void on_stack_reset(int reason) {
    /* On reset, print reset reason to console */
    ESP_LOGI(TAG, "nimble stack reset, reset reason: %d", reason);
}


static void on_stack_sync(void) {
    /* On stack sync, do advertising initialization */
        /* Local variables */
    int rc = 0;
    char addr_str[12] = {0};

    /* Make sure we have proper BT identity address set (random preferred) */
    rc = ble_hs_util_ensure_addr(0);
    if (rc != 0) {
        ESP_LOGE(TAG, "device does not have any available bt address!");
        return;
    }

    /* Figure out BT address to use while advertising (no privacy for now) */
    rc = ble_hs_id_infer_auto(0, &own_addr_type);
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to infer address type, error code: %d", rc);
        return;
    }

    /* Printing ADDR */
    rc = ble_hs_id_copy_addr(own_addr_type, addr_val, NULL);
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to copy device address, error code: %d", rc);
        return;
    }
    format_addr(addr_str, addr_val);
    ESP_LOGI(TAG, "device address: %s", addr_str);

    /* Start advertising. */
    start_advertising();

}

/*
 *  Handle GATT attribute register events
 *      - Service register event
 *      - Characteristic register event
 *      - Descriptor register event
 */
void gatt_svr_register_cb(struct ble_gatt_register_ctxt *ctxt, void *arg) {
    /* Local variables */
    char buf[BLE_UUID_STR_LEN];

    /* Handle GATT attributes register events */
    switch (ctxt->op) {

    /* Service register event */
    case BLE_GATT_REGISTER_OP_SVC:
        ESP_LOGD(TAG, "registered service %s with handle=%d",
                 ble_uuid_to_str(ctxt->svc.svc_def->uuid, buf),
                 ctxt->svc.handle);
        break;

    /* Characteristic register event */
    case BLE_GATT_REGISTER_OP_CHR:
        ESP_LOGD(TAG,
                 "registering characteristic %s with "
                 "def_handle=%d val_handle=%d",
                 ble_uuid_to_str(ctxt->chr.chr_def->uuid, buf),
                 ctxt->chr.def_handle, ctxt->chr.val_handle);
        break;

    /* Descriptor register event */
    case BLE_GATT_REGISTER_OP_DSC:
        ESP_LOGD(TAG, "registering descriptor %s with handle=%d",
                 ble_uuid_to_str(ctxt->dsc.dsc_def->uuid, buf),
                 ctxt->dsc.handle);
        break;

    /* Unknown event */
    default:
        assert(0);
        break;
    }
}


static void __bleprph_host_task(void *param)
{
    ESP_LOGI(TAG, "BLE Host Task Started");
    /* This function will return only when nimble_port_stop() is executed */
    nimble_port_run();
    nimble_port_freertos_deinit();
    ESP_LOGD(TAG, "BLE Host Task Ended");
}

void app_ble_init(){
    int ret, rc;
        /* NimBLE stack initialization */
    ret = nimble_port_init();
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "failed to initialize nimble stack, error code: %d ",
                 ret);
        return;
    }

        /* GAP service initialization */
    rc = gap_init();
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to initialize GAP service, error code: %d", rc);
        return;
    }


        /* GATT server initialization */
    rc = gatt_svc_init();
    if (rc != 0) {
        ESP_LOGE(TAG, "failed to initialize GATT server, error code: %d", rc);
        return;
    }


    /* Set host callbacks */
    ble_hs_cfg.reset_cb = on_stack_reset;
    ble_hs_cfg.sync_cb = on_stack_sync;
    ble_hs_cfg.gatts_register_cb = gatt_svr_register_cb;
    ble_hs_cfg.store_status_cb = ble_store_util_status_rr;

    /* Store host configuration */
    ble_store_config_init();



    nimble_port_freertos_init(__bleprph_host_task);
}



/**
 * Send an indicate to NimBLE stack, normally from the at cmd task context.
*/
esp_err_t app_ble_send_indicate(uint8_t *data, int len)
{
    if (!atomic_load(&g_ble_connected)) return BLE_HS_ENOTCONN;
    int rc = ESP_FAIL;
    struct os_mbuf *txom = NULL;

    // xEventGroupSetBits(g_eg_ble, EVENT_INDICATE_SENDING);

    int mtu = app_ble_get_current_mtu();
    int txlen = 0, txed_len = 0;

    const int wait_step_climb = 10, wait_max = 10000;  //ms
    int wait = 0, wait_step = wait_step_climb, wait_sum = 0;  //ms
    const int retry_max = 20;
    int retry_cnt = 0;
    while (len > 0 && atomic_load(&g_ble_connected)) {
        txlen = MIN(len, mtu - 3);
        txom = ble_hs_mbuf_from_flat(data + txed_len, txlen);
        ESP_LOGI(TAG, "after mbuf alloc, os_msys_count: %d, os_msys_num_free: %d", os_msys_count(), os_msys_num_free());
        if (!txom) {
            wait += wait_step;
            wait_step += wait_step_climb;
            ESP_LOGI(TAG, "app_ble_send_indicate, mbuf alloc failed, wait %dms", wait);
            vTaskDelay(pdMS_TO_TICKS(wait));
            wait_sum += wait;
            if (wait_sum > wait_max) {
                ESP_LOGE(TAG, "app_ble_send_indicate, mbuf alloc timeout!!!");
                rc = BLE_HS_ENOMEM;
                goto indicate_end;
            }
            continue;
        }
        wait = wait_sum = 0;
        wait_step = wait_step_climb;

        vTaskDelay(pdMS_TO_TICKS(50));  
        rc = ble_gatts_indicate_custom(read_param_chr_conn_handle, read_param_chr_val_handle, txom);
        //txom will be consumed anyways, we don't need to release it here.
        if (rc != 0) {
            ESP_LOGI(TAG, "ble_gatts_indicate_custom failed (rc=%d, mtu=%d, txlen=%d, remain_len=%d), retry ...", rc, mtu, txlen, len);
            retry_cnt++;
            if (retry_cnt > retry_max) {
                ESP_LOGE(TAG, "ble_gatts_indicate_custom failed overall after %d retries!!!", retry_max);
                rc = BLE_HS_ESTALLED;
                goto indicate_end;
            }
            continue;
        }
        txed_len += txlen;
        len -= txlen;
        // ESP_LOGI(TAG, "indication sent successfully, mtu=%d, txlen=%d, remain_len=%d", mtu, txlen, len);
        // vTaskDelay(pdMS_TO_TICKS(100));  // to avoid watchdog dead in cpu1
    }

    if (len != 0) {
        rc = BLE_ERR_CONN_TERM_LOCAL;
    }

indicate_end:
    ESP_LOGD(TAG, "before app_ble_send_indicate return, os_msys_count: %d, os_msys_num_free: %d", os_msys_count(), os_msys_num_free());
    // xEventGroupClearBits(g_eg_ble, EVENT_INDICATE_SENDING);
    return rc;
}
