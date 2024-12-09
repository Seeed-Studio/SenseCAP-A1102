#include "app_himax.h"
#include "app_modbus.h"
// #include "app_mqtt.h"
#include "cJSON.h"
#include "cJSON_Utils.h"
#include <regex.h>

static const char *TAG = "app_himax";

extern a1102_holding_reg_params_t a1102_msg;
extern holding_image_params modbus_image;
extern bool master_is_reading_flag;

#define CONCATENATE_STRINGS(str) str "\"" CONFIG_APP_PROJECT_VER "\"" "\r\n"

void himax_free_image(struct himax_data_image *p_data)
{
    p_data->len = 0;
    p_data->time = 0;
    if (p_data->p_buf != NULL)
    {
        free(p_data->p_buf);
    }
    p_data->p_buf = NULL;
}

void himax_data_inference_free(struct himax_data_inference_info *p_inference)
{
    if (p_inference->p_data != NULL)
    {
        free(p_inference->p_data);
    }
    p_inference->p_data = NULL;

    // for (int i = 0; p_inference->classes[i] != NULL && i < CONFIG_MODEL_CLASSES_MAX_NUM; i++)
    // {
    //     free(p_inference->classes[i]);
    //     p_inference->classes[i] = NULL;
    // }
    p_inference->cnt = 0;
    p_inference->is_valid = false;
    p_inference->type = INFERENCE_TYPE_UNKNOWN;
}

void extract_numbers_and_check_save_jpeg(const char *input)
{
  
    regex_t regex;
    regmatch_t matches[2];
    const char *pattern = ">=([0-9]+)"; 
    int ret;

    ret = regcomp(&regex, pattern, REG_EXTENDED);
    if (ret)
    {
        printf("Could not compile regex\n");
        return;
    }

    const char *cursor = input;
    int number_found = 0;
    int i = 0;
    while ((ret = regexec(&regex, cursor, 2, matches, 0)) == 0)
    {

        int match_len = matches[1].rm_eo - matches[1].rm_so;
        char number[match_len + 1];
        strncpy(number, cursor + matches[1].rm_so, match_len);
        number[match_len] = '\0';
        printf("Found number: %s\n", number);
        g_a1102_param.modle_p.confidence[i++] = atoi(number);
        cursor += matches[0].rm_eo; 
        number_found = 1;
    }

    if (!number_found)
    {
        printf("No numbers found in the string\n");
    }


    if (strstr(input, "save_jpeg") != NULL)
    {
        printf("The string contains 'save_jpeg'\n");
        g_a1102_param.modle_p.save_pic_flage = true;
        // sscma_client_invoke(client, -1, false, false);
    }
    else
    {
        printf("The string does not contain 'save_jpeg'\n");
        g_a1102_param.modle_p.save_pic_flage = false;
    }


    regfree(&regex);
}

int find_data_position_in_first_json(cJSON *first_json, cJSON *second_json) {

    cJSON *second_data = cJSON_GetObjectItem(second_json, "data");
    if (second_data == NULL) {
        printf("No 'data' found in second JSON.\n");
        return -1;
    }

    cJSON *data_array = cJSON_GetObjectItem(first_json, "data");
    if (data_array == NULL) {
        printf("No 'data' array found in first JSON.\n");
        return -1;
    }

    int data_count = cJSON_GetArraySize(data_array); 
    for (int i = 0; i < data_count; i++) {
        cJSON *item = cJSON_GetArrayItem(data_array, i);
        if (item != NULL) {
  
            cJSON *id_item = cJSON_GetObjectItem(item, "id");
            cJSON *id_second = cJSON_GetObjectItem(second_data, "id");

            cJSON *type_item = cJSON_GetObjectItem(item, "type");
            cJSON *type_second = cJSON_GetObjectItem(second_data, "type");

            cJSON *address_item = cJSON_GetObjectItem(item, "address");
            cJSON *address_second = cJSON_GetObjectItem(second_data, "address");

            cJSON *size_item = cJSON_GetObjectItem(item, "size");
            cJSON *size_second = cJSON_GetObjectItem(second_data, "size");

    
            if (id_item && type_item && address_item && size_item &&
                id_item->valueint == id_second->valueint &&
                type_item->valueint == type_second->valueint &&
                address_item->valueint == address_second->valueint &&
                size_item->valueint == size_second->valueint) {
                return data_count - i; 
            }
        }
    }

    printf("Data not found in first JSON.\n");
    return -1; 
}


void refresh_modbus_rigester(struct himax_module_ai_camera_preview_info *info)
{
    portENTER_CRITICAL(&param_lock);

    if (info->inference.type == INFERENCE_TYPE_BOX)
    {
        sscma_client_box_t *boxes = (sscma_client_box_t *)info->inference.p_data;
        for (int i = 0; i < info->inference.cnt && i < 9; i++)
        {
            mb_set_uint32_abcd((val_32_arr *)&a1102_msg.result[i], boxes[i].target * 1000 + boxes[i].score * 10);
            mb_set_uint32_abcd((val_32_arr *)&a1102_msg.xy[i], boxes[i].x * 1000 + boxes[i].y);
            mb_set_uint32_abcd((val_32_arr *)&a1102_msg.wh[i], boxes[i].w * 1000 + boxes[i].h);
        }
    }
    else if (info->inference.type == INFERENCE_TYPE_CLASS)
    {
        sscma_client_class_t *classes = (sscma_client_class_t *)info->inference.p_data;
        for (int i = 0; i < info->inference.cnt && i < 9; i++)
        {
            mb_set_uint32_abcd((val_32_arr *)&a1102_msg.result[i], classes[i].target * 1000 + classes[i].score * 10);
        }
    }
    else if (info->inference.type == INFERENCE_TYPE_POINT)
    {
        sscma_client_point_t *points = (sscma_client_point_t *)info->inference.p_data;
        for (int i = 0; i < info->inference.cnt && i < 9; i++)
        {
            mb_set_uint32_abcd((val_32_arr *)&a1102_msg.result[i], points[i].target * 1000 + points[i].score * 10);
        }
    }

    portEXIT_CRITICAL(&param_lock);
}

void on_event(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
  

    ESP_LOGI(TAG, "on_event print");
    // printf("Reply data: %s\n", reply->data);
    ESP_LOGI(TAG, "on_event");
    esp_err_t ret = ESP_OK;

    struct himax_module_ai_camera_preview_info info;
    sscma_client_class_t *classes = NULL;
    sscma_client_box_t *boxes = NULL;
    sscma_client_point_t *points = NULL;

    int class_count = 0;
    int box_count = 0;
    int point_count = 0;
    char *img = NULL;
    int img_size = 0;

    info.img.p_buf = NULL;
    info.img.len = 0;

    info.inference.cnt = 0;
    info.inference.is_valid = false;
    info.inference.p_data = NULL;

    if (sscma_utils_fetch_boxes_from_reply(reply, &boxes, &box_count) == ESP_OK)
    {
        info.inference.type = INFERENCE_TYPE_BOX;
        info.inference.p_data = (void *)boxes;
        info.inference.cnt = box_count;
        if (box_count > 0)
        {
            for (int i = 0; i < box_count; i++)
            {
                ESP_LOGD(TAG, "[box %d]: x=%d, y=%d, w=%d, h=%d, score=%d, target=%d", i, boxes[i].x, boxes[i].y, boxes[i].w, boxes[i].h, boxes[i].score, boxes[i].target);
            }
        }
    }
    else if (sscma_utils_fetch_classes_from_reply(reply, &classes, &class_count) == ESP_OK)
    {
        info.inference.type = INFERENCE_TYPE_CLASS;
        info.inference.p_data = (void *)classes;
        info.inference.cnt = class_count;
        if (class_count > 0)
        {
            for (int i = 0; i < class_count; i++)
            {
                ESP_LOGD(TAG, "[class %d]: target=%d, score=%d", i, classes[i].target, classes[i].score);
            }
        }
    }
    else if (sscma_utils_fetch_points_from_reply(reply, &points, &point_count) == ESP_OK)
    {
        info.inference.type = INFERENCE_TYPE_POINT;
        info.inference.p_data = (void *)points;
        info.inference.cnt = point_count;
        if (point_count > 0)
        {
            for (int i = 0; i < point_count; i++)
            {
                ESP_LOGD(TAG, "[point %d]: x=%d, y=%d, z=%d, score=%d, target=%d", i, points[i].x, points[i].y, points[i].z, points[i].score, points[i].target);
            }
        }
    }
    else
    {
        ESP_LOGI(TAG, " Unknown type ");
    }


    // img_get_cnt ++;
    ESP_LOGI(TAG, "img_get_cnt = %d",img_get_cnt);
    if(img_get_cnt >=2){
        if(sscma_utils_copy_image_from_reply(reply,modbus_image.image,20480,&modbus_image.image_size) != ESP_OK){
            ESP_LOGI(TAG, "image copy fail!");
        }else{
            ESP_LOGI(TAG, "image copy ok!");
            if (img_size & 1)
            {
                modbus_image.reg_length = modbus_image.image_size / 2 + 1;
            }
            else
            {
                modbus_image.reg_length = modbus_image.image_size / 2;
            }
        }
        xSemaphoreGive(xSemaphore);
        img_get_cnt = 0;
    }

    refresh_modbus_rigester(&info);

    himax_free_image(&info.img);
    himax_data_inference_free(&info.inference);

    // sscma_client_invoke(client, -1, false, false);
}

void on_log(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    ESP_LOGI(TAG, "on_log print");
    printf("Reply data: %s\n", reply->data);
    ESP_LOGI(TAG, "on_log");

    // esp_err_t ret = ESP_OK;

    // struct himax_module_ai_camera_preview_info info;
    // sscma_client_class_t *classes = NULL;
    // sscma_client_box_t *boxes = NULL;
    // sscma_client_point_t *points = NULL;

    // int class_count = 0;
    // int box_count = 0;
    // int point_count = 0;
    // char *img = NULL;
    // int img_size = 0;

    // info.img.p_buf = NULL;
    // info.img.len = 0;

    // info.inference.cnt = 0;
    // info.inference.is_valid = false;
    // info.inference.p_data = NULL;

    // if (sscma_utils_fetch_image_from_reply(reply, &img, &img_size) == ESP_OK)
    // {
    //     info.img.p_buf = (uint8_t *)img;
    //     info.img.len = img_size;
    //     info.img.time = time(NULL);
    //     ESP_LOGD(TAG, "Small img:%.1fk (%d), time: %ld", (float)img_size / 1024, img_size, (long)time(NULL));

    //     if (img_size > sizeof(modbus_image.image))
    //     {
    //         ESP_LOGI("event", "image_size > sizeof(modbus_image.image)\r\n");
    //     }
    //     else
    //     {
    //         if (master_is_reading_flag == false)
    //         {
    //             modbus_image.image_size = img_size;
    //             portENTER_CRITICAL(&param_lock);
    //             if (img_size & 1)
    //             {
    //                 modbus_image.reg_length = img_size / 2 + 1;
    //             }
    //             else
    //             {
    //                 modbus_image.reg_length = img_size / 2;
    //             }
    //             memset(modbus_image.image, 0, sizeof(modbus_image.image));
    //             memcpy(modbus_image.image, img, img_size);
    //             portEXIT_CRITICAL(&param_lock);
    //             ESP_LOGI("event", "memcpy img_size %d finish !! register size is %d !!\r\n", img_size, modbus_image.reg_length);
    //         }
    //     }
    // }

    // if (sscma_utils_fetch_boxes_from_reply(reply, &boxes, &box_count) == ESP_OK)
    // {
    //     info.inference.type = INFERENCE_TYPE_BOX;
    //     info.inference.p_data = (void *)boxes;
    //     info.inference.cnt = box_count;
    //     if (box_count > 0)
    //     {
    //         for (int i = 0; i < box_count; i++)
    //         {
    //             ESP_LOGD(TAG, "[box %d]: x=%d, y=%d, w=%d, h=%d, score=%d, target=%d", i, boxes[i].x, boxes[i].y, boxes[i].w, boxes[i].h, boxes[i].score, boxes[i].target);
    //         }
    //     }
    // }
    // else if (sscma_utils_fetch_classes_from_reply(reply, &classes, &class_count) == ESP_OK)
    // {
    //     info.inference.type = INFERENCE_TYPE_CLASS;
    //     info.inference.p_data = (void *)classes;
    //     info.inference.cnt = class_count;
    //     if (class_count > 0)
    //     {
    //         for (int i = 0; i < class_count; i++)
    //         {
    //             ESP_LOGD(TAG, "[class %d]: target=%d, score=%d", i, classes[i].target, classes[i].score);
    //         }
    //     }
    // }
    // else if (sscma_utils_fetch_points_from_reply(reply, &points, &point_count) == ESP_OK)
    // {
    //     info.inference.type = INFERENCE_TYPE_POINT;
    //     info.inference.p_data = (void *)points;
    //     info.inference.cnt = point_count;
    //     if (point_count > 0)
    //     {
    //         for (int i = 0; i < point_count; i++)
    //         {
    //             ESP_LOGD(TAG, "[point %d]: x=%d, y=%d, z=%d, score=%d, target=%d", i, points[i].x, points[i].y, points[i].z, points[i].score, points[i].target);
    //         }
    //     }
    // }
    // else
    // {
    //     ESP_LOGI(TAG, " Unknown type ");
    // }

    // refresh_modbus_rigester(&info);

    // himax_free_image(&info.img);
    // himax_data_inference_free(&info.inference);

    // sscma_client_invoke(client, -1, false, false);
}

void on_response(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{
    ESP_LOGI(TAG, "on_response print");
    printf("Reply data: %s\n", reply->data);

    ESP_LOGI(TAG, "on_response");

    // esp_err_t ret = ESP_OK;

    // struct himax_module_ai_camera_preview_info info;
    // sscma_client_class_t *classes = NULL;
    // sscma_client_box_t *boxes = NULL;
    // sscma_client_point_t *points = NULL;

    // int class_count = 0;
    // int box_count = 0;
    // int point_count = 0;
    // char *img = NULL;
    // int img_size = 0;

    // info.img.p_buf = NULL;
    // info.img.len = 0;

    // info.inference.cnt = 0;
    // info.inference.is_valid = false;
    // info.inference.p_data = NULL;

    // if(sscma_utils_copy_image_from_reply(reply,modbus_image.image,20480,&modbus_image.image_size) != ESP_OK){
    //     ESP_LOGI(TAG, "image copy fail!");
    // }

    // if (sscma_utils_fetch_boxes_from_reply(reply, &boxes, &box_count) == ESP_OK)
    // {
    //     info.inference.type = INFERENCE_TYPE_BOX;
    //     info.inference.p_data = (void *)boxes;
    //     info.inference.cnt = box_count;
    //     if (box_count > 0)
    //     {
    //         for (int i = 0; i < box_count; i++)
    //         {
    //             ESP_LOGD(TAG, "[box %d]: x=%d, y=%d, w=%d, h=%d, score=%d, target=%d", i, boxes[i].x, boxes[i].y, boxes[i].w, boxes[i].h, boxes[i].score, boxes[i].target);
    //         }
    //     }
    // }
    // else if (sscma_utils_fetch_classes_from_reply(reply, &classes, &class_count) == ESP_OK)
    // {
    //     info.inference.type = INFERENCE_TYPE_CLASS;
    //     info.inference.p_data = (void *)classes;
    //     info.inference.cnt = class_count;
    //     if (class_count > 0)
    //     {
    //         for (int i = 0; i < class_count; i++)
    //         {
    //             ESP_LOGD(TAG, "[class %d]: target=%d, score=%d", i, classes[i].target, classes[i].score);
    //         }
    //     }
    // }
    // else if (sscma_utils_fetch_points_from_reply(reply, &points, &point_count) == ESP_OK)
    // {
    //     info.inference.type = INFERENCE_TYPE_POINT;
    //     info.inference.p_data = (void *)points;
    //     info.inference.cnt = point_count;
    //     if (point_count > 0)
    //     {
    //         for (int i = 0; i < point_count; i++)
    //         {
    //             ESP_LOGD(TAG, "[point %d]: x=%d, y=%d, z=%d, score=%d, target=%d", i, points[i].x, points[i].y, points[i].z, points[i].score, points[i].target);
    //         }
    //     }
    // }
    // else
    // {
    //     ESP_LOGI(TAG, " Unknown type ");
    // }

    // refresh_modbus_rigester(&info);

    // himax_free_image(&info.img);
    // himax_data_inference_free(&info.inference);

    // sscma_client_invoke(client, -1, false, false);
}

void on_connect(sscma_client_handle_t client, const sscma_client_reply_t *reply, void *user_ctx)
{

  
    ESP_LOGI("APP_VERSION", "App version: %s", CONFIG_APP_PROJECT_VER);
    char *cmd = CONCATENATE_STRINGS("AT+WIFIVER=");
    printf("cmd = %s\r\n",cmd);
    sscma_client_reply_t rep;
    esp_err_t ree = sscma_client_request(client, cmd, &rep, true, CMD_WAIT_DELAY);

    if (ree == ESP_OK)
    {
        if (rep.payload != NULL)
        {
            cJSON *data = cJSON_GetObjectItem(rep.payload, "data");
            if (data != NULL)
            {
                char *data_string = cJSON_Print(data);

                if (data_string != NULL)
                {
                    printf("Data as string: %s\n", data_string);

                    free(data_string);
                }
                else
                {
                    printf("Failed to convert 'data' to string.\n");
                }
            }
            else
            {
                printf("'data' is NULL or not found in the JSON object.\n");
            }
            sscma_client_reply_clear(&rep);
        }
    }


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

        if (model->uuid != NULL)
        {
            portENTER_CRITICAL(&param_lock);
            mb_set_uint32_abcd((val_32_arr *)&a1102_msg.modle_id, (1000000000 + atoi(model->uuid) * 1000 + 140));
            portEXIT_CRITICAL(&param_lock);
            printf("atoi(model->uuid) * 1000 = : %d\n", atoi(model->uuid) * 1000);
            printf("1000000000 + atoi(model->uuid) * 1000 + 140 : %d\n", (1000000000 + atoi(model->uuid) * 1000 + 140));
        }
        else
        {
            printf("model->uuid == NULL\r\n");
        }

        printf("Name: %s\n", model->name ? model->name : "N/A");
        g_a1102_param.modle_p.model_name = model->name;
        printf("Version: %s\n", model->ver ? model->ver : "N/A");
        printf("URL: %s\n", model->url ? model->url : "N/A");
        printf("Checksum: %s\n", model->checksum ? model->checksum : "N/A");
        printf("Classes:\n");
        if (model->classes[0] != NULL)
        {
            g_a1102_param.modle_p.classes_count = 0;
            for (int i = 0; model->classes[i] != NULL; i++)
            {
                g_a1102_param.modle_p.classes[i] = model->classes[i];
                g_a1102_param.modle_p.classes_count++;
                g_a1102_param.modle_p.confidence[i] = 0;
                printf("  - %s\n", model->classes[i]);
            }
        }
        else
        {
            printf("  N/A\n");
        }
    }

    char command[30] = "AT+ACTION?\r\n";
    sscma_client_reply_t m_reply;
    esp_err_t ret = sscma_client_request(client, command, &m_reply, true, CMD_WAIT_DELAY);

    if (ret == ESP_OK)
    {
        if (m_reply.payload != NULL)
        {
            cJSON *data = cJSON_GetObjectItem(m_reply.payload, "data");
            if (data != NULL)
            {
                char *data_string = cJSON_Print(data);

                if (data_string != NULL)
                {
                    printf("Data as string: %s\n", data_string);

                    free(data_string);
                }
                else
                {
                    printf("Failed to convert 'data' to string.\n");
                }

              
                cJSON *action = cJSON_GetObjectItem(data, "action");
                if (action != NULL && cJSON_IsString(action))
                {
                   
                    printf("Action: %s\n", action->valuestring);
                    extract_numbers_and_check_save_jpeg(action->valuestring);
                }
                else
                {
                    printf("Action field not found or is not a string.\n");
                }
            }
            else
            {
                printf("'data' is NULL or not found in the JSON object.\n");
            }
            sscma_client_reply_clear(&m_reply);
        }
    }

    
    
    char command2[30] = "AT+MODELS?\r\n";
    sscma_client_reply_t m_reply2;
    ret = sscma_client_request(client, command2, &m_reply2, true, CMD_WAIT_DELAY);
    char command3[30] = "AT+MODEL?\r\n";
    sscma_client_reply_t m_reply3;
    ret = sscma_client_request(client, command3, &m_reply3, true, CMD_WAIT_DELAY);

    if(m_reply2.payload != NULL && m_reply3.payload != NULL){
        int ret; 
        ret = find_data_position_in_first_json(m_reply2.payload,m_reply3.payload);
        if(ret != -1){
            g_a1102_param.modle_p.current_modle = ret;
        }
        sscma_client_reply_clear(&m_reply2);
        sscma_client_reply_clear(&m_reply3);
        printf("find_data_position_in_first_json is %d,current model is %d\r\n",ret,g_a1102_param.modle_p.current_modle);
    }

    sscma_client_wifi_t wifi;
    memset(&wifi, 0, sizeof(wifi));
    if (get_wifi_config(client, &wifi) == ESP_OK)
    {
        ESP_LOGI(TAG, "SSID: %s", wifi.ssid == NULL ? "NULL" : wifi.ssid);
        ESP_LOGI(TAG, "Password: %s", wifi.password == NULL ? "NULL" : wifi.password);

        nvs_handle_t my_handle;
        esp_err_t ret = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &my_handle);

        if (ret != ESP_OK)
        {
            ESP_LOGE(TAG, "Error (%s) opening NVS handle!", esp_err_to_name(ret));
        }
        else
        {
            ESP_LOGI(TAG, "NVS handle opened successfully");

            const char *ssid = wifi.ssid ? wifi.ssid : "null";
            const char *password = wifi.password ? wifi.password : "null";

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

        if (ret != ESP_OK)
        {
            ESP_LOGE(TAG, "Error (%s) opening NVS handle!", esp_err_to_name(ret));
        }
        else
        {
            ESP_LOGI(TAG, "NVS handle opened successfully");

            const char *username = mqtt.username ? mqtt.username : "null";
            const char *password = mqtt.password ? mqtt.password : "null";
            const char *address = mqtt.address ? mqtt.address : "null";
            const char *client_id = mqtt.client_id ? mqtt.client_id : "null";
            const int *port1 = mqtt.port1 ? mqtt.port1 : "null";
            const int *use_ssl1 = mqtt.use_ssl1;

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



    char cmmd[30]="AT+SENSOR=1,1,0\r\n";
    sscma_client_reply_t rep2;
    ree = sscma_client_request(client, cmmd, &rep2, true, CMD_WAIT_DELAY);

    if (ree == ESP_OK)
    {
        if (rep2.payload != NULL)
        {
            cJSON *data = cJSON_GetObjectItem(rep2.payload, "data");
            if (data != NULL)
            {
                char *data_string = cJSON_Print(data);

                if (data_string != NULL)
                {
                    printf("Data as string: %s\n", data_string);

                    free(data_string);
                }
                else
                {
                    printf("Failed to convert 'data' to string.\n");
                }
            }
            else
            {
                printf("'data' is NULL or not found in the JSON object.\n");
            }
            sscma_client_reply_clear(&rep2);
        }
    }

    sscma_client_invoke(client, -1, 0, 0);
}

void preview()
{

    ESP_LOGI(TAG, "preview");
    portENTER_CRITICAL(&param_lock);
    for (int i = 0; i < 10; i++)
    {
        mb_set_uint32_abcd((val_32_arr *)&a1102_msg.result[i], (uint32_t)-1000);
    }
    modbus_image.image_size = 0;
    modbus_image.reg_length = 0;
    memset(modbus_image.image, '\0', sizeof(modbus_image.image));
    portEXIT_CRITICAL(&param_lock);
    // char at_command[20];
    // sprintf(at_command, "AT+INFO?\r\n");
    // sprintf(at_command, "AT+INVOKE=1,0,0\r\n");
    // printf("preview Command: %s\n", at_command);
    // sscma_client_write(client,at_command, 15);
    sscma_client_invoke(client, 1, 0, 1);
    vTaskDelay(100);
    img_get_cnt = 2;
    sscma_client_invoke(client, 1, 0, 1);
    ESP_LOGI(TAG, "preview end");
    vTaskDelay(100);
    sscma_client_invoke(client, -1, 0, 0);

}

void set_modle(int modle_number)
{
    ESP_LOGI(TAG, "change modle to %d", modle_number);

    sscma_client_set_model(client, modle_number);
    vTaskDelay(500);
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

        if (model->uuid != NULL)
        {
            portENTER_CRITICAL(&param_lock);
            mb_set_uint32_abcd((val_32_arr *)&a1102_msg.modle_id, (1000000000 + atoi(model->uuid) * 1000 + 140));
            portEXIT_CRITICAL(&param_lock);
            printf("atoi(model->uuid) * 1000 = : %d\n", atoi(model->uuid) * 1000);
            printf("1000000000 + atoi(model->uuid) * 1000 + 140 : %d\n", (1000000000 + atoi(model->uuid) * 1000 + 140));
        }
        else
        {
            printf("model->uuid == NULL\r\n");
        }

        printf("Name: %s\n", model->name ? model->name : "N/A");
        g_a1102_param.modle_p.model_name = model->name;
        printf("Version: %s\n", model->ver ? model->ver : "N/A");
        printf("URL: %s\n", model->url ? model->url : "N/A");
        printf("Checksum: %s\n", model->checksum ? model->checksum : "N/A");
        printf("Classes:\n");
        if (model->classes[0] != NULL)
        {
            g_a1102_param.modle_p.classes_count = 0;
            for (int i = 0; model->classes[i] != NULL; i++)
            {
                g_a1102_param.modle_p.classes[i] = model->classes[i];
                g_a1102_param.modle_p.classes_count++;
                g_a1102_param.modle_p.confidence[i] = 0;
                printf("  - %s\n", model->classes[i]);
            }
        }
        else
        {
            printf("  N/A\n");
        }
    }

    char command[30] = "AT+ACTION?\r\n";
    sscma_client_reply_t m_reply;
    esp_err_t ret = sscma_client_request(client, command, &m_reply, true, CMD_WAIT_DELAY);

    if (ret == ESP_OK)
    {
        if (m_reply.payload != NULL)
        {
            cJSON *data = cJSON_GetObjectItem(m_reply.payload, "data");
            if (data != NULL)
            {
                char *data_string = cJSON_Print(data);

                if (data_string != NULL)
                {
                    printf("Data as string: %s\n", data_string);

                    free(data_string);
                }
                else
                {
                    printf("Failed to convert 'data' to string.\n");
                }

                // 获取 "action" 字段
                cJSON *action = cJSON_GetObjectItem(data, "action");
                if (action != NULL && cJSON_IsString(action))
                {
                    // 打印 "action" 字段的内容
                    printf("Action: %s\n", action->valuestring);
                    extract_numbers_and_check_save_jpeg(action->valuestring);
                }
                else
                {
                    printf("Action field not found or is not a string.\n");
                }
            }
            else
            {
                printf("'data' is NULL or not found in the JSON object.\n");
            }
            sscma_client_reply_clear(&m_reply);
        }
    }



    char command2[30] = "AT+MODELS?\r\n";
    sscma_client_reply_t m_reply2;
    ret = sscma_client_request(client, command2, &m_reply2, true, CMD_WAIT_DELAY);
    char command3[30] = "AT+MODEL?\r\n";
    sscma_client_reply_t m_reply3;
    ret = sscma_client_request(client, command3, &m_reply3, true, CMD_WAIT_DELAY);

    if(m_reply2.payload != NULL && m_reply3.payload != NULL){
        int ret; 
        ret = find_data_position_in_first_json(m_reply2.payload,m_reply3.payload);
        if(ret != -1 && ret != g_a1102_param.modle_p.current_modle){
            g_a1102_param.modle_p.current_modle = ret;
            model_change_flag = true;
        }else{
            model_change_flag = false;
        }
        sscma_client_reply_clear(&m_reply2);
        sscma_client_reply_clear(&m_reply3);
        printf("find_data_position_in_first_json is %d,current model is %d\r\n",ret,g_a1102_param.modle_p.current_modle);
    }

}

int set_modle_confidence(int count, ClassData *data, bool save_flag)
{

    int buffer_size = 50 * count + 50;
    char *command = (char *)malloc(buffer_size);
    if (!command)
    {
        printf("Memory allocation failed.\n");
        return -1;
    }

    g_a1102_param.modle_p.save_pic_flage = save_flag;

    if(count == 0 && data == NULL && save_flag == 0){
        strcpy(command, "AT+ACTION=\"");
        strcat(command, "\"\r\n");
        memset(g_a1102_param.modle_p.confidence, 0, sizeof(g_a1102_param.modle_p.confidence));
    }else{
        strcpy(command, "AT+ACTION=\"(");
        for (int i = 0; i < count; i++)
        {
            char condition[50];
            sprintf(condition, "(max_score(target,%d)>=%d)", i, data->classes[i].confidence);
            strcat(command, condition);
            if (i < count - 1)
            {
                strcat(command, "||");
            }

            g_a1102_param.modle_p.confidence[i] = data->classes[i].confidence;
        }
        
        if (save_flag)
        {
            strcat(command, ")");
            strcat(command, "&&save_jpeg()\"\r\n");
        }
        else
        {
            strcat(command, ")\"\r\n");
            // strcpy(command, "AT+ACTION=\"");
            // strcat(command, "\"\r\n");
            // memset(g_a1102_param.modle_p.confidence, 0, sizeof(g_a1102_param.modle_p.confidence));
        }
    }

    

    if (command)
    {
        printf("%s", command);
        // sscma_client_write(client,command, 15);
        sscma_client_reply_t reply;
        ESP_RETURN_ON_ERROR(sscma_client_request(client, command, &reply, true, CMD_WAIT_DELAY), TAG, "confidence set failed");

        if (reply.payload != NULL)
        {
            cJSON *data = cJSON_GetObjectItem(reply.payload, "data");
            if (data != NULL)
            {
                char *data_string = cJSON_Print(data);

                if (data_string != NULL)
                {
                    printf("Data as string: %s\n", data_string);

                    free(data_string);
                }
                else
                {
                    printf("Failed to convert 'data' to string.\n");
                }
            }
            else
            {
                printf("'data' is NULL or not found in the JSON object.\n");
            }
            sscma_client_reply_clear(&reply);
        }
        free(command);
    }


    if(data != NULL){
        if (data->classes != NULL)
        {
            free(data->classes);
            data->classes = NULL;
        }
        data->class_count = 0;
    }

    
    
    return 1;
}

void get_model_info()
{
    ESP_LOGI(TAG, "get_model_info");
    char at_command[20];
    sprintf(at_command, "AT+INFO?\r\n");
    printf("get_model_info Command: %s\n", at_command);
    sscma_client_write(client, at_command, 15);
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

    ESP_ERROR_CHECK(uart_driver_install(UART_NUM_1, 4 * 1024, 0, 0, NULL, intr_alloc_flags));
    ESP_ERROR_CHECK(uart_param_config(UART_NUM_1, &uart_config));
    ESP_ERROR_CHECK(uart_set_pin(UART_NUM_1, 21, 20, -1, -1));

    sscma_client_io_uart_config_t io_uart_config = {
        .user_ctx = NULL,
    };

    sscma_client_config_t sscma_client_config = SSCMA_CLIENT_CONFIG_DEFAULT();
    sscma_client_config.reset_gpio_num = 5;
    sscma_client_config.tx_buffer_size = 2048;
    sscma_client_config.rx_buffer_size = 24 * 1024;

    sscma_client_new_io_uart_bus((sscma_client_uart_bus_handle_t)1, &io_uart_config, &io);

    sscma_client_new(io, &sscma_client_config, &client);

    const sscma_client_callback_t callback = {
        .on_connect = on_connect,
        .on_event = on_event,
        .on_response = on_response,
        .on_log = on_log,
    };

    if (client == NULL)
    {
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

    // sscma_client_invoke(client, -1, false, false);
    vTaskDelay(2000);
    // ESP_LOGI(TAG,"init get pic\n");
    // sscma_client_write(client,"AT+INVOKE=1,0,0", 15);
    // vTaskDelay(500);
    // sscma_client_invoke(client, -1, false, false);

    // ESP_LOGI(TAG,"init get pic ok !\n");
}