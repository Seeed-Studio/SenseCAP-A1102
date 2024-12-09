#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <regex.h>
#include <time.h>
#include <mbedtls/base64.h>
#include <sys/param.h>

#include "sdkconfig.h"
#include "freertos/FreeRTOS.h"
#include "esp_chip_info.h"
#include "esp_flash.h"
#include "esp_system.h"
#include "esp_log.h"
#include "esp_err.h"
#include "esp_check.h"
#include "esp_event.h"
#include "esp_wifi.h"
#include "cJSON.h"
#include "esp_check.h"



#include "app_ble.h"
#include "uhash.h"
#include "at_cmd.h"
#include "app_modbus.h"
#include "app_nvs.h"



#define AT_CMD_BUFFER_LEN_STEP   (1024 * 10)  // the growing step of the size of at cmd buffer
#define AT_CMD_BUFFER_MAX_LEN    (1024 * 50)  // top limit of the at cmd buffer size, don't be too huge
#define BLE_MSG_Q_SIZE            10

extern a1102_holding_reg_params_t a1102_msg;
extern holding_image_params modbus_image;

/*------------------system basic DS-----------------------------------------------------*/
const char *TAG = "at_cmd";
const char *pattern = "^AT\\+([a-zA-Z0-9]+)(\\?|=(\\{.*\\}))?\r\n$";
command_entry *commands = NULL; // Global variable to store the commands
static at_cmd_buffer_t g_at_cmd_buffer = {.buff = NULL, .cap = AT_CMD_BUFFER_LEN_STEP, .wr_ptr = 0};



/*-----------------------------------bind index--------------------------------------------*/
static int bind_index;

/*----------------------------------------------------------------------------------------*/

static SemaphoreHandle_t data_sem_handle = NULL;

static void __data_lock(void)
{
    if( data_sem_handle == NULL ) {
        return;
    }
    xSemaphoreTake(data_sem_handle, portMAX_DELAY);
}
static void __data_unlock(void)
{
    if( data_sem_handle == NULL ) {
        return;
    }
    xSemaphoreGive(data_sem_handle);  
}







// AT command system
/*----------------------------------------------------------------------------------------------------*/

/**
 * @brief Creates an AT command response by appending a standard suffix to the given message.
 *
 * This function takes a message string, appends the standard suffix "\r\nok\r\n" to it,
 * and allocates memory for the complete response. It returns an AT_Response structure
 * containing the formatted response and its length.
 *
 * @param message A constant character pointer to the message to be included in the response.
 *                If the message is NULL, an empty response is created.
 * @return AT_Response A structure containing the formatted response string and its length.
 */
esp_err_t send_at_response(const char *message)
{
    esp_err_t ret = ESP_FAIL;
    char *response = NULL;

    if (message)
    {
        const char *suffix = "\r\nok\r\n";
        size_t total_length = strlen(message) + strlen(suffix) + 10; // a few bytes more
        response = malloc(total_length);
        if (response)
        {
            ESP_LOGI(TAG, "malloc response OK");
            strcpy(response, message);
            strcat(response, suffix);
            size_t newlen = strlen(response);

            ret = app_ble_send_indicate((uint8_t *)response, newlen);
            if (ret != ESP_OK) {
                ESP_LOGW(TAG, "Failed to send AT response, ret=%d", ret);
            }
        }
    }

    if (response){
        ESP_LOGI(TAG, "FREE response ");
        ESP_LOGI(TAG, "Free heap before free(response): %ld", esp_get_free_heap_size());
        free(response);
        ESP_LOGI(TAG, "Free heap before free(response): %ld", esp_get_free_heap_size());
    }

    return ret;
}

/**
 * @brief Add a command to the hash table of commands.
 *
 * This function creates a new command entry and adds it to the hash table of commands.
 *
 * @param commands A pointer to the hash table of commands.
 * @param name The name of the command.
 * @param func A pointer to the function that implements the command.
 */
void add_command(command_entry **commands, const char *name, at_cmd_error_code (*func)(char *params))
{
    // command_entry *entry = (command_entry *)malloc(sizeof(command_entry)); // Allocate memory for the new entry
    command_entry *entry = (command_entry *)malloc(sizeof(command_entry));
    strcpy(entry->command_name, name);            // Copy the command name to the new entry
    entry->func = func;                           // Assign the function pointer to the new entry
    HASH_ADD_STR(*commands, command_name, entry); // Add the new entry to the hash table
}

/**
 * @brief Execute a command from the hash table.
 *
 * This function searches for a command by name in the hash table and executes it
 * with the provided parameters. If the query character is '?', the command is treated
 * as a query command.
 *
 * @param commands A pointer to the hash table of commands.
 * @param name The name of the command to execute.
 * @param params The parameters to pass to the command function.
 * @param query The query character that modifies the command behavior.
 */
void exec_command(command_entry **commands, const char *name, char *params, char query)
{
    ESP_LOGI(TAG, "Free heap before exec_command: %ld", esp_get_free_heap_size());

    at_cmd_error_code error_code=ESP_OK;
    command_entry *entry;
    char full_command[128];
    snprintf(full_command, sizeof(full_command), "%s%c", name, query); // Append the query character to the command name
    ESP_LOGD(TAG, "full_command: %s", full_command);
    HASH_FIND_STR(*commands, full_command, entry);
    if (entry)
    {
        if (query == '?') // If the query character is '?', then the command is a query command
        {
            error_code = entry->func(NULL);
        }
        else
        {
            error_code = entry->func(params);
        }
    }
    else
    {
        ESP_LOGI(TAG, "Command not found\n");
        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "name", "Command_not_found");
        cJSON_AddNumberToObject(root, "code", ERROR_CMD_COMMAND_NOT_FOUND);
        char *json_string = cJSON_Print(root);
        ESP_LOGE(TAG, "JSON String: %s\n", json_string);
        send_at_response(json_string);
        cJSON_Delete(root);
        free(json_string);
    }
    if (error_code != ESP_OK)
    {
        ESP_LOGE(TAG, "Error code: %d\n", error_code);
        ESP_LOGI(TAG, "Commond exec failed \n");
        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "name", "Commond_exec_failed");
        cJSON_AddNumberToObject(root, "code", error_code);
        char *json_string = cJSON_Print(root);
        ESP_LOGD(TAG, "JSON String: %s\n", json_string);
        send_at_response(json_string);
        cJSON_Delete(root);
        free(json_string);
    }
}

/**
 * @brief Register the AT commands.
 *
 * This function adds various AT commands to the hash table of commands.
 */
void AT_command_reg()
{
    // deviceinfo   devicecfg= modle= modle? confidence= confidence? 波特率= 波特率？ 从机ID= 从机ID? 
    // Register the AT commands
    add_command(&commands, "deviceinfo?", handle_deviceinfo_command);
    add_command(&commands, "devicecfg=", handle_deviceinfo_cfg_command);  //why `devicecfg=` ? this would be history problem
    add_command(&commands, "classconfidence=", handle_classconfidence);
    add_command(&commands, "preview?", handle_preview_command);
    add_command(&commands, "reset?", handle_reset_command);
    add_command(&commands, "DSN=", handle_set_ble_sn_name_command);
    add_command(&commands, "SN?", handle_get_sn_command);

}

/**
 * @brief Frees all allocated memory for AT command entries in the hash table.
 *
 * This function iterates over all command entries in the hash table, deletes each entry from the hash table,
 * and frees the allocated memory for each command entry.
 */
void AT_command_free()
{
    command_entry *current_command, *tmp;
    HASH_ITER(hh, commands, current_command, tmp)
    {
        HASH_DEL(commands, current_command); // Delete the entry from the hash table
        free(current_command);
    }
}



int baute_rate_to_enum(uint32_t baute_rate){
    int b_enum = 0;
    switch (baute_rate)
    {
    case 4800:
        b_enum = 0;
        break;
    case 9600:
        b_enum = 1;
        break;
    case 14400:
        b_enum = 2;
        break;
    case 19200:
        b_enum = 3;
        break;
    case 38400:
        b_enum = 4;
        break;
    case 57600:
        b_enum = 5;
        break;
    case 115200:
        b_enum = 6;
        break;        
    default:
        ESP_LOGI(TAG,"unkown baute_rate");
        break;
    }
    return b_enum;
}


/**
 * @brief Handles the "deviceinfo" command by generating a JSON response with device information.
 *
 *
 * @param params A string containing the parameters for the command. This parameter is currently unused.
 *
 * The generated JSON object includes the following fields:
 * - name: "deviceinfo?"
 * - code: 0
 * - data: An object containing:
 *   - baud_rate: "1"
 *   - slave_id: "1"
 *   - Current_Model: 1
 *
 * The JSON string is then sent as an AT response.
 */
at_cmd_error_code handle_deviceinfo_command(char *params)
{
    (void)params; // Prevent unused parameter warning

    ESP_LOGI(TAG, "handle_deviceinfo_command\n");


    cJSON *root = cJSON_CreateObject();

    cJSON_AddStringToObject(root, "name", "deviceinfo");

    cJSON_AddNumberToObject(root, "code", 0);

    cJSON *data = cJSON_CreateObject();

    cJSON_AddItemToObject(root, "data", data);
    cJSON_AddNumberToObject(data, "baud_rate", baute_rate_to_enum(g_a1102_param.modbus_p.modbus_baud));
    cJSON_AddNumberToObject(data, "slave_id", g_a1102_param.modbus_p.modbus_address);
    cJSON_AddNumberToObject(data, "current_model", g_a1102_param.modle_p.current_modle);
    cJSON_AddNumberToObject(data, "save", g_a1102_param.modle_p.save_pic_flage);
    
    cJSON *classes_array = cJSON_CreateArray();
    for (int i = 0; i < g_a1102_param.modle_p.classes_count; i++) {
        cJSON *class_obj = cJSON_CreateObject();
        cJSON_AddStringToObject(class_obj, "class", g_a1102_param.modle_p.classes[i]);
        cJSON_AddNumberToObject(class_obj, "conf", g_a1102_param.modle_p.confidence[i]);
        cJSON_AddItemToArray(classes_array, class_obj);
    }

    cJSON_AddItemToObject(data, "classes", classes_array);
    
    char *json_string = cJSON_Print(root);

    ESP_LOGI(TAG, "JSON String in handle_deviceinfo_command: %s\n", json_string);
    esp_err_t send_result = send_at_response(json_string);
    if (send_result != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to send AT response\n");
        cJSON_Delete(root);
        free(json_string);
        return ERROR_CMD_RESPONSE;
    }
    cJSON_Delete(root);
    free(json_string);
    return AT_CMD_SUCCESS;
}


uint32_t parse_baute_rate(int baut_numbuer){
    uint32_t baute_rate = 0;
    switch (baut_numbuer)
    {
    case 0:
        baute_rate = 4800;
        break;
    case 1:
        baute_rate = 9600;
        break;
    case 2: 
        baute_rate = 14400;
        break;
    case 3:
        baute_rate = 19200;
        break;
    case 4:
        baute_rate = 38400;
        break;
    case 5: 
        baute_rate = 57600;
        break;
    case 6: 
        baute_rate = 115200;
        break;

    default:
        ESP_LOGI(TAG,"Unkown baut_numbuer");
        break;
    }

    return baute_rate;
}

at_cmd_error_code handle_deviceinfo_cfg_command(char *params)
{
    ESP_LOGI(TAG, "handle_deviceinfo_cfg_command\n");
    int time_flag = 0;
    bool is_need_refresh_slave_id = false;
    bool is_need_refresh_bauterate = false;
    bool is_need_refresh_modle_type = false;
    // bool is_need_refresh_modle_confidence = false;

    int new_slave_id = 0;
    int new_baud_rate = 0;
    int new_modle = 0;
    float new_confidence = 0;

    cJSON *json = cJSON_Parse(params);
    if (json == NULL)
    {
        const char *error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL)
        {
            ESP_LOGE(TAG, "Error before: %s\n", error_ptr);
        }
        return ERROR_CMD_JSON_PARSE;
    }

    cJSON *data = cJSON_GetObjectItemCaseSensitive(json, "data");
    if (cJSON_IsObject(data))
    {
        
        cJSON *baud_rate = cJSON_GetObjectItem(data, "baud_rate");
        if (cJSON_IsNumber(baud_rate)) {
            printf("Baud Rate: %d\n", baud_rate->valueint);
            uint32_t parse_b = parse_baute_rate(baud_rate->valueint);
            if(parse_b != 0 && parse_b != g_a1102_param.modbus_p.modbus_baud){
                is_need_refresh_bauterate = true;
                new_baud_rate = parse_b;
            }
        }

      
        cJSON *slave_id = cJSON_GetObjectItem(data, "slave_id");
        if (cJSON_IsNumber(slave_id)) {
            printf("Slave ID: %d\n", slave_id->valueint);
            if(slave_id->valueint != g_a1102_param.modbus_p.modbus_address){
                is_need_refresh_slave_id = true;
                new_slave_id = slave_id->valueint;
            }
        }

    
        cJSON *current_model = cJSON_GetObjectItem(data, "current_model");
        if (cJSON_IsNumber(current_model)) {
            printf("Current Model: %d\n", current_model->valueint);
            if(current_model->valueint != g_a1102_param.modle_p.current_modle){
                is_need_refresh_modle_type = true;
                new_modle = current_model->valueint;
            }
        }


    }
    else
    {
            ESP_LOGI(TAG, "Timezone or timestamp or daylight not found or not a valid string in JSON\n");
    }

    cJSON_Delete(json);
    
    model_change_flag = false;

    if(  is_need_refresh_slave_id
        || is_need_refresh_bauterate
        || is_need_refresh_modle_type)
    {
        //Respond first, then execute
        vTaskDelay(200 / portTICK_PERIOD_MS);
        if(is_need_refresh_slave_id && is_need_refresh_bauterate){
            ESP_LOGI(TAG, "change modbus");
            param_change_event_data_t event_data;
            event_data.new_slave_id = new_slave_id;
            event_data.new_baud_rate = new_baud_rate;
            ESP_ERROR_CHECK(esp_event_post_to(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, PARAM_CHANGE_MODBUS, &event_data, sizeof(param_change_event_data_t), portMAX_DELAY));
        }else if( is_need_refresh_slave_id ) {
            ESP_LOGI(TAG, "change slave id");
            param_change_event_data_t event_data;
            event_data.new_slave_id = new_slave_id;
            ESP_ERROR_CHECK(esp_event_post_to(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, PARAM_CHANGE_SLAVE_ID, &event_data, sizeof(param_change_event_data_t), portMAX_DELAY));
        }else if( is_need_refresh_bauterate ){
            ESP_LOGI(TAG, "change bauterate");
            param_change_event_data_t event_data;
            event_data.new_baud_rate = new_baud_rate;
            ESP_ERROR_CHECK(esp_event_post_to(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, PARAM_CHANGE_BAUD_RATE, &event_data, sizeof(param_change_event_data_t), portMAX_DELAY));
        }

        if( is_need_refresh_modle_type ) {
            ESP_LOGI(TAG, "change modle");
            param_change_event_data_t event_data;
            event_data.new_model_type = new_modle;
            ESP_ERROR_CHECK(esp_event_post_to(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, PARAM_CHANGE_MODEL_TYPE, &event_data, sizeof(param_change_event_data_t), portMAX_DELAY));
        }
        
    }else{
        ESP_LOGI(TAG, "no change");
    }


    vTaskDelay(3000);

    cJSON *root = cJSON_CreateObject();
    if (root == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        return ERROR_CMD_JSON_CREATE;
    }
    cJSON *data_rep = cJSON_CreateObject();
    if (data_rep == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        cJSON_Delete(root);
        return ERROR_CMD_JSON_CREATE;
    }

    cJSON_AddStringToObject(root, "name", "devicecfg");
    if(is_need_refresh_modle_type){
        ESP_LOGI(TAG, "is_need_refresh_modle_type = true\n");
        if(model_change_flag){
            ESP_LOGI(TAG, "model_change_flag = true\n");
            cJSON_AddNumberToObject(root, "code", 0);
        }else{
            ESP_LOGI(TAG, "model_change_flag = false\n");
            cJSON_AddNumberToObject(root, "code", 1);
        }
    }else{
        ESP_LOGI(TAG, "is_need_refresh_modle_type = FALSE\n");
        cJSON_AddNumberToObject(root, "code", 0);
    }


    char *json_string = cJSON_Print(root);
    ESP_LOGD(TAG, "JSON String in device cfg command: %s\n", json_string);
    esp_err_t send_result = send_at_response(json_string);
    if (send_result != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to send AT response\n");
        cJSON_Delete(root);
        free(json_string);
        return ERROR_CMD_RESPONSE;
    }
    cJSON_Delete(root);
    free(json_string);

    return AT_CMD_SUCCESS;
}

at_cmd_error_code handle_classconfidence(char *params){

    param_change_event_data_t event_data;

    cJSON *root = cJSON_Parse(params);
    if (root == NULL) {
        printf("Failed to parse JSON.\n");
        return -1;
    }

    cJSON *code_item = cJSON_GetObjectItem(root, "save");
    if (code_item == NULL) {
        printf("Error: 'code' field not found in the JSON\n");
        cJSON_Delete(root);
        return -1;
    }

    if (cJSON_IsNumber(code_item)) {
        int code_value = code_item->valueint;
        if (code_value == 0) {
            event_data.save_pic_flage = false;
        } else {
            event_data.save_pic_flage = true;
        }
    } else {
        printf("Error: 'code' field is not a number\n");
    }



    cJSON *data_array = cJSON_GetObjectItem(root, "data");
    if (!cJSON_IsArray(data_array)) {
        printf("Invalid JSON: 'data' is not an array.\n");
        cJSON_Delete(root);
        return -1;
    }

    int class_count = cJSON_GetArraySize(data_array);
    event_data.class_data.class_count = class_count;
    
    event_data.class_data.classes = (ClassInfo *)malloc(sizeof(ClassInfo) * class_count);


    for (int i = 0; i < class_count; i++) {
        cJSON *item = cJSON_GetArrayItem(data_array, i);
        if (cJSON_IsObject(item)) {
            cJSON *class_item = cJSON_GetObjectItem(item, "class");
            cJSON *conf_item = cJSON_GetObjectItem(item, "conf");

            if (cJSON_IsString(class_item) && cJSON_IsNumber(conf_item)) {
                strcpy(event_data.class_data.classes[i].class_name, class_item->valuestring);
                event_data.class_data.classes[i].confidence = conf_item->valueint;
            }
        }
    }

    ESP_ERROR_CHECK(esp_event_post_to(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, PARAM_CHANGE_CONFIDENCE, &event_data, sizeof(param_change_event_data_t), portMAX_DELAY));


    cJSON_Delete(root);

    root = cJSON_CreateObject();
    if (root == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        return ERROR_CMD_JSON_CREATE;
    }
    cJSON *data_rep = cJSON_CreateObject();
    if (data_rep == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        cJSON_Delete(root);
        return ERROR_CMD_JSON_CREATE;
    }
    cJSON_AddStringToObject(root, "name", "classconfidence");
    cJSON_AddNumberToObject(root, "code", 0);
    char *json_string = cJSON_Print(root);
    ESP_LOGD(TAG, "JSON String in device cfg command: %s\n", json_string);
    esp_err_t send_result = send_at_response(json_string);
    if (send_result != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to send AT response\n");
        cJSON_Delete(root);
        free(json_string);
        return ERROR_CMD_RESPONSE;
    }
    cJSON_Delete(root);
    free(json_string);


    return AT_CMD_SUCCESS;
}



at_cmd_error_code handle_preview_command(char *params) {
    (void)params; // Prevent unused parameter warning
  

    ESP_LOGI(TAG, "handle_preview_command\n");

    xSemaphoreTake(img_memory_lock, portMAX_DELAY);
    atomic_store(&img_memory_lock_owner, 1);
    
    xSemaphoreTake(xSemaphore, 0);
    img_get_cnt = 0;
    esp_event_post_to(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, PARAM_PREVIEW, NULL, 0, portMAX_DELAY);

    ESP_LOGI(TAG, "wait himax img give xSemaphore\n");
    if (xSemaphoreTake(xSemaphore, portMAX_DELAY) == pdTRUE) {

        vTaskDelay(1000);//Wait for memory to be released
        cJSON *root = cJSON_CreateObject();
        if (root == NULL) {
            xSemaphoreGive(xSemaphore); 
            xSemaphoreGive(img_memory_lock);
            return ERROR_CMD_JSON_CREATE;
        }

        cJSON_AddStringToObject(root, "name", "preview");
        cJSON_AddNumberToObject(root, "code", 0);

        cJSON *data_object = cJSON_CreateObject();
        if (data_object == NULL) {
            cJSON_Delete(root);
            xSemaphoreGive(xSemaphore);
            xSemaphoreGive(img_memory_lock);
            return ERROR_CMD_JSON_CREATE;
        }
     
        cJSON *result_array = cJSON_CreateArray();
        if (result_array == NULL) {
            cJSON_Delete(data_object);
            cJSON_Delete(root);
            xSemaphoreGive(xSemaphore);
            xSemaphoreGive(img_memory_lock);
            return ERROR_CMD_JSON_CREATE;
        }
  
        portENTER_CRITICAL(&param_lock);
        for (int i = 0; i < 10; i++) {
            cJSON *value = cJSON_CreateNumber(mb_get_uint32_abcd((val_32_arr *)&a1102_msg.result[i]));
            if (value == NULL) {
                portEXIT_CRITICAL(&param_lock);
                cJSON_Delete(result_array);
                cJSON_Delete(data_object);
                cJSON_Delete(root);
                xSemaphoreGive(xSemaphore);
                xSemaphoreGive(img_memory_lock);
                return ERROR_CMD_JSON_CREATE;
            }
            cJSON_AddItemToArray(result_array, value);
        }
        portEXIT_CRITICAL(&param_lock);
        cJSON_AddItemToObject(data_object, "result", result_array);

        modbus_image.image[modbus_image.image_size] = '\0';
        char *image_string = (char *)modbus_image.image;
                    // ESP_LOGI(TAG, "modbus_image.image = %s\r\n", image_string);
        if (cJSON_AddStringToObject(data_object, "image", image_string) == NULL) {
            ESP_LOGE(TAG, "Failed to add image to JSON object");
            cJSON_Delete(result_array);
            cJSON_Delete(data_object);
            cJSON_Delete(root);
            xSemaphoreGive(xSemaphore);
            xSemaphoreGive(img_memory_lock);
            return ERROR_CMD_JSON_CREATE;
        }

        
        cJSON_AddItemToObject(root, "data", data_object);

        char *json_string = cJSON_Print(root);
        if (json_string == NULL) {
            ESP_LOGE(TAG, "Failed to get json_string");
            cJSON_Delete(root);
            ESP_LOGI(TAG, "Free heap before cJSON_Delete(root) %ld", esp_get_free_heap_size());
            xSemaphoreGive(xSemaphore);
            return ERROR_CMD_JSON_CREATE;
        }
        xSemaphoreGive(img_memory_lock);
        atomic_store(&img_memory_lock_owner, 0);
        ESP_LOGI(TAG, "Free heap size: %ld", esp_get_free_heap_size());
        // printf("handle_preview_command json: %s\r\n", json_string);
        esp_err_t send_result = send_at_response(json_string);
        free(json_string); 
        cJSON_Delete(root); 

        if (send_result != ESP_OK) {
            ESP_LOGE(TAG, "Failed to send AT response");
            xSemaphoreGive(xSemaphore);
            return ERROR_CMD_RESPONSE;
        }

        xSemaphoreGive(xSemaphore);
        return AT_CMD_SUCCESS;
    }

    xSemaphoreGive(xSemaphore);
    return ERROR_CMD_RESPONSE;
}

at_cmd_error_code handle_reset_command(char *params) {
    (void)params; // Prevent unused parameter warning
    ESP_LOGI(TAG, "handle_reset_command\n");

    ESP_ERROR_CHECK(esp_event_post_to(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, PARAM_RESET, NULL, 0, portMAX_DELAY));

    vTaskDelay(3000);
    cJSON *root = cJSON_CreateObject();
    if (root == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        return ERROR_CMD_JSON_CREATE;
    }
    cJSON_AddStringToObject(root, "name", "reset");
    cJSON_AddNumberToObject(root, "code", 0);

    char *json_string = cJSON_Print(root);
    ESP_LOGD(TAG, "JSON String in device cfg command: %s\n", json_string);
    esp_err_t send_result = send_at_response(json_string);
    if (send_result != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to send AT response\n");
        cJSON_Delete(root);
        free(json_string);
        return ERROR_CMD_RESPONSE;
    }
    cJSON_Delete(root);
    free(json_string);

    return AT_CMD_SUCCESS;
}

at_cmd_error_code handle_set_ble_sn_name_command(char *params){

    cJSON *root = cJSON_Parse(params);
    if (root == NULL) {
        printf("Failed to parse JSON\n");
        return -1;
    }

    cJSON *dsn_item = cJSON_GetObjectItem(root, "DSN");
    if (dsn_item != NULL && cJSON_IsString(dsn_item)) {
        printf("DSN: %s\n", dsn_item->valuestring);
        esp_err_t ret;
        ret = set_sn(dsn_item->valuestring);\
        if(ret != ESP_OK){
            cJSON_Delete(root);
            return ERROR_CMD_RESPONSE;
        }
    } else {
        printf("Failed to get 'DSN' value or it is not a string\n");
    }

    cJSON_Delete(root);


    root = cJSON_CreateObject();
    if (root == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        return ERROR_CMD_JSON_CREATE;
    }
    cJSON *data_rep = cJSON_CreateObject();
    if (data_rep == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        cJSON_Delete(root);
        return ERROR_CMD_JSON_CREATE;
    }
    cJSON_AddStringToObject(root, "name", "DSN");
    cJSON_AddNumberToObject(root, "code", 0);
    char *json_string = cJSON_Print(root);
    ESP_LOGD(TAG, "JSON String in device cfg command: %s\n", json_string);
    esp_err_t send_result = send_at_response(json_string);
    if (send_result != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to send AT response\n");
        cJSON_Delete(root);
        free(json_string);
        return ERROR_CMD_RESPONSE;
    }
    cJSON_Delete(root);
    free(json_string);

    return AT_CMD_SUCCESS;

}

at_cmd_error_code handle_get_sn_command(char *params){
    (void)params;

    cJSON *root = cJSON_CreateObject();
    if (root == NULL)
    {
        ESP_LOGE(TAG, "Failed to create JSON object\n");
        return ERROR_CMD_JSON_CREATE;
    }
    cJSON_AddStringToObject(root, "name", "SN");
    cJSON_AddNumberToObject(root, "code", 0);
    
    cJSON_AddStringToObject(root, "data", get_SN(0));
    char *json_string = cJSON_Print(root);
    ESP_LOGD(TAG, "JSON String in device cfg command: %s\n", json_string);
    esp_err_t send_result = send_at_response(json_string);
    if (send_result != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to send AT response\n");
        cJSON_Delete(root);
        free(json_string);
        return ERROR_CMD_RESPONSE;
    }
    cJSON_Delete(root);
    free(json_string);

    return AT_CMD_SUCCESS;
}

/*-----------------------------------------------------------------------------------------------------------*/


/**
 * @brief A static task that handles incoming AT commands, parses them, and executes the corresponding actions.
 *
 * This function runs in an infinite loop, receiving messages from a stream buffer within bluetooth. It parses the received AT commands,
 * converts the hex data to a string, and uses regular expressions to match and extract command details.
 * The extracted command is then executed. The function relies on auxiliary functions like `byte_array_to_hex_string` to process
 * the received data.
 *
 * This task is declared static, indicating that it is intended to be used only within the file it is defined in,and placed in PSRAM
 */

void __at_cmd_proc_task(void *arg)
{
    at_cmd_buffer_t *cmd_buff = &g_at_cmd_buffer;

    while (1)
    {
        ble_msg_t ble_msg;
        if (xQueueReceive(ble_msg_queue, &ble_msg, portMAX_DELAY) != pdPASS) {
            ESP_LOGE(TAG, "Failed to receive ble message from queue");
            continue;
        }
        ESP_LOGI(TAG,"xQueueReceived");
        printf("BLE message as string: %s\n", ble_msg.msg);
        printf("BLE message len: %d\n", ble_msg.size);

        // grow the buffer
        __data_lock();
        while ((cmd_buff->cap - cmd_buff->wr_ptr - 1/* \0 */) < ble_msg.size && cmd_buff->cap < AT_CMD_BUFFER_MAX_LEN) {
            cmd_buff->cap += AT_CMD_BUFFER_LEN_STEP;
            if (cmd_buff->cap > AT_CMD_BUFFER_MAX_LEN) {
                ESP_LOGE(TAG, "at cmd buffer can't grow anymore, max_size=%d, want=%d", AT_CMD_BUFFER_MAX_LEN, cmd_buff->cap);
                cmd_buff->cap = AT_CMD_BUFFER_MAX_LEN;
            }
            cmd_buff->buff = malloc(cmd_buff->cap);
            if (cmd_buff->buff == NULL) {
                ESP_LOGE(TAG, "at cmd buffer mem alloc failed!!!");
                __data_unlock();
                vTaskDelay(portMAX_DELAY);
            }
            ESP_LOGI(TAG, "at cmd buffer grow to size %d", cmd_buff->cap);
        }

   

        // copy msg into buffer
        int wr_len = MIN(ble_msg.size, (cmd_buff->cap - cmd_buff->wr_ptr - 1/* \0 */));
        memcpy(cmd_buff->buff + cmd_buff->wr_ptr, ble_msg.msg, wr_len);
        cmd_buff->wr_ptr += wr_len;
        cmd_buff->buff[cmd_buff->wr_ptr] = '\0';
        __data_unlock();



        ESP_LOGD(TAG, "at cmd buffer write_pointer=%d", cmd_buff->wr_ptr);



        // if this msg is the last slice ( ending with "\r\n")
        if (!strstr((char *)ble_msg.msg, "\r\n")) {  //app_ble.c ensures there's null-terminator in ble_msg.msg
        // if(!memmem(ble_msg.msg, ble_msg.size, "\\r\\n", 2)){
            printf("BLE message as string: %s\n", (char *)ble_msg.msg);
            free(ble_msg.msg);
            ESP_LOGI(TAG,"free continue OK"); 
            continue;
        }
        free(ble_msg.msg);

        ESP_LOGI(TAG,"free OK"); 

        ESP_LOGI(TAG, "at cmd buffer recv the \\r\\n, process the buffer...");
        cmd_buff->wr_ptr = 0;
        char *test_strings = (char *)cmd_buff->buff;
        // const char *test_strings = "AT+deviceinfo?\r\n";
        // const char *test_strings = "AT+devicecfg={\"name\":\"devicecfg\",\"data\":{\"baud_rate\":115200,\"slave_id\":1,\"Current_Model\":2,\"Confidence\":0.9,\"esp32softwareversion\":\"1.0\",\"himaxsoftwareversion\":\"1.0\"}}\r\n";
        // const char *test_strings = "AT+classconfidence={"
        //                    "\"name\":\"classconfidence\","
        //                    "\"data\":["
        //                    "    {\"class\":\"0\",\"conf\":95},"
        //                    "    {\"class\":\"1\",\"conf\":95}"
        //                    "]"
        //                    "}\r\n";

        ESP_LOGD(TAG, "%s", test_strings);
        
        regex_t regex;
        int ret;
        ret = regcomp(&regex, pattern, REG_EXTENDED);
        if (ret)
        {
            ESP_LOGI(TAG, "Could not compile regex");
            cJSON *root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "name", "compile_regex_failed");
            cJSON_AddNumberToObject(root, "code", ERROR_CMD_REGEX_FAIL);
            char *json_string = cJSON_Print(root);
            ESP_LOGD(TAG, "JSON String: %s\n", json_string);
            send_at_response(json_string);
            cJSON_Delete(root);
            free(json_string);
            continue;
        }
        regmatch_t matches[4];
        ret = regexec(&regex, test_strings, 4, matches, 0);
        if (!ret)
        {
            // ESP_LOGI("recv_in match: %.*s\n", test_strings);
            char command_type[20];
            snprintf(command_type, sizeof(command_type), "%.*s", (int)(matches[1].rm_eo - matches[1].rm_so), test_strings + matches[1].rm_so);

            char *params = test_strings;
            if (matches[3].rm_so != -1)
            {
                int length = (int)(matches[3].rm_eo - matches[3].rm_so);
                params = test_strings + matches[3].rm_so;
                ESP_LOGD(TAG, "Matched string: %.50s... (total length: %d)\n", params, length);
            }
            char query_type = test_strings[matches[1].rm_eo] == '?' ? '?' : '=';
            // ESP_LOGD(TAG, "regex, command_type=%s, params=%s, query_type=%c", command_type, params, query_type);
            exec_command(&commands, command_type, params, query_type);
        }
        else if (ret == REG_NOMATCH)
        {
            ESP_LOGE(TAG, "No match: %s\n", test_strings);
            cJSON *root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "name", "Nomatch");
            cJSON_AddNumberToObject(root, "code", ERROR_CMD_FORMAT);
            char *json_string = cJSON_Print(root);
            ESP_LOGD(TAG, "JSON String: %s\n", json_string);
            send_at_response(json_string);
            cJSON_Delete(root);
            free(json_string);
        }
        else
        {
            char errbuf[100];
            regerror(ret, &regex, errbuf, sizeof(errbuf));
            ESP_LOGE(TAG, "Regex match failed: %s\n", errbuf);
            cJSON *root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "name", "RegexError");
            cJSON_AddNumberToObject(root, "code", ERROR_CMD_REGEX_FAIL);
            char *json_string = cJSON_Print(root);
            ESP_LOGD(TAG, "JSON String: %s\n", json_string);
            send_at_response(json_string);
            cJSON_Delete(root);
            free(json_string);
        }
        regfree(&regex);
    }
}


/**
 * @brief Initializes the AT command handling system.
 *
 * This function sets up the necessary components for handling AT commands, including creating the response queue,
 * initializing semaphores, and initializing tasks and WiFi stacks.
 */
void app_at_cmd_init()
{
#if CONFIG_ENABLE_FACTORY_FW_DEBUG_LOG
    esp_log_level_set(TAG, ESP_LOG_DEBUG);
#endif

    data_sem_handle = xSemaphoreCreateMutex();


    g_at_cmd_buffer.cap = AT_CMD_BUFFER_LEN_STEP;
    g_at_cmd_buffer.wr_ptr = 0;
    g_at_cmd_buffer.buff = malloc(g_at_cmd_buffer.cap);

    AT_command_reg();

    // init at cmd msg Q
    ble_msg_queue = xQueueCreate(BLE_MSG_Q_SIZE, sizeof(ble_msg_t));

    // init at cmd processing task
    const uint32_t stack_size = 6 * 1024;
    StackType_t *task_stack1 = (StackType_t *)malloc(stack_size * sizeof(StackType_t));
    StaticTask_t *task_tcb1 = heap_caps_calloc(1, sizeof(StaticTask_t), MALLOC_CAP_INTERNAL);
    xTaskCreateStatic(__at_cmd_proc_task, "at_cmd", stack_size, NULL, 9, task_stack1, task_tcb1);

}

