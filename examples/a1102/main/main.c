#include "main.h"

void print_class_data(const ClassData *data) {
    printf("Class Count: %d\n", data->class_count);
    for (int i = 0; i < data->class_count; i++) {
        printf("Class: %s, Confidence: %d\n",
               data->classes[i].class_name,
               data->classes[i].confidence);
    }
}


static void param_change_event_handler(void *handler_arg, esp_event_base_t base, int32_t id, void *event_data) {
    if (base != PARAM_CHANGE_EVENT_BASE) return;

    param_change_event_data_t *data = (param_change_event_data_t *)event_data;

    switch (id) {
        case PARAM_CHANGE_MODBUS:
            ESP_LOGI("EVENT_HANDLER", "MODBUS Changed: New Baud Rate Value=%d  New Slave ID  Value=%d", data->new_baud_rate,data->new_slave_id);

            modbus_reset_flage = 1;
            restart_flage = true;
            set_up_modbus(data->new_slave_id,data->new_baud_rate);
            g_a1102_param.modbus_p.modbus_baud = data->new_baud_rate;
            g_a1102_param.modbus_p.modbus_address = data->new_slave_id;
            break;
        case PARAM_CHANGE_BAUD_RATE:
            ESP_LOGI("EVENT_HANDLER", "Baud Rate Changed: New Value=%d", data->new_baud_rate);

            modbus_reset_flage = 1;
            restart_flage = true;
            set_up_modbus(g_a1102_param.modbus_p.modbus_address,data->new_baud_rate);
            g_a1102_param.modbus_p.modbus_baud = data->new_baud_rate;
            
            break;
        case PARAM_CHANGE_SLAVE_ID:
            ESP_LOGI("EVENT_HANDLER", "Slave ID Changed: New Value=%d", data->new_slave_id);
            modbus_reset_flage = 1;
            restart_flage = true;
            set_up_modbus(data->new_slave_id,g_a1102_param.modbus_p.modbus_baud);
            g_a1102_param.modbus_p.modbus_address = data->new_slave_id;
            
            break;
        case PARAM_CHANGE_CONFIDENCE:
            
            ESP_LOGI(TAG,"class cnt is %d",data->class_data.class_count);
            print_class_data(&data->class_data);
            set_modle_confidence(data->class_data.class_count,&data->class_data,data->save_pic_flage);

            break;
        case PARAM_CHANGE_MODEL_TYPE:
            ESP_LOGI("EVENT_HANDLER", "Model Type Changed: New Value=%d", data->new_model_type);
            set_modle(data->new_model_type);
            g_a1102_param.modle_p.current_modle = data->new_model_type;
            break;
        case PARAM_PREVIEW:
            preview();
            break;
        case PARAM_RESET:
            if(g_a1102_param.modbus_p.modbus_baud != 115200 || g_a1102_param.modbus_p.modbus_address != 1){
                modbus_reset_flage = 1;
                restart_flage = true;
                set_up_modbus(1,115);
                g_a1102_param.modbus_p.modbus_baud = 115200;
                g_a1102_param.modbus_p.modbus_address = 1;
            }
            
            set_modle_confidence(0,NULL,0);
            set_modle(1);
            g_a1102_param.modle_p.current_modle = 1;
            break;
        default:
            ESP_LOGW("EVENT_HANDLER", "Unknown Event ID: %ld", id);
            break;
    }
}



extern int _data_start, _data_end;  // data
extern int _bss_start, _bss_end;    // BSS 
extern int _text_start, _text_end;  // code

void print_memory_usage() {

    size_t data_size = (size_t)&_data_end - (size_t)&_data_start;  
    size_t total_data_size = 1024; 
    float data_usage = (float)data_size / total_data_size * 100.0;

    size_t bss_size = (size_t)&_bss_end - (size_t)&_bss_start;   
    size_t total_bss_size = 1024;  
    float bss_usage = (float)bss_size / total_bss_size * 100.0;

    size_t text_size = (size_t)&_text_end - (size_t)&_text_start; 
    size_t total_text_size = 1024;  
    float text_usage = (float)text_size / total_text_size * 100.0;

    size_t total_heap = heap_caps_get_total_size(MALLOC_CAP_DEFAULT); 
    size_t free_heap = esp_get_free_heap_size();                      
    size_t used_heap = total_heap - free_heap;                         
    float heap_usage = (float)used_heap / total_heap * 100.0;

    ESP_LOGI("MEMORY", "================== Memory Usage ==================");
    ESP_LOGI("MEMORY", "Code (.text)    : %7.2f%% Used, %8zu B / %8zu B", text_usage, text_size, total_text_size);
    ESP_LOGI("MEMORY", "Data (.data)    : %7.2f%% Used, %8zu B / %8zu B", data_usage, data_size, total_data_size);
    ESP_LOGI("MEMORY", "BSS  (.bss)     : %7.2f%% Used, %8zu B / %8zu B", bss_usage, bss_size, total_bss_size);
    ESP_LOGI("MEMORY", "Heap            : %7.2f%% Used, %8zu B / %8zu B", heap_usage, used_heap, total_heap);
    ESP_LOGI("MEMORY", "==================================================");
}



void app_main(void)
{
    bsp_init();
    app_nvs_init();

    esp_event_loop_args_t loop_args = {
        .queue_size = 10,               
        .task_name = "change_param_event_task",
        .task_priority = uxTaskPriorityGet(NULL) + 1, 
        .task_stack_size = 2048,     
        .task_core_id = tskNO_AFFINITY, 
    };

    
    esp_event_loop_create(&loop_args, &change_param_event_loop);
    ESP_ERROR_CHECK(esp_event_handler_register_with(change_param_event_loop, PARAM_CHANGE_EVENT_BASE, ESP_EVENT_ANY_ID, param_change_event_handler, NULL));
    

    app_at_cmd_init();
    app_ble_init();
    app_modbus_init();
    himax_sscam_init();
    while (1)
    {
        // print_memory_usage();
        // ESP_LOGI(TAG, "wait !!");
        vTaskDelay(pdMS_TO_TICKS(2000));

    }
    
   
}
