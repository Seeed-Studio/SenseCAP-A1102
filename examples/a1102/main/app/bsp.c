#include "bsp.h"


sscma_client_io_handle_t io = NULL;
sscma_client_handle_t client = NULL;

portMUX_TYPE param_lock = portMUX_INITIALIZER_UNLOCKED;
esp_event_loop_handle_t change_param_event_loop;
QueueHandle_t ble_msg_queue;
ESP_EVENT_DEFINE_BASE(PARAM_CHANGE_EVENT_BASE);
SemaphoreHandle_t xSemaphore;
SemaphoreHandle_t img_memory_lock;
atomic_int img_memory_lock_owner = ATOMIC_VAR_INIT(0);// owner:  modbus or cmd  
bool model_change_flag =false;

int img_get_cnt = 0;
A1102_param g_a1102_param;

const char* model_to_string(modle_type model) {
    switch (model) {
    case Prefab_Models:
        return "Prefab_Models";
    case Human_Body_Detection:
        return "Human_Body_Detection";
    case Meter_Identification:
        return "Meter_Identification";
    case People_Counting:
        return "People_Counting";
    default:
        return "Unknown";
    }
}

void bsp_init(void){

    xSemaphore = xSemaphoreCreateBinary();
    xSemaphoreGive(xSemaphore);

    img_memory_lock = xSemaphoreCreateBinary();
    xSemaphoreGive(img_memory_lock);
    if (img_memory_lock == NULL) {
        printf("Failed to create memory lock.\n");
        return;
    }
}