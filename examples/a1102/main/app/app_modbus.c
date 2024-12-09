#include "app_modbus.h"


holding_image_params modbus_image = {0};
a1102_holding_reg_params_t a1102_msg = {0};
int modbus_reset_flage = 0;
mb_param_info_t reg_info; // keeps the Modbus registers access information
mb_communication_info_t comm_info; // Modbus communication parameters
mb_register_area_descriptor_t reg_area; // Modbus register area descriptor structure
void* mbc_slave_handler = NULL;


TickType_t start_time;
const TickType_t max_hold_time = pdMS_TO_TICKS(2000);
const TickType_t img_max_hold_time = pdMS_TO_TICKS(15000);
bool in_read =false;
bool in_read_img = false;
bool restart_flage = false;

static const char *TAG = "app_modbus";

// Modbus Register content initialization
static void init_a1102_msg(uint16_t new_slava_id,uint16_t new_baudrate){
    

    memset(&modbus_image.image,0,sizeof(modbus_image.image));
    modbus_image.reg_length = 0;
    modbus_image.image_size = 0;
    portENTER_CRITICAL(&param_lock);
   
    mb_set_uint16_ab((val_16_arr *)&a1102_msg.slave_id, (uint16_t)new_slava_id);
    mb_set_uint16_ab(&a1102_msg.buatrate, (uint16_t)new_baudrate);
    mb_set_uint32_abcd((val_32_arr *)&a1102_msg.device_version, (uint32_t)1);
    mb_set_uint32_abcd((val_32_arr *)&a1102_msg.device_id, (uint32_t)1);
    mb_set_uint32_abcd((val_32_arr *)&a1102_msg.modle_id, (uint32_t)1);


    for (int i = 0; i < 10; i++) {
        mb_set_uint32_abcd((val_32_arr *)&a1102_msg.result[i], (uint32_t)-1000);
    }

    for (int i = 0; i < 9; i++) {
        mb_set_uint32_abcd((val_32_arr *)&a1102_msg.xy[i], (uint32_t)0);
        mb_set_uint32_abcd((val_32_arr *)&a1102_msg.wh[i], (uint32_t)0);
    }

    
    portEXIT_CRITICAL(&param_lock);

}






void set_up_modbus(uint16_t new_slava_id,uint16_t new_baudrate){
    if(modbus_reset_flage != 0){
        printf("reset modbus setting !!!\r\n");
        mbc_slave_destroy();
    }
    
    printf("start modbus setting !!!\r\n");
    // Set UART log level
    esp_log_level_set(TAG, ESP_LOG_INFO);
    

    
    ESP_ERROR_CHECK(mbc_slave_init(MB_PORT_SERIAL_SLAVE, &mbc_slave_handler)); // Initialization of Modbus controller
    printf("mbc_slave_init end !!!\r\n");
    // Setup communication parameters and start stack



    comm_info.mode = MB_MODE_RTU,
    comm_info.slave_addr = new_slava_id;
    comm_info.port = 0;
    if(new_baudrate == 115){
        comm_info.baudrate = 115200;
    }else{
        comm_info.baudrate = (uint32_t)new_baudrate;
    }
    // comm_info.baudrate = 115200;
    comm_info.parity = MB_PARITY_NONE;
    ESP_ERROR_CHECK(mbc_slave_setup((void*)&comm_info));
    printf("mbc_slave_setup end !!!\r\n");
    /*
        Start mapping register area：
            slave_id  -------------> 0
            buatrate  -------------> 1
            device_version --------> 2
            device_id -------------> 0X8000 ----> 32768
            modle_id --------------> 0X8002 ----> 32770
            result 0~9 ------------> 0x1000 ~ 0x1020 
            Coordinate information xy  -----> 0x2000 ~0x2000 + 9
            Coordinate information wh  -----> 0x3000 ~ 0x3000 + 9
            picture length：0X8006 
            picture: 0x8007 + 20480
    */

    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x0000; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.slave_id; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(a1102_msg.slave_id);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));
    
 
    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x0001; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.buatrate; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(uint32_t);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));

 
    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x0003; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.device_version; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(a1102_msg.device_version);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));


    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0X8000; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.device_id; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(a1102_msg.device_id);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));


 
    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0X8002; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.modle_id; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(a1102_msg.modle_id);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));

    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x1000; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.result; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(a1102_msg.result);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));

 
    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x2000; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.xy; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(a1102_msg.xy);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));

   
    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x3000; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&a1102_msg.wh; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(a1102_msg.wh);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));


  
    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x8006; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&modbus_image.reg_length; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(modbus_image.reg_length);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));


    reg_area.type = MB_PARAM_HOLDING; // Set type of register area
    reg_area.start_offset = 0x8007; // Offset of register area in Modbus protocol
    reg_area.address = (void*)&modbus_image.image; // Set pointer to storage instance
    // Set the size of register storage instance in bytes
    reg_area.size = sizeof(modbus_image.image);
    ESP_ERROR_CHECK(mbc_slave_set_descriptor(reg_area));

    printf("mbc_slave_set_descriptor end !!!\r\n");

    
    init_a1102_msg(new_slava_id,new_baudrate);
    printf("init_a1102_msg end !!!\r\n");
    // Starts of modbus controller and stack
    ESP_ERROR_CHECK(mbc_slave_start());

    printf("mbc_slave_start  !!!\r\n");

    // Set UART pin numbers
    ESP_ERROR_CHECK(uart_set_pin(0, 6,
                            7, 4,
                            UART_PIN_NO_CHANGE));

    printf("uart_set_pin end  !!!\r\n");

    // Set UART driver mode to Half Duplex
    ESP_ERROR_CHECK(uart_set_mode(0, UART_MODE_RS485_HALF_DUPLEX));

    printf("uart_set_mode end  !!!\r\n");

    ESP_LOGI(TAG, "Modbus slave stack initialized.");
    ESP_LOGI(TAG, "Start modbus test...");
    vTaskDelay(1000);
    restart_flage = false;

}


void modbus_task(void *pvParameter){
    set_up_modbus(1,115);



    while (1)
    {
        if(restart_flage != true)
            (void)mbc_slave_check_event(MB_READ_WRITE_MASK);

        if(restart_flage != true){
            
            if (ESP_OK == mbc_slave_get_param_info(&reg_info, MB_PAR_INFO_GET_TOUT)) {
                        const char* rw_str = (reg_info.type & MB_READ_MASK) ? "READ" : "WRITE";

                        // Filter events and process them accordingly
                        if(reg_info.type & (MB_EVENT_HOLDING_REG_WR | MB_EVENT_HOLDING_REG_RD)) {
                            // Get parameter information from parameter queue
                            ESP_LOGI(TAG, "HOLDING %s (%" PRIu32 " us), ADDR:%u, TYPE:%u, INST_ADDR:0x%" PRIx32 ", SIZE:%u",
                                            rw_str,
                                            reg_info.time_stamp,
                                            (unsigned)reg_info.mb_offset,
                                            (unsigned)reg_info.type,
                                            (uint32_t)reg_info.address,
                                            (unsigned)reg_info.size);

                            // Modify baud rate
                            if (reg_info.address == (uint8_t*)&a1102_msg.buatrate && reg_info.type &  MB_EVENT_HOLDING_REG_WR) {
                                ESP_LOGI(TAG,"change buatrate");
                                modbus_reset_flage = 1;
                                set_up_modbus(a1102_msg.slave_id,a1102_msg.buatrate);
                            }

                            // Modify slave id
                            if (reg_info.address == (uint8_t*)&a1102_msg.slave_id && reg_info.type &  MB_EVENT_HOLDING_REG_WR) {
                                ESP_LOGI(TAG,"change slave_id");
                                modbus_reset_flage = 1;
                                set_up_modbus(a1102_msg.slave_id,a1102_msg.buatrate);
                            }

                            // Transfer pictures
                            if (reg_info.mb_offset == 0x8002)
                            {
                                if (xSemaphoreTake(img_memory_lock, portMAX_DELAY) == pdTRUE){
                                    atomic_store(&img_memory_lock_owner, 2);
                                    start_time = xTaskGetTickCount();
                                    in_read = true;
                                    ESP_LOGI(TAG,"reg_info.mb_offset == 0x8002");

                                    portENTER_CRITICAL(&param_lock);
                                    for (int i = 0; i < 10; i++) {
                                        mb_set_uint32_abcd((val_32_arr *)&a1102_msg.result[i], (uint32_t)-1000);
                                    }

                                    for (int i = 0; i < 9; i++) {
                                        mb_set_uint32_abcd((val_32_arr *)&a1102_msg.xy[i], (uint32_t)0);
                                        mb_set_uint32_abcd((val_32_arr *)&a1102_msg.wh[i], (uint32_t)0);
                                    }

                                    modbus_image.image_size = 0;
                                    modbus_image.reg_length = 0;
                                    memset(modbus_image.image, '\0', sizeof(modbus_image.image));
                                    
                                    portEXIT_CRITICAL(&param_lock);

                                    img_get_cnt = 0;
                                    // sscma_client_write(client,"AT+INVOKE=1,0,0", 15);
                                    // sscma_client_invoke(client, -1, false, false);
                                    sscma_client_invoke(client, 1, 0, 1);
                                    vTaskDelay(100);
                                    img_get_cnt = 2;
                                    sscma_client_invoke(client, 1, 0, 1);
                                    printf("sscma_client send ok \r\n");
                                    vTaskDelay(100);
                                    sscma_client_invoke(client, -1, 0, 0);
                                }
                            }

                            if(reg_info.mb_offset ==  0x8006){
                                in_read_img = true;
                            }

                            if(in_read){
                                if(in_read_img){
                                    // printf("reg_info.mb_offset = %d \r\n",reg_info.mb_offset);
                                    // printf("32775 + modbus_image.reg_length = %d \r\n",32775 + modbus_image.reg_length);
                                    if((reg_info.mb_offset == 32775 + modbus_image.reg_length - modbus_image.reg_length % 125) || (xTaskGetTickCount() - start_time > img_max_hold_time)){
                                        atomic_store(&img_memory_lock_owner, 0);
                                        xSemaphoreGive(img_memory_lock);
                                        in_read_img = false;
                                        in_read = false;

                                        printf("in_read_img xSemaphoreGive(img_memory_lock) \r\n");
                                    }
                                }else{
                                    if((reg_info.mb_offset == 0x1000) || (xTaskGetTickCount() - start_time > max_hold_time)){
                                        atomic_store(&img_memory_lock_owner, 0);
                                        xSemaphoreGive(img_memory_lock);
                                        in_read_img = false;
                                        in_read = false;
                                        printf(" in_read xSemaphoreGive(img_memory_lock) \r\n");
                                    }
                                }

                            }


                        }

                    }
        }else{
            vTaskDelay(200);
        }
        
        ESP_LOGI(TAG,"Modbus while.");


    }
    
    // Destroy of Modbus controller on alarm
    ESP_LOGI(TAG,"Modbus controller destroyed.");
    vTaskDelay(100);
    ESP_ERROR_CHECK(mbc_slave_destroy());
}

void modbus_statu(void *pvParameter){
    while (1)
    {
        vTaskDelay(500);
        if(atomic_load(&img_memory_lock_owner) != 2){
            continue;
        }

        if(in_read && in_read_img && (xTaskGetTickCount() - start_time > img_max_hold_time)){
            atomic_store(&img_memory_lock_owner, 0);
            xSemaphoreGive(img_memory_lock);
            in_read_img = false;
            in_read = false;
            printf("modbus_statu xSemaphoreGive(img_memory_lock) \r\n");
        
        }

        if(in_read && !in_read_img && (xTaskGetTickCount() - start_time > max_hold_time)){
            atomic_store(&img_memory_lock_owner, 0);
            xSemaphoreGive(img_memory_lock);
            in_read = false;
            printf("modbus_statu in_read xSemaphoreGive(img_memory_lock) \r\n");
        }

    }
    
}
void app_modbus_init(){
    
    xTaskCreate(&modbus_task, "modbus_task", 1024 * 4, NULL, 5,NULL);
    xTaskCreate(modbus_statu,"modbus_statu", 1024, NULL, 4, NULL);
}



