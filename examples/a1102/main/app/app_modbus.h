#ifndef __APP_MODBUS_H
#define __APP_MODBUS_H



#include "bsp.h"

#define MB_PAR_INFO_GET_TOUT (10) // Timeout for get parameter info
#define MB_READ_MASK         (MB_EVENT_INPUT_REG_RD | MB_EVENT_HOLDING_REG_RD | MB_EVENT_DISCRETE_RD | MB_EVENT_COILS_RD)
#define MB_WRITE_MASK        (MB_EVENT_HOLDING_REG_WR | MB_EVENT_COILS_WR)
#define MB_READ_WRITE_MASK   (MB_READ_MASK | MB_WRITE_MASK)

extern int modbus_reset_flage;
extern bool restart_flage;

typedef struct{
    uint16_t slave_id; // slave address  RW
    uint16_t buatrate; //baud rate   RW
    uint32_t device_version; //Device version  R
    uint32_t device_id; // Device ID  R
    uint32_t modle_id;//Model ID  R
    uint32_t result[10];//result   R
    uint32_t xy[9];
    uint32_t wh[9];
    

}a1102_holding_reg_params_t;


//Currently using fixed Memory to store images
typedef struct 
{
    uint16_t reg_length;
    uint16_t image_size;
    uint16_t image[20480]
   
}holding_image_params;





void app_modbus_init();
void set_up_modbus(uint16_t new_slava_id,uint16_t new_baudrate);
#endif