#ifndef _APP_HIMAX_
#define _APP_HIMAX
#include "bsp.h"
#include "time.h"

#define TF_MODULE_AI_CAMERA_SENSOR_RESOLUTION_240_240    0
#define TF_MODULE_AI_CAMERA_SENSOR_RESOLUTION_416_416    1
#define TF_MODULE_AI_CAMERA_SENSOR_RESOLUTION_480_480    2
#define TF_MODULE_AI_CAMERA_SENSOR_RESOLUTION_640_480    3

#define TF_MODULE_AI_CAMERA_MODES_INFERENCE  0
#define TF_MODULE_AI_CAMERA_MODES_SAMPLE     1

struct himax_data_buf
{
    uint8_t *p_buf;
    uint32_t len;
};

struct himax_data_image
{
    uint8_t *p_buf;  //base64 data
    uint32_t len;
    time_t   time;
};

enum himax_data_inference_type {
    INFERENCE_TYPE_UNKNOWN = 0,
    INFERENCE_TYPE_BOX,    //sscma_client_box_t
    INFERENCE_TYPE_CLASS,  //sscma_client_class_t
    INFERENCE_TYPE_POINT,  //sscma_client_point_t
    INFERENCE_TYPE_KEYPOINT     //sscma_client_keypoint_t
};

// classes max num
#define CONFIG_MODEL_CLASSES_MAX_NUM       20

struct himax_data_inference_info
{
    bool is_valid;
    enum himax_data_inference_type   type;
    void  *p_data;
    uint32_t cnt;
    // char *classes[CONFIG_MODEL_CLASSES_MAX_NUM];
};



struct himax_module_ai_camera_preview_info
{
    struct himax_data_image                      img;
    struct himax_data_inference_info inference;
};



void himax_sscam_init();

#endif