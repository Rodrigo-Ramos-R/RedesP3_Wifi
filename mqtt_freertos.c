/*
 * Copyright (c) 2016, Freescale Semiconductor, Inc.
 * Copyright 2016-2022 NXP
 * All rights reserved.
 *
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*******************************************************************************
 * Includes
 ******************************************************************************/
#include "mqtt_freertos.h"
#include <stdio.h>
#include "timers.h"
#include "task.h"
#include "FreeRTOS.h"

#include "board.h"
#include "fsl_silicon_id.h"

#include "lwip/opt.h"
#include "lwip/api.h"
#include "lwip/apps/mqtt.h"
#include "lwip/tcpip.h"

// FIXME cleanup

/*******************************************************************************
 * Definitions
 ******************************************************************************/

/*! @brief MQTT server host name or IP address. */
#ifndef EXAMPLE_MQTT_SERVER_HOST
#define EXAMPLE_MQTT_SERVER_HOST "broker.hivemq.com"
#endif

/*! @brief MQTT server port number. */
#ifndef EXAMPLE_MQTT_SERVER_PORT
#define EXAMPLE_MQTT_SERVER_PORT 1883
#endif

/*! @brief Stack size of the temporary lwIP initialization thread. */
#define INIT_THREAD_STACKSIZE 1024

/*! @brief Priority of the temporary lwIP initialization thread. */
#define INIT_THREAD_PRIO DEFAULT_THREAD_PRIO

/*! @brief Stack size of the temporary initialization thread. */
#define APP_THREAD_STACKSIZE 1024

/*! @brief Priority of the temporary initialization thread. */
#define APP_THREAD_PRIO DEFAULT_THREAD_PRIO

#define Publish_priority 	4
#define Subscribe_priority 	3

#define GAS_TRESH 	0
#define PART_TRESH 	1
#define GAS_VALUE 	2
#define PART_VALUE 	3

#define GAS_DEFAULT 		10
#define PART_DEFAULT 		10

#define TOTAL_TOPICS 4
#define MAX_RETRIES 3

#define INIT_VAL 1

/*******************************************************************************
 * Prototypes
 ******************************************************************************/

static void connect_to_mqtt(void *ctx);

void vThread_Publish_state(void * pvParameters);

void vThread_Publish_funcionality(void * pvParameters);

static void mqtt_message_published_cb(void *arg, err_t err);




/*******************************************************************************
 * Variables
 ******************************************************************************/
uint8_t Gas_Treshold = GAS_DEFAULT;

uint8_t Particles_Treshold = GAS_DEFAULT;

uint8_t Gas_Values = INIT_VAL;

uint8_t Part_Values = INIT_VAL;

TimerHandle_t xTimer;

TimerHandle_t xTimer1;

volatile uint8_t change_status = 1;

volatile uint8_t change_state = 1;

/*! @brief MQTT client data. */
static mqtt_client_t *mqtt_client;

/*! @brief MQTT client ID string. */
static char client_id[(SILICONID_MAX_LENGTH * 2) + 5];

/*! @brief MQTT client information. */
static const struct mqtt_connect_client_info_t mqtt_client_info = {
    .client_id   = (const char *)&client_id[0],
    .client_user = NULL,
    .client_pass = NULL,
    .keep_alive  = 100,
    .will_topic  = NULL,
    .will_msg    = NULL,
    .will_qos    = 0,
    .will_retain = 0,
#if LWIP_ALTCP && LWIP_ALTCP_TLS
    .tls_config = NULL,
#endif
};

/*! @brief MQTT broker IP address. */
static ip_addr_t mqtt_addr;

/*! @brief Indicates connection to MQTT broker. */
static volatile bool connected = false;

typedef struct {
    const char *topic;
    int qos;
    int retries;
    bool subscribed;
} mqtt_topic_t;

static mqtt_topic_t topic_list[TOTAL_TOPICS] = {
    {"Air_Filtering/GasThresh", 0, 0, false},
    {"Air_Filtering/PartThresh", 0, 0, false},
    {"Air_Filtering/GasValue", 0, 0, false},
    {"Air_Filtering/PartValue", 0, 0, false}
};


/*******************************************************************************
 * Code
 ******************************************************************************/

/*!
 * @brief Called when subscription request finishes.
 */
static void mqtt_topic_subscribed_cb(void *arg, err_t err)
{
    int topic_index = (int)(uintptr_t)arg;

    if (topic_index < 0 || topic_index >= TOTAL_TOPICS)
        return;

    mqtt_topic_t *topic = &topic_list[topic_index];

    if (err == ERR_OK)
    {
        topic->subscribed = true;
        PRINTF("Subscribed to the topic \"%s\".\r\n", topic->topic);
    }
    else
    {
        topic->retries++;
        PRINTF("Failed to subscribe to the topic \"%s\": %d (retry %d)\r\n",
               topic->topic, err, topic->retries);
    }

    // Check if all subscriptions are successful
    bool all_subscribed = true;
    for (int i = 0; i < TOTAL_TOPICS; i++)
    {
        if (!topic_list[i].subscribed)
        {
            all_subscribed = false;
            break;
        }
    }

    // Create publisher threads only once
    static bool publisher_threads_created = false;
    if (all_subscribed && !publisher_threads_created)
    {
        PRINTF("All topics subscribed successfully. Creating publisher threads...\r\n");

        if (xTaskCreate(vThread_Publish_state, "Gases", 1000, NULL, Publish_priority, NULL) != pdPASS)
        {
            PRINTF("Error: No se pudo crear la tarea vThread_Publish_state.\r\n");
        }

        if (xTaskCreate(vThread_Publish_funcionality, "Particles", 1000, NULL, Publish_priority, NULL) != pdPASS)
        {
            PRINTF("Error: No se pudo crear la tarea vThread_Publish_funcionality.\r\n");
        }

        publisher_threads_created = true;
    }
}



uint8_t Topic_id = 0;

/*!
 * @brief Called when there is a message on a subscribed topic.
 */
static void mqtt_incoming_publish_cb(void *arg, const char *topic, u32_t tot_len)
{
    LWIP_UNUSED_ARG(arg);

    PRINTF("Received %u bytes from the topic \"%s\": \"", tot_len, topic);

    if(0 == strcmp(topic, "Air_Filtering/GasThresh")){
    	Topic_id = GAS_TRESH;
    } else if(0 == strcmp(topic, "Air_Filtering/PartThresh")){
    	Topic_id = PART_TRESH;
    }else if(0 == strcmp(topic, "Air_Filtering/GasValue")){
    	Topic_id = GAS_VALUE;
    }else if(0 == strcmp(topic, "Air_Filtering/PartValue")){
    	Topic_id = PART_VALUE;
    }
}


/*!
 * @brief Called when received incoming published message fragment.
 */
static void mqtt_incoming_data_cb(void *arg, const u8_t *data, u16_t len, u8_t flags)
{
    int i;
    uint8_t System_id = 0;

    LWIP_UNUSED_ARG(arg);

    for (i = 0; i < len; i++)
    {
        if (isprint(data[i]))
        {
            PRINTF("%c", (char)data[i]);
        }
        else
        {
            PRINTF("\\x%02x", data[i]);
        }
    }

    switch(Topic_id){
    case 0:
			Gas_Treshold = (uint8_t)data[0] - 48;
			Gas_Treshold = Gas_Treshold*10 + (uint8_t)data[1] - 48;
			PRINTF("Gas Treshhold actualizado: %d\r\n", Gas_Treshold);
    	break;
    case 1:
    		Particles_Treshold = (uint8_t)data[0] - 48;
    		Particles_Treshold = Particles_Treshold*10 +(uint8_t)data[1] - 48;
    		PRINTF("Particles Treshhold actualizado: %d\r\n", Particles_Treshold);
    	break;
    case 2:
    		Gas_Values = ((data[0] - 48)*10);
    		Gas_Values = Gas_Values + (data[1] - 48);

        	if( Gas_Values >= Gas_Treshold){
            	xTimerStart(xTimer, 0);
        		PRINTF("Se ha prendido el Purificador\r\n");
        		change_state = 1;
        	}

    	break;
    case 3:
    	    Part_Values = ((data[0] - 48)*10);
    	    Part_Values = Part_Values + (data[1] - 48);

        	xTimerStart(xTimer1, 0);
        	if( Particles_Treshold >= 30 && Part_Values >= Particles_Treshold){
        		PRINTF("Purificador en Velocidad Alta\r\n");
        		change_status = 3;
        	}else if(Particles_Treshold >=  20 && Part_Values >= Particles_Treshold){
        		PRINTF("Purificador en Velocidad Media\r\n");
        		change_status = 2;
        	}

		break;
    default:

    		PRINTF("ID no valido %d\r\n");
    	break;

    }

    if (flags & MQTT_DATA_FLAG_LAST)
    {
        PRINTF("\"\r\n");
    }
}

/*!
 * @brief Subscribe to MQTT topics.
 */
static void mqtt_subscribe_topics(mqtt_client_t *client)
{
    mqtt_set_inpub_callback(client, mqtt_incoming_publish_cb, mqtt_incoming_data_cb,
                            LWIP_CONST_CAST(void *, &mqtt_client_info));

    for (int i = 0; i < TOTAL_TOPICS; i++)
    {
        if (!topic_list[i].subscribed && topic_list[i].retries < MAX_RETRIES)
        {
            err_t err = mqtt_subscribe(client,
                                       topic_list[i].topic,
                                       topic_list[i].qos,
                                       mqtt_topic_subscribed_cb,
                                       (void *)(uintptr_t)i);

            if (err == ERR_OK)
            {
                PRINTF("Subscribing to the topic \"%s\" with QoS %d...\r\n",
                       topic_list[i].topic, topic_list[i].qos);
            }
            else
            {
                topic_list[i].retries++;
                PRINTF("Immediate subscribe error for topic \"%s\": %d (retry %d)\r\n",
                       topic_list[i].topic, err, topic_list[i].retries);
            }

            sys_msleep(100); // Small delay between subscriptions
        }
    }

    // Schedule retry if not all subscribed
    bool needs_retry = false;
    for (int i = 0; i < TOTAL_TOPICS; i++)
    {
        if (!topic_list[i].subscribed && topic_list[i].retries < MAX_RETRIES)
        {
            needs_retry = true;
            break;
        }
    }

    if (needs_retry)
    {
        sys_timeout(1000, (sys_timeout_handler)mqtt_subscribe_topics, client);
    }
}

void vTimerCallback(TimerHandle_t ARHandle)
{
    xTimerStop(xTimer, portMAX_DELAY);
    PRINTF("Purificador Apagandose\r\n");
	SDK_DelayAtLeastUs(1000000, CLOCK_GetFreq(kCLOCK_CoreSysClk));
    PRINTF("Purificador Apagado\r\n");
	change_state = 1;
}

void vTimerCallback1(TimerHandle_t ARHandle)
{
    xTimerStop(xTimer1, portMAX_DELAY);
    PRINTF("Purificador en Velocidad Estandar\r\n");
	change_status = 1;

}

/*!
 * @brief Called when connection state changes.
 */
static void mqtt_connection_cb(mqtt_client_t *client, void *arg, mqtt_connection_status_t status)
{
    const struct mqtt_connect_client_info_t *client_info = (const struct mqtt_connect_client_info_t *)arg;

    connected = (status == MQTT_CONNECT_ACCEPTED);

    switch (status)
    {
        case MQTT_CONNECT_ACCEPTED:
            PRINTF("MQTT client \"%s\" connected.\r\n", client_info->client_id);
            mqtt_subscribe_topics(client);
            break;

        case MQTT_CONNECT_DISCONNECTED:
            PRINTF("MQTT client \"%s\" not connected.\r\n", client_info->client_id);
            /* Try to reconnect 1 second later */
            sys_timeout(1000, connect_to_mqtt, NULL);
            break;

        case MQTT_CONNECT_TIMEOUT:
            PRINTF("MQTT client \"%s\" connection timeout.\r\n", client_info->client_id);
            /* Try again 1 second later */
            sys_timeout(1000, connect_to_mqtt, NULL);
            break;

        case MQTT_CONNECT_REFUSED_PROTOCOL_VERSION:
        case MQTT_CONNECT_REFUSED_IDENTIFIER:
        case MQTT_CONNECT_REFUSED_SERVER:
        case MQTT_CONNECT_REFUSED_USERNAME_PASS:
        case MQTT_CONNECT_REFUSED_NOT_AUTHORIZED_:
            PRINTF("MQTT client \"%s\" connection refused: %d.\r\n", client_info->client_id, (int)status);
            /* Try again 10 seconds later */
            sys_timeout(10000, connect_to_mqtt, NULL);
            break;

        default:
            PRINTF("MQTT client \"%s\" connection status: %d.\r\n", client_info->client_id, (int)status);
            /* Try again 10 seconds later */
            sys_timeout(10000, connect_to_mqtt, NULL);
            break;
    }
}

/*!
 * @brief Starts connecting to MQTT broker. To be called on tcpip_thread.
 */
static void connect_to_mqtt(void *ctx)
{
    LWIP_UNUSED_ARG(ctx);

    PRINTF("Connecting to MQTT broker at %s...\r\n", ipaddr_ntoa(&mqtt_addr));

    mqtt_client_connect(mqtt_client, &mqtt_addr, EXAMPLE_MQTT_SERVER_PORT, mqtt_connection_cb,
                        LWIP_CONST_CAST(void *, &mqtt_client_info), &mqtt_client_info);
}

/*!
 * @brief Called when publish request finishes.
 */
static void mqtt_message_published_cb(void *arg, err_t err)
{
    const char *topic = (const char *)arg;

    if (err == ERR_OK)
    {
        PRINTF("Published to the topic \"%s\".\r\n", topic);
    }
    else
    {
        PRINTF("Failed to publish to the topic \"%s\": %d.\r\n", topic, err);
    }
}



struct mqtt_publish_params {
    struct mqtt_client_t *client;
    const char *topic;
    const char *payload;
    u8_t qos;
    u8_t retain;
    mqtt_request_cb_t cb;
    void *arg;
};

static void mqtt_publish_callback(void *arg) {
    struct mqtt_publish_params *params = (struct mqtt_publish_params *)arg;

    mqtt_publish(params->client,
                 params->topic,
                 params->payload,
                 strlen(params->payload),
                 params->qos,
                 params->retain,
                 params->cb,
                 params->arg);

    // Free dynamically allocated memory
    free(params);
}


#include <string.h>
#include <stdlib.h>

char *my_strdup(const char *src) {
    if (!src) return NULL;
    size_t len = strlen(src) + 1;
    char *dst = malloc(len);
    if (dst) {
        memcpy(dst, src, len);
    }
    return dst;
}

void vThread_Publish_state(void *pvParameters) {
    char *Topic_State = "Air_Filtering/State";
    char *state_str;

    while (1) {
        // Determinar el string basado en el valor de change_state
        if (change_state == 0) {
            state_str = "on";
        } else if (change_state == 1) {
            state_str = "off";
        } else {
            state_str = "unknown";  // Por si acaso tiene un valor inválido
        }

        PRINTF("Estado del sistema: %s\r\n", state_str);

        struct mqtt_publish_params *params = malloc(sizeof(struct mqtt_publish_params));
        if (params) {
            params->client = mqtt_client;
            params->topic = Topic_State;
            params->payload = my_strdup(state_str);  // Copia dinámica del string
            params->qos = 1;
            params->retain = 0;
            params->cb = mqtt_message_published_cb;
            params->arg = (void *)Topic_State;

            tcpip_callback(mqtt_publish_callback, params);
        }

        sys_msleep(3000U); // Delay de 6 segundos
    }
}

void vThread_Publish_funcionality(void * pvParameters){
    char *Topic_State = "Air_Filtering/Speed";
    char *state_str;

    while (1) {
        // Determinar el string basado en el valor de change_state
        switch (change_status)
        {
            case 1:
            	state_str = "bajo";
                break;
            case 2:
            	state_str = "medio";
                break;
            case 3:
            	state_str = "alto";
                break;
            default:
            	state_str = "desconocido";  // Valor por defecto si change_status no es 1, 2 o 3
                break;
        }

        PRINTF("Velcoidad del sistema: %s\r\n", state_str);

        struct mqtt_publish_params *params = malloc(sizeof(struct mqtt_publish_params));
        if (params) {
            params->client = mqtt_client;
            params->topic = Topic_State;
            params->payload = my_strdup(state_str);  // Copia dinámica del string
            params->qos = 1;
            params->retain = 0;
            params->cb = mqtt_message_published_cb;
            params->arg = (void *)Topic_State;

            tcpip_callback(mqtt_publish_callback, params);
        }

        sys_msleep(3000U); // Delay de 6 segundos
    }
}

/*!
 * @brief Publishes a message. To be called on tcpip_thread.
 */
static void publish_message(void *ctx)
{
    static const char *topic   = "lwip_topic/100";
    static const char *message = "message from board";

    LWIP_UNUSED_ARG(ctx);

    PRINTF("Going to publish to the topic \"%s\"...\r\n", topic);

    mqtt_publish(mqtt_client, topic, message, strlen(message), 1, 0, mqtt_message_published_cb, (void *)topic);
}

/*!
 * @brief Application thread.
 */
static void app_thread(void *arg)
{
    struct netif *netif = (struct netif *)arg;
    err_t err;
    int i;

    PRINTF("\r\nIPv4 Address     : %s\r\n", ipaddr_ntoa(&netif->ip_addr));
    PRINTF("IPv4 Subnet mask : %s\r\n", ipaddr_ntoa(&netif->netmask));
    PRINTF("IPv4 Gateway     : %s\r\n\r\n", ipaddr_ntoa(&netif->gw));

    /*
     * Check if we have an IP address or host name string configured.
     * Could just call netconn_gethostbyname() on both IP address or host name,
     * but we want to print some info if goint to resolve it.
     */
    if (ipaddr_aton(EXAMPLE_MQTT_SERVER_HOST, &mqtt_addr) && IP_IS_V4(&mqtt_addr))
    {
        /* Already an IP address */
        err = ERR_OK;
    }
    else
    {
        /* Resolve MQTT broker's host name to an IP address */
        PRINTF("Resolving \"%s\"...\r\n", EXAMPLE_MQTT_SERVER_HOST);
        err = netconn_gethostbyname(EXAMPLE_MQTT_SERVER_HOST, &mqtt_addr);
    }

    if (err == ERR_OK)
    {
        /* Start connecting to MQTT broker from tcpip_thread */
        err = tcpip_callback(connect_to_mqtt, NULL);
        if (err != ERR_OK)
        {
            PRINTF("Failed to invoke broker connection on the tcpip_thread: %d.\r\n", err);
        }
    }
    else
    {
        PRINTF("Failed to obtain IP address: %d.\r\n", err);
    }

    vTaskDelete(NULL);
}

static void generate_client_id(void)
{
    uint8_t silicon_id[SILICONID_MAX_LENGTH];
    const char *hex = "0123456789abcdef";
    status_t status;
    uint32_t id_len = sizeof(silicon_id);
    int idx         = 0;
    int i;
    bool id_is_zero = true;

    /* Get unique ID of SoC */
    status = SILICONID_GetID(&silicon_id[0], &id_len);
    assert(status == kStatus_Success);
    assert(id_len > 0U);
    (void)status;

    /* Covert unique ID to client ID string in form: nxp_hex-unique-id */

    /* Check if client_id can accomodate prefix, id and terminator */
    assert(sizeof(client_id) >= (5U + (2U * id_len)));

    /* Fill in prefix */
    client_id[idx++] = 'n';
    client_id[idx++] = 'x';
    client_id[idx++] = 'p';
    client_id[idx++] = '_';

    /* Append unique ID */
    for (i = (int)id_len - 1; i >= 0; i--)
    {
        uint8_t value    = silicon_id[i];
        client_id[idx++] = hex[value >> 4];
        client_id[idx++] = hex[value & 0xFU];

        if (value != 0)
        {
            id_is_zero = false;
        }
    }

    /* Terminate string */
    client_id[idx] = '\0';

    if (id_is_zero)
    {
        PRINTF(
            "WARNING: MQTT client id is zero. (%s)"
#ifdef OCOTP
            " This might be caused by blank OTP memory."
#endif
            "\r\n",
            client_id);
    }
}

/*!
 * @brief Create and run example thread
 *
 * @param netif  netif which example should use
 */
void mqtt_freertos_run_thread(struct netif *netif)
{
    LOCK_TCPIP_CORE();
    mqtt_client = mqtt_client_new();
    UNLOCK_TCPIP_CORE();
    if (mqtt_client == NULL)
    {
        PRINTF("mqtt_client_new() failed.\r\n");
        while (1)
        {
        }
    }

    xTimer = xTimerCreate("Timer5s",              // Nombre
                              pdMS_TO_TICKS(4000),    // Periodo en ticks (5000 ms)
                              pdTRUE,                 // Auto-reload (pdTRUE = repetitivo)
                              (void *)0,              // ID del timer (opcional)
                              vTimerCallback);        // Función callback

    xTimer1 = xTimerCreate("Timer3s",              // Nombre
                              pdMS_TO_TICKS(3000),    // Periodo en ticks (5000 ms)
                              pdTRUE,                 // Auto-reload (pdTRUE = repetitivo)
                              (void *)0,              // ID del timer (opcional)
                              vTimerCallback1);        // Función callback

    generate_client_id();

    // Esperar hasta tener IP válida
    while (!netif_is_up(netif_default) || !dhcp_supplied_address(netif_default)) {
        vTaskDelay(pdMS_TO_TICKS(500));
    }


    if (sys_thread_new("app_task", app_thread, netif, APP_THREAD_STACKSIZE, APP_THREAD_PRIO) == NULL)
    {
        LWIP_ASSERT("mqtt_freertos_start_thread(): Task creation failed.", 0);
    }

    vTaskDelete(NULL);

}
