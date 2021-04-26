/*
 * Copyright (c) 2020, Pycom Limited.
 *
 * This software is licensed under the GNU GPL version 3 or any
 * later version, with permitted additional terms. For more information
 * see the Pycom Licence v1.0 document supplied with this file, or
 * available at https://www.pycom.io/opensource/licensing
 */
/*
 / _____)             _              | |
( (____  _____ ____ _| |_ _____  ____| |__
 \____ \| ___ |    (_   _) ___ |/ ___)  _ \
 _____) ) ____| | | || |_| ____( (___| | | |
(______/|_____)_|_|_| \__)_____)\____)_| |_|
  (C)2013 Semtech-Cycleo


 * Modified by Rupert Smith (rhs1g18@soton.ac.uk) to enable the gateway to
 * be used in peer to peer LoRa networks. The Packet forwarder is modified
 * to provide a simplified UDP protcol which allows LoRa packets to be transmitted
 * using the loopback interface provided the WLAN module is initialised.


Description:
    Configure Lora concentrator and forward packets to a server

License: Revised BSD License, see LICENSE.TXT file include in the project
*/


/* -------------------------------------------------------------------------- */
/* --- DEPENDANCIES --------------------------------------------------------- */

/* fix an issue between POSIX and C99 */
#if __STDC_VERSION__ >= 199901L
#define _XOPEN_SOURCE 600
#else
#define _XOPEN_SOURCE 500
#endif

#include <stdint.h>         /* C99 types */
#include <stdbool.h>        /* bool type */
#include <stdio.h>          /* printf, fprintf, snprintf, fopen, fputs */

#include <string.h>         /* memset */
#include <signal.h>         /* sigaction */
#include <time.h>           /* time, clock_gettime, strftime, gmtime */
#include <sys/time.h>       /* timeval */
#include <unistd.h>         /* getopt, access */
#include <stdlib.h>         /* atoi, exit */
#include <errno.h>          /* error messages */
#include <math.h>           /* modf */
#include <assert.h>

#include <sys/socket.h>     /* socket specific definitions */
#include <netinet/in.h>     /* INET constants and stuff */
#include <arpa/inet.h>      /* IP address conversion stuff */
#include <netdb.h>          /* gai_strerror */

#include <pthread.h>

#include "trace.h"
#include "jitqueue.h"
#include "timersync.h"
#include "parson.h"
#include "base64.h"
#include "loragw_hal.h"
#include "loragw_reg.h"
#include "loragw_aux.h"
#include "esp_pthread.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_attr.h"

#include "lwip/err.h"
#include "lwip/apps/sntp.h"
#include "utils/interrupt_char.h"
#include "py/obj.h"
#include "py/mpprint.h"
#include "modmachine.h"
#include "machpin.h"
#include "pins.h"
#include "sx1308-config.h"

#include "udp_pkt_fwd.h" /* UDP packet structure */

/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

#define ARRAY_SIZE(a)   (sizeof(a) / sizeof((a)[0]))
#define STRINGIFY(x)    #x
#define STR(x)          STRINGIFY(x)
#define exit(x)         loragw_exit(x)
/* -------------------------------------------------------------------------- */
/* --- PRIVATE CONSTANTS ---------------------------------------------------- */

#ifndef VERSION_STRING
#define VERSION_STRING "undefined"
#endif

#define DEFAULT_SERVER      127.0.0.1   /* hostname also supported */
#define DEFAULT_PORT_UP     1780
#define DEFAULT_PORT_DW     1782
#define DEFAULT_KEEPALIVE   5           /* default time interval for downstream keep-alive packet */
#define DEFAULT_STAT        30          /* default time interval for statistics */
#define PUSH_TIMEOUT_MS     100
#define PULL_TIMEOUT_MS     200
#define FETCH_SLEEP_MS      50          /* nb of ms waited when a fetch return no packets */

#define PROTOCOL_VERSION    2           /* v1.3 */

#define PKT_PUSH_DATA   0
#define PKT_PUSH_ACK    1
#define PKT_PULL_DATA   2
#define PKT_PULL_RESP   3
#define PKT_PULL_ACK    4
#define PKT_TX_ACK      5

#define NB_PKT_MAX      2 /* max number of packets per fetch/send cycle */

#define MIN_LORA_PREAMB 6 /* minimum Lora preamble length for this application */
#define STD_LORA_PREAMB 8
#define MIN_FSK_PREAMB  3 /* minimum FSK preamble length for this application */
#define STD_FSK_PREAMB  5

#define STATUS_SIZE     200
#define TX_BUFF_SIZE    ((540 * NB_PKT_MAX) + 30 + STATUS_SIZE)

#define NI_NUMERICHOST	1	/* return the host address, not the name */

#define COM_PATH_DEFAULT "/dev/ttyACM0"

#define LORA_GW_STACK_SIZE                                             (15000)
#define LORA_GW_PRIORITY                                               (10)

#define LORA_PACKET 0x01
#define TX_INFO_PACKET 0x02

TaskHandle_t xLoraGwTaskHndl;
typedef void (*_sig_func_cb_ptr)(int);

void TASK_lora_gw(void *pvParameters);
void mp_hal_set_signal_exit_cb (_sig_func_cb_ptr fun);
bool mach_is_rtc_synced (void);
/* -------------------------------------------------------------------------- */
/* --- PRIVATE TYPES -------------------------------------------------------- */

/**
@struct coord_s
@brief Geodesic coordinates
*/
struct coord_s {
    double  lat;    /*!> latitude [-90,90] (North +, South -) */
    double  lon;    /*!> longitude [-180,180] (East +, West -)*/
    short   alt;    /*!> altitude in meters (WGS 84 geoid ref.) */
};

/* -------------------------------------------------------------------------- */
/* --- PRIVATE VARIABLES (GLOBAL) ------------------------------------------- */

/* signal handling variables */
volatile DRAM_ATTR bool exit_sig = false; /* 1 -> application terminates cleanly (shut down hardware, close open files, etc) */
volatile DRAM_ATTR bool quit_sig = false; /* 1 -> application terminates without shutting down the hardware */

/* packets filtering configuration variables */
static bool fwd_valid_pkt = true; /* packets with PAYLOAD CRC OK are forwarded */
static bool fwd_error_pkt = false; /* packets with PAYLOAD CRC ERROR are NOT forwarded */
static bool fwd_nocrc_pkt = false; /* packets with NO PAYLOAD CRC are NOT forwarded */

/* network configuration variables */
static uint64_t lgwm = 0; /* Lora gateway MAC address */
static char serv_addr[64] = STR(DEFAULT_SERVER); /* address of the server (host name or IPv4/IPv6) */
static char serv_port_up[8] = STR(DEFAULT_PORT_UP); /* server port for upstream traffic */
static char serv_port_down[8] = STR(DEFAULT_PORT_DW); /* server port for downstream traffic */
static int keepalive_time = DEFAULT_KEEPALIVE; /* send a PULL_DATA request every X seconds, negative = disabled */

/* statistics collection configuration variables */
static unsigned stat_interval = DEFAULT_STAT; /* time interval (in sec) at which statistics are collected and displayed */

/* gateway <-> MAC protocol variables */
static uint32_t net_mac_h; /* Most Significant Nibble, network order */
static uint32_t net_mac_l; /* Least Significant Nibble, network order */

/* network protocol variables */
static struct timeval push_timeout_half = {0, (PUSH_TIMEOUT_MS * 500)}; /* cut in half, critical for throughput */
static struct timeval pull_timeout = {0, (PULL_TIMEOUT_MS * 1000)}; /* non critical for throughput */

/* hardware access control and correction */
pthread_mutex_t mx_concent = PTHREAD_MUTEX_INITIALIZER; /* control access to the concentrator */

/* Reference coordinates, for broadcasting (beacon) */
static struct coord_s reference_coord;

/* Enable faking the GPS coordinates of the gateway */
static bool gps_fake_enable; /* enable the feature */

/* measurements to establish statistics */
static pthread_mutex_t mx_meas_up = PTHREAD_MUTEX_INITIALIZER; /* control access to the upstream measurements */
static uint32_t meas_nb_rx_rcv = 0; /* count packets received */
static uint32_t meas_nb_rx_ok = 0; /* count packets received with PAYLOAD CRC OK */
static uint32_t meas_nb_rx_bad = 0; /* count packets received with PAYLOAD CRC ERROR */
static uint32_t meas_nb_rx_nocrc = 0; /* count packets received with NO PAYLOAD CRC */
static uint32_t meas_up_pkt_fwd = 0; /* number of radio packet forwarded to the server */
static uint32_t meas_up_network_byte = 0; /* sum of UDP bytes sent for upstream traffic */
static uint32_t meas_up_payload_byte = 0; /* sum of radio payload bytes sent for upstream traffic */
static uint32_t meas_up_dgram_sent = 0; /* number of datagrams sent for upstream traffic */
static uint32_t meas_up_ack_rcv = 0; /* number of datagrams acknowledged for upstream traffic */

static pthread_mutex_t mx_meas_dw = PTHREAD_MUTEX_INITIALIZER; /* control access to the downstream measurements */
static uint32_t meas_dw_pull_sent = 0; /* number of PULL requests sent for downstream traffic */
static uint32_t meas_dw_ack_rcv = 0; /* number of PULL requests acknowledged for downstream traffic */
static uint32_t meas_dw_dgram_rcv = 0; /* count PULL response packets received for downstream traffic */
static uint32_t meas_dw_network_byte = 0; /* sum of UDP bytes sent for upstream traffic */
static uint32_t meas_dw_payload_byte = 0; /* sum of radio payload bytes sent for upstream traffic */
static uint32_t meas_nb_tx_ok = 0; /* count packets emitted successfully */
static uint32_t meas_nb_tx_fail = 0; /* count packets were TX failed for other reasons */
static uint32_t meas_nb_tx_requested = 0; /* count TX request from server (downlinks) */
static uint32_t meas_nb_tx_rejected_collision_packet = 0; /* count packets were TX request were rejected due to collision with another packet already programmed */
static uint32_t meas_nb_tx_rejected_collision_beacon = 0; /* count packets were TX request were rejected due to collision with a beacon already programmed */
static uint32_t meas_nb_tx_rejected_too_late = 0; /* count packets were TX request were rejected because it is too late to program it */
static uint32_t meas_nb_tx_rejected_too_early = 0; /* count packets were TX request were rejected because timestamp is too much in advance */

static pthread_mutex_t mx_stat_rep = PTHREAD_MUTEX_INITIALIZER; /* control access to the status report */
static bool report_ready = false; /* true when there is a new report to send to the server */
static char status_report[STATUS_SIZE]; /* status report as a JSON object */

/* auto-quit function */
static uint32_t autoquit_threshold = 0; /* enable auto-quit after a number of non-acknowledged PULL_DATA (0 = disabled)*/

/* Gateway specificities */
static int8_t antenna_gain = 0;

/* TX capabilities */
static struct lgw_tx_gain_lut_s txlut; /* TX gain table */
static uint32_t tx_freq_min[LGW_RF_CHAIN_NB]; /* lowest frequency supported by TX chain */
static uint32_t tx_freq_max[LGW_RF_CHAIN_NB]; /* highest frequency supported by TX chain */

int debug_level = LORAPF_INFO_;

/* LoRa forward to UDP socket */
static int udp_sock_desc;
static struct sockaddr_in udp_server_addr;
static int8_t buff_udp[256]; /* Must be a static global var or we overrun stack limit */

static int udp_sock_desc_down;
static uint8_t buff_down[1000];
static struct sockaddr_in udp_server_addr_down;


/* -------------------------------------------------------------------------- */
/* --- PRIVATE FUNCTIONS DECLARATION ---------------------------------------- */

static IRAM_ATTR void sig_handler(int sigio);

static int parse_SX1301_configuration(const char * conf_file);

static int parse_gateway_configuration(const char * conf_file);

//static double difftimespec(struct timespec end, struct timespec beginning);

static double time_diff(struct timeval x , struct timeval y);

static void obtain_time(void);

static void loragw_exit(int status);

/* threads */
void thread_up(void);
void thread_down(void);
/*void thread_jit(void);*/
void thread_timersync(void);

/* -------------------------------------------------------------------------- */
/* --- PRIVATE FUNCTIONS DEFINITION ----------------------------------------- */

static void exit_cleanup(void) {
    MSG_INFO("[main] Stopping concentrator\n");
    lgw_stop();
}

static IRAM_ATTR void sig_handler(int sigio) {
    if (sigio == SIGQUIT) {
        quit_sig = true;
    } else if ((sigio == SIGINT) || (sigio == SIGTERM)) {
        exit_sig = true;
    }
    return;
}
static void loragw_exit(int status)
{
    exit_sig = false;
    if(status == EXIT_FAILURE)
    {
        exit_cleanup();
        machine_pygate_set_status(PYGATE_ERROR);
        vTaskDelete(NULL);
        for(;;);
    }
    else
    {
        if(quit_sig)
        {
            quit_sig = false;
            machine_pygate_set_status(PYGATE_STOPPED);
            vTaskDelete(NULL);
            for(;;);
        }
        esp_restart();
    }
}

static int parse_SX1301_configuration(const char * conf_file) {
    int i;
    char param_name[32]; /* used to generate variable parameter names */
    const char *str; /* used to store string value from JSON object */
    const char conf_obj_name[] = "SX1301_conf";
    JSON_Value *root_val = NULL;
    JSON_Object *conf_obj = NULL;
    JSON_Value *val = NULL;
    struct lgw_conf_board_s boardconf;
    struct lgw_conf_rxrf_s rfconf;
    struct lgw_conf_rxif_s ifconf;
    uint32_t sf, bw, fdev;

    /* try to parse JSON */
    root_val = json_parse_file_with_comments(conf_file);
    if (root_val == NULL) {
        MSG_ERROR("[main] %s is not a valid JSON file\n", conf_file);
        exit(EXIT_FAILURE);
    }

    /* point to the gateway configuration object */
    conf_obj = json_object_get_object(json_value_get_object(root_val), conf_obj_name);
    if (conf_obj == NULL) {
        MSG_INFO("[main] %s does not contain a JSON object named %s\n", conf_file, conf_obj_name);
        return -1;
    } else {
        //MSG_INFO("[main] %s does contain a JSON object named %s, parsing SX1301 parameters\n", conf_file, conf_obj_name);
    }

    /* set board configuration */
    memset(&boardconf, 0, sizeof boardconf); /* initialize configuration structure */
    val = json_object_get_value(conf_obj, "lorawan_public"); /* fetch value (if possible) */
    if (json_value_get_type(val) == JSONBoolean) {
        boardconf.lorawan_public = (bool)json_value_get_boolean(val);
    } else {
        MSG_WARN("[main] Data type for lorawan_public seems wrong, please check\n");
        boardconf.lorawan_public = false;
    }
    val = json_object_get_value(conf_obj, "clksrc"); /* fetch value (if possible) */
    if (json_value_get_type(val) == JSONNumber) {
        boardconf.clksrc = (uint8_t)json_value_get_number(val);
    } else {
        MSG_WARN("[main] Data type for clksrc seems wrong, please check\n");
        boardconf.clksrc = 0;
    }
    MSG_INFO("[main] lorawan_public %d, clksrc %d\n", boardconf.lorawan_public, boardconf.clksrc);
    /* all parameters parsed, submitting configuration to the HAL */
    if (lgw_board_setconf(&boardconf) != LGW_HAL_SUCCESS) {
        MSG_ERROR("[main] Failed to configure board\n");
        return -1;
    }

    /* set antenna gain configuration */
    val = json_object_get_value(conf_obj, "antenna_gain"); /* fetch value (if possible) */
    if (val != NULL) {
        if (json_value_get_type(val) == JSONNumber) {
            antenna_gain = (int8_t)json_value_get_number(val);
        } else {
            MSG_WARN("[main] Data type for antenna_gain seems wrong, please check\n");
            antenna_gain = 0;
        }
    }
    MSG_INFO("[main] antenna_gain %d dBi\n", antenna_gain);

    /* set configuration for tx gains */
    memset(&txlut, 0, sizeof txlut); /* initialize configuration structure */
    for (i = 0; i < TX_GAIN_LUT_SIZE_MAX; i++) {
        snprintf(param_name, sizeof param_name, "tx_lut_%i", i); /* compose parameter path inside JSON structure */
        val = json_object_get_value(conf_obj, param_name); /* fetch value (if possible) */
        if (json_value_get_type(val) != JSONObject) {
            MSG_INFO("[main] no configuration for tx gain lut %i\n", i);
            continue;
        }
        txlut.size++; /* update TX LUT size based on JSON object found in configuration file */
        /* there is an object to configure that TX gain index, let's parse it */
        snprintf(param_name, sizeof param_name, "tx_lut_%i.pa_gain", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONNumber) {
            txlut.lut[i].pa_gain = (uint8_t)json_value_get_number(val);
        } else {
            MSG_WARN("[main] Data type for %s[%d] seems wrong, please check\n", param_name, i);
            txlut.lut[i].pa_gain = 0;
        }
        snprintf(param_name, sizeof param_name, "tx_lut_%i.dac_gain", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONNumber) {
            txlut.lut[i].dac_gain = (uint8_t)json_value_get_number(val);
        } else {
            txlut.lut[i].dac_gain = 3; /* This is the only dac_gain supported for now */
        }
        snprintf(param_name, sizeof param_name, "tx_lut_%i.dig_gain", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONNumber) {
            txlut.lut[i].dig_gain = (uint8_t)json_value_get_number(val);
        } else {
            MSG_WARN("[main] Data type for %s[%d] seems wrong, please check\n", param_name, i);
            txlut.lut[i].dig_gain = 0;
        }
        snprintf(param_name, sizeof param_name, "tx_lut_%i.mix_gain", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONNumber) {
            txlut.lut[i].mix_gain = (uint8_t)json_value_get_number(val);
        } else {
            MSG_WARN("[main] Data type for %s[%d] seems wrong, please check\n", param_name, i);
            txlut.lut[i].mix_gain = 0;
        }
        snprintf(param_name, sizeof param_name, "tx_lut_%i.rf_power", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONNumber) {
            txlut.lut[i].rf_power = (int8_t)json_value_get_number(val);
        } else {
            MSG_WARN("[main] Data type for %s[%d] seems wrong, please check\n", param_name, i);
            txlut.lut[i].rf_power = 0;
        }
    }
    /* all parameters parsed, submitting configuration to the HAL */
    if (txlut.size > 0) {
        MSG_INFO("[main] Configuring TX LUT with %u indexes\n", txlut.size);
        if (lgw_txgain_setconf(&txlut) != LGW_HAL_SUCCESS) {
            MSG_ERROR("[main] Failed to configure concentrator TX Gain LUT\n");
            return -1;
        }
    } else {
        MSG_WARN("[main] No TX gain LUT defined\n");
    }

    /* set configuration for RF chains */
    for (i = 0; i < LGW_RF_CHAIN_NB; ++i) {
        memset(&rfconf, 0, sizeof rfconf); /* initialize configuration structure */
        snprintf(param_name, sizeof param_name, "radio_%i", i); /* compose parameter path inside JSON structure */
        val = json_object_get_value(conf_obj, param_name); /* fetch value (if possible) */
        if (json_value_get_type(val) != JSONObject) {
            MSG_INFO("[main] no configuration for radio %i\n", i);
            continue;
        }
        /* there is an object to configure that radio, let's parse it */
        snprintf(param_name, sizeof param_name, "radio_%i.enable", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONBoolean) {
            rfconf.enable = (bool)json_value_get_boolean(val);
        } else {
            rfconf.enable = false;
        }
        if (rfconf.enable == false) { /* radio disabled, nothing else to parse */
            MSG_INFO("[main] radio %i disabled\n", i);
        } else  { /* radio enabled, will parse the other parameters */
            snprintf(param_name, sizeof param_name, "radio_%i.freq", i);
            rfconf.freq_hz = (uint32_t)json_object_dotget_number(conf_obj, param_name);
            snprintf(param_name, sizeof param_name, "radio_%i.rssi_offset", i);
            rfconf.rssi_offset = (float)json_object_dotget_number(conf_obj, param_name);
            snprintf(param_name, sizeof param_name, "radio_%i.type", i);
            str = json_object_dotget_string(conf_obj, param_name);
            if (!strncmp(str, "SX1255", 6)) {
                rfconf.type = LGW_RADIO_TYPE_SX1255;
            } else if (!strncmp(str, "SX1257", 6)) {
                rfconf.type = LGW_RADIO_TYPE_SX1257;
            } else {
                MSG_WARN("[main] invalid radio type: %s (should be SX1255 or SX1257)\n", str);
            }
            snprintf(param_name, sizeof param_name, "radio_%i.tx_enable", i);
            val = json_object_dotget_value(conf_obj, param_name);
            if (json_value_get_type(val) == JSONBoolean) {
                rfconf.tx_enable = (bool)json_value_get_boolean(val);
                if (rfconf.tx_enable == true) {
                    /* tx is enabled on this rf chain, we need its frequency range */
                    snprintf(param_name, sizeof param_name, "radio_%i.tx_freq_min", i);
                    tx_freq_min[i] = (uint32_t)json_object_dotget_number(conf_obj, param_name);
                    snprintf(param_name, sizeof param_name, "radio_%i.tx_freq_max", i);
                    tx_freq_max[i] = (uint32_t)json_object_dotget_number(conf_obj, param_name);
                    if ((tx_freq_min[i] == 0) || (tx_freq_max[i] == 0)) {
                        MSG_WARN("[main] no frequency range specified for TX rf chain %d\n", i);
                    }
                }
            } else {
                rfconf.tx_enable = false;
            }
            MSG_INFO("[main] radio %i enabled (type %s), center frequency %u, RSSI offset %f, tx enabled %d\n", i, str, rfconf.freq_hz, rfconf.rssi_offset, rfconf.tx_enable);
        }
        /* all parameters parsed, submitting configuration to the HAL */
        if (lgw_rxrf_setconf(i, &rfconf) != LGW_HAL_SUCCESS) {
            MSG_ERROR("[main] invalid configuration for radio %i\n", i);
            return -1;
        }
    }

    /* set configuration for Lora multi-SF channels (bandwidth cannot be set) */
    for (i = 0; i < LGW_MULTI_NB; ++i) {
        memset(&ifconf, 0, sizeof ifconf); /* initialize configuration structure */
        snprintf(param_name, sizeof param_name, "chan_multiSF_%i", i); /* compose parameter path inside JSON structure */
        val = json_object_get_value(conf_obj, param_name); /* fetch value (if possible) */
        if (json_value_get_type(val) != JSONObject) {
            MSG_INFO("[main] no configuration for Lora multi-SF channel %i\n", i);
            continue;
        }
        /* there is an object to configure that Lora multi-SF channel, let's parse it */
        snprintf(param_name, sizeof param_name, "chan_multiSF_%i.enable", i);
        val = json_object_dotget_value(conf_obj, param_name);
        if (json_value_get_type(val) == JSONBoolean) {
            ifconf.enable = (bool)json_value_get_boolean(val);
        } else {
            ifconf.enable = false;
        }
        if (ifconf.enable == false) { /* Lora multi-SF channel disabled, nothing else to parse */
            MSG_INFO("[main] Lora multi-SF channel %i disabled\n", i);
        } else  { /* Lora multi-SF channel enabled, will parse the other parameters */
            snprintf(param_name, sizeof param_name, "chan_multiSF_%i.radio", i);
            ifconf.rf_chain = (uint32_t)json_object_dotget_number(conf_obj, param_name);
            snprintf(param_name, sizeof param_name, "chan_multiSF_%i.if", i);
            ifconf.freq_hz = (int32_t)json_object_dotget_number(conf_obj, param_name);
            // TODO: handle individual SF enabling and disabling (spread_factor)
            MSG_INFO("[main] Lora multi-SF channel %i>  radio %i, IF %i Hz, 125 kHz bw, SF 7 to 12\n", i, ifconf.rf_chain, ifconf.freq_hz);
        }
        /* all parameters parsed, submitting configuration to the HAL */
        if (lgw_rxif_setconf(i, &ifconf) != LGW_HAL_SUCCESS) {
            MSG_ERROR("[main] invalid configuration for Lora multi-SF channel %i\n", i);
            return -1;
        }
    }

    /* set configuration for Lora standard channel */
    memset(&ifconf, 0, sizeof ifconf); /* initialize configuration structure */
    val = json_object_get_value(conf_obj, "chan_Lora_std"); /* fetch value (if possible) */
    if (json_value_get_type(val) != JSONObject) {
        MSG_INFO("[main] no configuration for Lora standard channel\n");
    } else {
        val = json_object_dotget_value(conf_obj, "chan_Lora_std.enable");
        if (json_value_get_type(val) == JSONBoolean) {
            ifconf.enable = (bool)json_value_get_boolean(val);
        } else {
            ifconf.enable = false;
        }
        if (ifconf.enable == false) {
            MSG_INFO("[main] Lora standard channel %i disabled\n", i);
        } else  {
            ifconf.rf_chain = (uint32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.radio");
            ifconf.freq_hz = (int32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.if");
            bw = (uint32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.bandwidth");
            switch(bw) {
                case 500000:
                    ifconf.bandwidth = BW_500KHZ;
                    break;
                case 250000:
                    ifconf.bandwidth = BW_250KHZ;
                    break;
                case 125000:
                    ifconf.bandwidth = BW_125KHZ;
                    break;
                default:
                    ifconf.bandwidth = BW_UNDEFINED;
            }
            sf = (uint32_t)json_object_dotget_number(conf_obj, "chan_Lora_std.spread_factor");
            switch(sf) {
                case  7:
                    ifconf.datarate = DR_LORA_SF7;
                    break;
                case  8:
                    ifconf.datarate = DR_LORA_SF8;
                    break;
                case  9:
                    ifconf.datarate = DR_LORA_SF9;
                    break;
                case 10:
                    ifconf.datarate = DR_LORA_SF10;
                    break;
                case 11:
                    ifconf.datarate = DR_LORA_SF11;
                    break;
                case 12:
                    ifconf.datarate = DR_LORA_SF12;
                    break;
                default:
                    ifconf.datarate = DR_UNDEFINED;
            }
            MSG_INFO("[main] Lora std channel> radio %i, IF %i Hz, %u Hz bw, SF %u\n", ifconf.rf_chain, ifconf.freq_hz, bw, sf);
        }
        if (lgw_rxif_setconf(8, &ifconf) != LGW_HAL_SUCCESS) {
            MSG_ERROR("[main] invalid configuration for Lora standard channel\n");
            return -1;
        }
    }

    /* set configuration for FSK channel */
    memset(&ifconf, 0, sizeof ifconf); /* initialize configuration structure */
    val = json_object_get_value(conf_obj, "chan_FSK"); /* fetch value (if possible) */
    if (json_value_get_type(val) != JSONObject) {
        MSG_INFO("[main] no configuration for FSK channel\n");
    } else {
        val = json_object_dotget_value(conf_obj, "chan_FSK.enable");
        if (json_value_get_type(val) == JSONBoolean) {
            ifconf.enable = (bool)json_value_get_boolean(val);
        } else {
            ifconf.enable = false;
        }
        if (ifconf.enable == false) {
            MSG_INFO("[main] FSK channel %i disabled\n", i);
        } else  {
            ifconf.rf_chain = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.radio");
            ifconf.freq_hz = (int32_t)json_object_dotget_number(conf_obj, "chan_FSK.if");
            bw = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.bandwidth");
            fdev = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.freq_deviation");
            ifconf.datarate = (uint32_t)json_object_dotget_number(conf_obj, "chan_FSK.datarate");

            /* if chan_FSK.bandwidth is set, it has priority over chan_FSK.freq_deviation */
            if ((bw == 0) && (fdev != 0)) {
                bw = 2 * fdev + ifconf.datarate;
            }
            if      (bw == 0) {
                ifconf.bandwidth = BW_UNDEFINED;
            } else if (bw <= 7800) {
                ifconf.bandwidth = BW_7K8HZ;
            } else if (bw <= 15600) {
                ifconf.bandwidth = BW_15K6HZ;
            } else if (bw <= 31200) {
                ifconf.bandwidth = BW_31K2HZ;
            } else if (bw <= 62500) {
                ifconf.bandwidth = BW_62K5HZ;
            } else if (bw <= 125000) {
                ifconf.bandwidth = BW_125KHZ;
            } else if (bw <= 250000) {
                ifconf.bandwidth = BW_250KHZ;
            } else if (bw <= 500000) {
                ifconf.bandwidth = BW_500KHZ;
            } else {
                ifconf.bandwidth = BW_UNDEFINED;
            }

            MSG_INFO("[main] FSK channel> radio %i, IF %i Hz, %u Hz bw, %u bps datarate\n", ifconf.rf_chain, ifconf.freq_hz, bw, ifconf.datarate);
        }
        if (lgw_rxif_setconf(9, &ifconf) != LGW_HAL_SUCCESS) {
            MSG_ERROR("[main] invalid configuration for FSK channel\n");
            return -1;
        }
    }
    json_value_free(root_val);

    return 0;
}

static int parse_gateway_configuration(const char * conf_file) {
    const char conf_obj_name[] = "gateway_conf";
    JSON_Value *root_val;
    JSON_Object *conf_obj = NULL;
    JSON_Value *val = NULL; /* needed to detect the absence of some fields */
    const char *str; /* pointer to sub-strings in the JSON data */
    unsigned long long ull = 0;

    /* try to parse JSON */
    root_val = json_parse_file_with_comments(conf_file);
    if (root_val == NULL) {
        MSG_ERROR("[main] %s is not a valid JSON file\n", conf_file);
        exit(EXIT_FAILURE);
    }

    /* point to the gateway configuration object */
    conf_obj = json_object_get_object(json_value_get_object(root_val), conf_obj_name);
    if (conf_obj == NULL) {
        MSG_INFO("[main] %s does not contain a JSON object named %s\n", conf_file, conf_obj_name);
        return -1;
    } else {
        //MSG_INFO("[main] %s does contain a JSON object named %s, parsing gateway parameters\n", conf_file, conf_obj_name);
    }

    /* gateway unique identifier (aka MAC address) (optional) */
    str = json_object_get_string(conf_obj, "gateway_ID");
    if (str != NULL) {
        sscanf(str, "%llx", &ull);
        lgwm = ull;
        MSG_INFO("[main] gateway MAC address is configured to %016llX\n", ull);
    }

    /* server hostname or IP address (optional) */
    str = json_object_get_string(conf_obj, "server_address");
    if (str != NULL) {
        strncpy(serv_addr, str, sizeof serv_addr);
        MSG_INFO("[main] server hostname or IP address is configured to \"%s\"\n", serv_addr);
    }

    /* get up and down ports (optional) */
    val = json_object_get_value(conf_obj, "serv_port_up");
    if (val != NULL) {
        snprintf(serv_port_up, sizeof serv_port_up, "%u", (uint16_t)json_value_get_number(val));
        MSG_INFO("[main] upstream port is configured to \"%s\"\n", serv_port_up);
    }
    val = json_object_get_value(conf_obj, "serv_port_down");
    if (val != NULL) {
        snprintf(serv_port_down, sizeof serv_port_down, "%u", (uint16_t)json_value_get_number(val));
        MSG_INFO("[main] downstream port is configured to \"%s\"\n", serv_port_down);
    }

    /* get keep-alive interval (in seconds) for downstream (optional) */
    val = json_object_get_value(conf_obj, "keepalive_interval");
    if (val != NULL) {
        keepalive_time = (int)json_value_get_number(val);
        MSG_INFO("[main] downstream keep-alive interval is configured to %u seconds\n", keepalive_time);
    }

    /* get interval (in seconds) for statistics display (optional) */
    val = json_object_get_value(conf_obj, "stat_interval");
    if (val != NULL) {
        stat_interval = (unsigned)json_value_get_number(val);
        MSG_INFO("[main] statistics display interval is configured to %u seconds\n", stat_interval);
    }

    /* get time-out value (in ms) for upstream datagrams (optional) */
    val = json_object_get_value(conf_obj, "push_timeout_ms");
    if (val != NULL) {
        push_timeout_half.tv_usec = 500 * (long int)json_value_get_number(val);
        MSG_INFO("[main] upstream PUSH_DATA time-out is configured to %u ms\n", (unsigned)(push_timeout_half.tv_usec / 500));
    }

    /* packet filtering parameters */
    val = json_object_get_value(conf_obj, "forward_crc_valid");
    if (json_value_get_type(val) == JSONBoolean) {
        fwd_valid_pkt = (bool)json_value_get_boolean(val);
    }
    MSG_INFO("[main] packets received with a valid CRC will%s be forwarded\n", (fwd_valid_pkt ? "" : " NOT"));
    val = json_object_get_value(conf_obj, "forward_crc_error");
    if (json_value_get_type(val) == JSONBoolean) {
        fwd_error_pkt = (bool)json_value_get_boolean(val);
    }
    MSG_INFO("[main] packets received with a CRC error will%s be forwarded\n", (fwd_error_pkt ? "" : " NOT"));
    val = json_object_get_value(conf_obj, "forward_crc_disabled");
    if (json_value_get_type(val) == JSONBoolean) {
        fwd_nocrc_pkt = (bool)json_value_get_boolean(val);
    }
    MSG_INFO("[main] packets received with no CRC will%s be forwarded\n", (fwd_nocrc_pkt ? "" : " NOT"));

    /* get reference coordinates */
    val = json_object_get_value(conf_obj, "ref_latitude");
    if (val != NULL) {
        reference_coord.lat = (double)json_value_get_number(val);
        MSG_INFO("[main] Reference latitude is configured to %f deg\n", reference_coord.lat);
    }
    val = json_object_get_value(conf_obj, "ref_longitude");
    if (val != NULL) {
        reference_coord.lon = (double)json_value_get_number(val);
        MSG_INFO("[main] Reference longitude is configured to %f deg\n", reference_coord.lon);
    }
    val = json_object_get_value(conf_obj, "ref_altitude");
    if (val != NULL) {
        reference_coord.alt = (short)json_value_get_number(val);
        MSG_INFO("[main] Reference altitude is configured to %i meters\n", reference_coord.alt);
    }

    /* Gateway GPS coordinates hardcoding (aka. faking) option */
    val = json_object_get_value(conf_obj, "fake_gps");
    if (json_value_get_type(val) == JSONBoolean) {
        gps_fake_enable = (bool)json_value_get_boolean(val);
        if (gps_fake_enable == true) {
            MSG_INFO("[main] fake GPS is enabled\n");
        } else {
            MSG_INFO("[main] fake GPS is disabled\n");
        }
    }

    /* Auto-quit threshold (optional) */
    val = json_object_get_value(conf_obj, "autoquit_threshold");
    if (val != NULL) {
        autoquit_threshold = (uint32_t)json_value_get_number(val);
        MSG_INFO("[main] Auto-quit after %u non-acknowledged PULL_DATA\n", autoquit_threshold);
    }

    /* free JSON parsing data structure */
    json_value_free(root_val);
    return 0;
}

static double time_diff(struct timeval x , struct timeval y)
{
    double x_ms , y_ms , diff;

    x_ms = (double)x.tv_sec*1000000 + (double)x.tv_usec;
    y_ms = (double)y.tv_sec*1000000 + (double)y.tv_usec;

    diff = (double)y_ms - (double)x_ms;

    return diff / 1000000.0;
}

/*static double difftimespec(struct timespec end, struct timespec beginning) {
    double x;

    x = 1E-9 * (double)(end.tv_nsec - beginning.tv_nsec);
    x += (double)(end.tv_sec - beginning.tv_sec);

    return x;
}*/

static void obtain_time(void)
{
    // wait for time to be set
    time_t now = 0;
    struct tm timeinfo = { 0 };
    int retry = 0;
    const int retry_count = 10;
    while(!mach_is_rtc_synced() && ++retry < retry_count) {
        MSG_INFO("[main] Waiting for system time to be set... (%d/%d)\n", retry, retry_count);
        vTaskDelay(2000 / portTICK_PERIOD_MS);
        time(&now);
        localtime_r(&now, &timeinfo);
    }
    if(retry == retry_count)
    {
        MSG_ERROR("[main] Failed to set system time.. please Sync time via an NTP server using RTC module.!\n");
        exit(EXIT_FAILURE);
    }

}


/* -------------------------------------------------------------------------- */
/* --- MAIN FUNCTION -------------------------------------------------------- */

void lora_gw_init(const char* global_conf) {
    MSG_INFO("lora_gw_init() start fh=%u high=%u LORA_GW_STACK_SIZE=%u\n", xPortGetFreeHeapSize(), uxTaskGetStackHighWaterMark(NULL), LORA_GW_STACK_SIZE);

    quit_sig = false;
    exit_sig = false;

    xTaskCreatePinnedToCore(TASK_lora_gw, "LoraGW",
        LORA_GW_STACK_SIZE / sizeof(StackType_t),
        (void *) global_conf,
        LORA_GW_PRIORITY, &xLoraGwTaskHndl, 1);
    MSG_INFO("lora_gw_init() done fh=%u high=%u\n", xPortGetFreeHeapSize(), uxTaskGetStackHighWaterMark(NULL));
}

void pygate_reset() {
    MSG_INFO("pygate_reset\n");

    // pull sx1257 and sx1308 reset high, the PIC FW should power cycle the ESP32 as a result
    pin_obj_t* sx1308_rst = SX1308_RST_PIN;
    pin_config(sx1308_rst, -1, -1, GPIO_MODE_OUTPUT, MACHPIN_PULL_NONE, 0);
    pin_obj_t* sx1257_rst = (&PIN_MODULE_P8);
    pin_config(sx1257_rst, -1, -1, GPIO_MODE_OUTPUT, MACHPIN_PULL_NONE, 0);

    sx1308_rst->value = 1;
    sx1257_rst->value = 1;

    pin_set_value(sx1308_rst);
    pin_set_value(sx1257_rst);

    vTaskDelay(5000 / portTICK_PERIOD_MS);

    // if this is still being executed, then it seems the ESP32 reset did not take place
    // set the two reset lines low again and stop the lora gw task, to make sure we return to a defined state
    MSG_ERROR("pygate_reset failed to reset\n");
    sx1308_rst->value = 0;
    sx1257_rst->value = 0;
    pin_set_value(sx1308_rst);
    pin_set_value(sx1257_rst);

    if (xLoraGwTaskHndl){
        vTaskDelete(xLoraGwTaskHndl);
        xLoraGwTaskHndl = NULL;
    }

}

int lora_gw_get_debug_level(){
    return debug_level;
}

void lora_gw_set_debug_level(int level){
    debug_level = level;
}

/*void lora_gw_stop(void) {
    vTaskDelete(xLoraGwTaskHndl);
}*/

/*void logger(const char *msg) {
    FILE *fp;

    fp = fopen("/flash/debug.txt", "a+");
    fprintf(fp, msg);
    fclose(fp);
}*/

void TASK_lora_gw(void *pvParameters) {

    int i; /* loop variable and temporary variable for return value */
    int x;
    //set signal handler cb
    mp_hal_set_signal_exit_cb(sig_handler);
    machine_register_pygate_sig_handler(sig_handler);
    mp_hal_set_interrupt_char(3);
    /* COM interfaces */
    const char com_path_default[] = COM_PATH_DEFAULT;
    const char *com_path = com_path_default;

    /* configuration file related */
    //char *global_cfg_path = "/flash/global_conf.json"; /* contain global (typ. network-wide) configuration */
    //char *local_cfg_path = "/flash/local_conf.json"; /* contain node specific configuration, overwrite global parameters for parameters that are defined in both */
    //char *debug_cfg_path = "/flash/debug_conf.json"; /* if present, all other configuration files are ignored */

    /* threads */
    pthread_t thrid_up;
    pthread_t thrid_down;
    /* No longer need jit thread */
    /*pthread_t thrid_jit;*/
    pthread_t thrid_timersync;

    /* network socket creation */
    struct addrinfo hints;
    struct addrinfo *result; /* store result of getaddrinfo */
    struct addrinfo *q; /* pointer to move into *result data */
    //char host_name[64];
    //char port_name[64];

    /* variables to get local copies of measurements */
    uint32_t cp_nb_rx_rcv;
    uint32_t cp_nb_rx_ok;
    uint32_t cp_nb_rx_bad;
    uint32_t cp_nb_rx_nocrc;
    uint32_t cp_up_pkt_fwd;
    uint32_t cp_up_network_byte;
    uint32_t cp_up_payload_byte;
    uint32_t cp_up_dgram_sent;
    uint32_t cp_up_ack_rcv;
    uint32_t cp_dw_pull_sent;
    uint32_t cp_dw_ack_rcv;
    uint32_t cp_dw_dgram_rcv;
    uint32_t cp_dw_network_byte;
    uint32_t cp_dw_payload_byte;
    uint32_t cp_nb_tx_ok;
    uint32_t cp_nb_tx_fail;
    uint32_t cp_nb_tx_requested = 0;
    uint32_t cp_nb_tx_rejected_collision_packet = 0;
    uint32_t cp_nb_tx_rejected_collision_beacon = 0;
    uint32_t cp_nb_tx_rejected_too_late = 0;
    uint32_t cp_nb_tx_rejected_too_early = 0;

    /* GPS coordinates variables */
    struct coord_s cp_gps_coord = {0.0, 0.0, 0};

    /* statistics variable */
    time_t t;
    char stat_timestamp[24];
    float rx_ok_ratio;
    float rx_bad_ratio;
    float rx_nocrc_ratio;
    float up_ack_ratio;
    float dw_ack_ratio;

    /* Parse command line options */
    /*while((i = getopt(argc, argv, "hd:")) != -1) {
        switch(i) {
        case 'h':
            usage();
            return EXIT_SUCCESS;

        case 'd':
            if (optarg != NULL) {
                com_path = optarg;
            }
            break;

        default:
            MSG_ERROR("[main] argument parsing options, use -h option for help\n");
            usage();
            return EXIT_FAILURE;
        }
    }*/

    /* display version informations */
    MSG_INFO("*** Packet Forwarder for Lora PicoCell Gateway ***\nVersion: " VERSION_STRING "\n");
    MSG_INFO("lorapf *** Lora concentrator HAL library version info ***\n%s\n", lgw_version_info());

    /* Open communication bridge */
    x = lgw_connect(NULL);
    if (x == LGW_REG_ERROR) {
        MSG_ERROR("[main] FAIL TO CONNECT BOARD ON %s\n", com_path);
        exit(EXIT_FAILURE);
    }

    time_t now;
    struct tm timeinfo;

    time(&now);
    localtime_r(&now, &timeinfo);
    // Is time set? If not, tm_year will be (1970 - 1900).
    // Time not important...
    /*if (timeinfo.tm_year < (2016 - 1900)) {
        MSG_INFO("[main] Time is not set.\n");
        obtain_time();
        // update 'now' variable with current time
        time(&now);
    }*/

    //MSG("lorapf *** MCU FW version for LoRa PicoCell Gateway ***\nVersion: 0x%08X\n***\n", lgw_mcu_version_info());

    /* display host endianness */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    MSG_INFO("[main] Little endian host\n");
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    MSG_INFO("[main] Big endian host\n");
#else
    MSG_INFO("[main] Host endianness unknown\n");
#endif

    x = parse_SX1301_configuration((char *)pvParameters);
    if (x != 0) {
       exit(EXIT_FAILURE);
    }
    x = parse_gateway_configuration((char *)pvParameters);
    if (x != 0) {
       exit(EXIT_FAILURE);
    }

    MSG_INFO("[main] found global configuration file and parsed correctly\n");
    wait_ms (2000);

    /* load configuration files */
    /*if (access(debug_cfg_path, R_OK) == 0) { // if there is a debug conf, parse only the debug conf
        MSG_INFO("[main] found debug configuration file %s, parsing it\n", debug_cfg_path);
        MSG_INFO("[main] other configuration files will be ignored\n");
        x = parse_SX1301_configuration(debug_cfg_path);
        if (x != 0) {
            exit(EXIT_FAILURE);
        }
        x = parse_gateway_configuration(debug_cfg_path);
        if (x != 0) {
            exit(EXIT_FAILURE);
        }
    } else if (access(global_cfg_path, R_OK) == 0) { // if there is a global conf, parse it and then try to parse local conf
        MSG_INFO("[main] found global configuration file %s, parsing it\n", global_cfg_path);
        x = parse_SX1301_configuration(global_cfg_path);
        if (x != 0) {
            exit(EXIT_FAILURE);
        }
        x = parse_gateway_configuration(global_cfg_path);
        if (x != 0) {
            exit(EXIT_FAILURE);
        }
        if (access(local_cfg_path, R_OK) == 0) {
            MSG_INFO("[main] found local configuration file %s, parsing it\n", local_cfg_path);
            MSG_INFO("[main] redefined parameters will overwrite global parameters\n");
            parse_SX1301_configuration(local_cfg_path);
            parse_gateway_configuration(local_cfg_path);
        }
    } else if (access(local_cfg_path, R_OK) == 0) { // if there is only a local conf, parse it and that's all
        MSG_INFO("[main] found local configuration file %s, parsing it\n", local_cfg_path);
        x = parse_SX1301_configuration(local_cfg_path);
        if (x != 0) {
            exit(EXIT_FAILURE);
        }
        x = parse_gateway_configuration(local_cfg_path);
        if (x != 0) {
            exit(EXIT_FAILURE);
        }
    } else {
        MSG_ERROR("[main] failed to find any configuration file named %s, %s OR %s\n", global_cfg_path, local_cfg_path, debug_cfg_path);
        exit(EXIT_FAILURE);
    }*/

    /* get timezone info */
    tzset();

    /* sanity check on configuration variables */
    // TODO

    /* process some of the configuration variables */
    net_mac_h = htonl((uint32_t)(0xFFFFFFFF & (lgwm >> 32)));
    net_mac_l = htonl((uint32_t)(0xFFFFFFFF &  lgwm  ));




    /* Initialise UDP sockets */

    udp_sock_desc = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock_desc < 0){
        MSG_INFO("Error while creating socket\n");
    } else {
        MSG_INFO("Socket created successfully\n");
    }

    udp_server_addr.sin_family = AF_INET;
    udp_server_addr.sin_port = htons(6000);
    udp_server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    udp_sock_desc_down = socket(AF_INET, SOCK_DGRAM,0);
    if (udp_sock_desc_down < 0){
        MSG_INFO("Error while creating socket\n");
    } else {
        MSG_INFO("Socket created successfully\n");
    }

    udp_server_addr_down.sin_family = AF_INET;
    udp_server_addr_down.sin_port = htons(6001);
    udp_server_addr_down.sin_addr.s_addr = INADDR_ANY;

    /* Bind down server */
    if (bind(udp_sock_desc_down, (const struct sockaddr *)&udp_server_addr_down, sizeof(udp_server_addr_down)) < 0){
        MSG_INFO("Error while binding socket\n");
    }



    /* starting the concentrator */
    i = lgw_start();
    if (i == LGW_HAL_SUCCESS) {
        MSG_INFO("[main] concentrator started, packet can now be received\n");
    } else {
        MSG_ERROR("[main] failed to start the concentrator\n");
        //exit(EXIT_FAILURE);
    }
    esp_pthread_cfg_t cfg = {
            (10 * 1024),
            10,
            true
    };
    esp_pthread_set_cfg(&cfg);
    /* spawn threads to manage upstream and downstream */
    i = pthread_create( &thrid_up, NULL, (void * (*)(void *))thread_up, NULL);
    if (i != 0) {
        MSG_ERROR("[main] impossible to create upstream thread\n");
        exit(EXIT_FAILURE);
    }

    i = pthread_create( &thrid_down, NULL, (void * (*)(void *))thread_down, NULL);
    if (i != 0) {
        MSG_ERROR("[main] impossible to create downstream thread\n");
        exit_sig = true;
        pthread_join(thrid_up, NULL);
        exit(EXIT_FAILURE);
    }

    /* Do not need jit thread anymore */
   /* cfg.stack_size = (7 * 1024);
    esp_pthread_set_cfg(&cfg);
    i = pthread_create( &thrid_jit, NULL, (void * (*)(void *))thread_jit, NULL);
    if (i != 0) {
        MSG_ERROR("[main] impossible to create JIT thread\n");
        exit_sig = true;
        pthread_join(thrid_up, NULL);
        pthread_join(thrid_down, NULL);
        exit(EXIT_FAILURE);
    }*/

    cfg.stack_size = (7 * 1024);
    esp_pthread_set_cfg(&cfg);
    i = pthread_create( &thrid_timersync, NULL, (void * (*)(void *))thread_timersync, NULL);
    if (i != 0) {
        MSG_ERROR("[main] impossible to create Timer Sync thread (%d) (%d) (%d,%d,%d)\n", i, errno, EAGAIN, EINVAL, EPERM);
        exit_sig = true;
        pthread_join(thrid_up, NULL);
        pthread_join(thrid_down, NULL);
        /*pthread_join(thrid_jit, NULL);*/
        exit(EXIT_FAILURE);
    }

    machine_pygate_set_status(PYGATE_STARTED);
    mp_printf(&mp_plat_print, "LoRa GW started\n");

    /* main loop task : statistics collection */
    while (!exit_sig && !quit_sig) {
        /* wait for next reporting interval */
        wait_ms ((1000 * stat_interval) / portTICK_PERIOD_MS);

        /* get timestamp for statistics */
        t = time(NULL);
        strftime(stat_timestamp, sizeof stat_timestamp, "%F %T %Z", gmtime(&t));

        /* access upstream statistics, copy and reset them */
        pthread_mutex_lock(&mx_meas_up);
        cp_nb_rx_rcv       = meas_nb_rx_rcv;
        cp_nb_rx_ok        = meas_nb_rx_ok;
        cp_nb_rx_bad       = meas_nb_rx_bad;
        cp_nb_rx_nocrc     = meas_nb_rx_nocrc;
        cp_up_pkt_fwd      = meas_up_pkt_fwd;
        cp_up_network_byte = meas_up_network_byte;
        cp_up_payload_byte = meas_up_payload_byte;
        cp_up_dgram_sent   = meas_up_dgram_sent;
        cp_up_ack_rcv      = meas_up_ack_rcv;
        meas_nb_rx_rcv = 0;
        meas_nb_rx_ok = 0;
        meas_nb_rx_bad = 0;
        meas_nb_rx_nocrc = 0;
        meas_up_pkt_fwd = 0;
        meas_up_network_byte = 0;
        meas_up_payload_byte = 0;
        meas_up_dgram_sent = 0;
        meas_up_ack_rcv = 0;
        pthread_mutex_unlock(&mx_meas_up);
        if (cp_nb_rx_rcv > 0) {
            rx_ok_ratio = (float)cp_nb_rx_ok / (float)cp_nb_rx_rcv;
            rx_bad_ratio = (float)cp_nb_rx_bad / (float)cp_nb_rx_rcv;
            rx_nocrc_ratio = (float)cp_nb_rx_nocrc / (float)cp_nb_rx_rcv;
        } else {
            rx_ok_ratio = 0.0;
            rx_bad_ratio = 0.0;
            rx_nocrc_ratio = 0.0;
        }
        if (cp_up_dgram_sent > 0) {
            up_ack_ratio = (float)cp_up_ack_rcv / (float)cp_up_dgram_sent;
        } else {
            up_ack_ratio = 0.0;
        }

        /* access downstream statistics, copy and reset them */
        pthread_mutex_lock(&mx_meas_dw);
        cp_dw_pull_sent    =  meas_dw_pull_sent;
        cp_dw_ack_rcv      =  meas_dw_ack_rcv;
        cp_dw_dgram_rcv    =  meas_dw_dgram_rcv;
        cp_dw_network_byte =  meas_dw_network_byte;
        cp_dw_payload_byte =  meas_dw_payload_byte;
        cp_nb_tx_ok        =  meas_nb_tx_ok;
        cp_nb_tx_fail      =  meas_nb_tx_fail;
        cp_nb_tx_requested                 +=  meas_nb_tx_requested;
        cp_nb_tx_rejected_collision_packet +=  meas_nb_tx_rejected_collision_packet;
        cp_nb_tx_rejected_collision_beacon +=  meas_nb_tx_rejected_collision_beacon;
        cp_nb_tx_rejected_too_late         +=  meas_nb_tx_rejected_too_late;
        cp_nb_tx_rejected_too_early        +=  meas_nb_tx_rejected_too_early;
        meas_dw_pull_sent = 0;
        meas_dw_ack_rcv = 0;
        meas_dw_dgram_rcv = 0;
        meas_dw_network_byte = 0;
        meas_dw_payload_byte = 0;
        meas_nb_tx_ok = 0;
        meas_nb_tx_fail = 0;
        meas_nb_tx_requested = 0;
        meas_nb_tx_rejected_collision_packet = 0;
        meas_nb_tx_rejected_collision_beacon = 0;
        meas_nb_tx_rejected_too_late = 0;
        meas_nb_tx_rejected_too_early = 0;
        pthread_mutex_unlock(&mx_meas_dw);
        if (cp_dw_pull_sent > 0) {
            dw_ack_ratio = (float)cp_dw_ack_rcv / (float)cp_dw_pull_sent;
        } else {
            dw_ack_ratio = 0.0;
        }

        /* overwrite with reference coordinates if function is enabled */
        if (gps_fake_enable == true) {
            cp_gps_coord = reference_coord;
        }

        /* display a report */
#if LORAPF_DEBUG_LEVEL >= LORAPF_INFO_
        if ( debug_level >= LORAPF_INFO_){
        MSG_INFO("[main] report\n##### %s #####\n", stat_timestamp);
        mp_printf(&mp_plat_print, "### [UPSTREAM] ###\n");
        mp_printf(&mp_plat_print, "# RF packets received by concentrator: %u\n", cp_nb_rx_rcv);
        mp_printf(&mp_plat_print, "# CRC_OK: %.2f%%, CRC_FAIL: %.2f%%, NO_CRC: %.2f%%\n", 100.0 * rx_ok_ratio, 100.0 * rx_bad_ratio, 100.0 * rx_nocrc_ratio);
        mp_printf(&mp_plat_print, "# RF packets forwarded: %u (%u bytes)\n", cp_up_pkt_fwd, cp_up_payload_byte);
        mp_printf(&mp_plat_print, "# PUSH_DATA datagrams sent: %u (%u bytes)\n", cp_up_dgram_sent, cp_up_network_byte);
        mp_printf(&mp_plat_print, "# PUSH_DATA acknowledged: %.2f%%\n", 100.0 * up_ack_ratio);
        mp_printf(&mp_plat_print, "### [DOWNSTREAM] ###\n");
        mp_printf(&mp_plat_print, "# PULL_DATA sent: %u (%.2f%% acknowledged)\n", cp_dw_pull_sent, 100.0 * dw_ack_ratio);
        mp_printf(&mp_plat_print, "# PULL_RESP(onse) datagrams received: %u (%u bytes)\n", cp_dw_dgram_rcv, cp_dw_network_byte);
        mp_printf(&mp_plat_print, "# RF packets sent to concentrator: %u (%u bytes)\n", (cp_nb_tx_ok + cp_nb_tx_fail), cp_dw_payload_byte);
        mp_printf(&mp_plat_print, "# TX errors: %u\n", cp_nb_tx_fail);
        if (cp_nb_tx_requested != 0 ) {
            mp_printf(&mp_plat_print, "# TX rejected (collision packet): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_collision_packet / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_collision_packet);
            mp_printf(&mp_plat_print, "# TX rejected (collision beacon): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_collision_beacon / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_collision_beacon);
            mp_printf(&mp_plat_print, "# TX rejected (too late): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_too_late / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_too_late);
            mp_printf(&mp_plat_print, "# TX rejected (too early): %.2f%% (req:%u, rej:%u)\n", 100.0 * cp_nb_tx_rejected_too_early / cp_nb_tx_requested, cp_nb_tx_requested, cp_nb_tx_rejected_too_early);
        }
        mp_printf(&mp_plat_print, "### [GPS] ###\n");
        if (gps_fake_enable == true) {
            mp_printf(&mp_plat_print, "# GPS *FAKE* coordinates: latitude %.5f, longitude %.5f, altitude %i m\n", cp_gps_coord.lat, cp_gps_coord.lon, cp_gps_coord.alt);
        } else {
            mp_printf(&mp_plat_print, "# GPS sync is disabled\n");
        }
        mp_printf(&mp_plat_print, "##### END #####\n");
        }
#endif

        /* generate a JSON report (will be sent to server by upstream thread) */
        pthread_mutex_lock(&mx_stat_rep);
        snprintf(status_report, STATUS_SIZE, "\"stat\":{\"time\":\"%s\",\"rxnb\":%u,\"rxok\":%u,\"rxfw\":%u,\"ackr\":%.1f,\"dwnb\":%u,\"txnb\":%u}", stat_timestamp, cp_nb_rx_rcv, cp_nb_rx_ok, cp_up_pkt_fwd, 100.0 * up_ack_ratio, cp_dw_dgram_rcv, cp_nb_tx_ok);
        report_ready = true;
        pthread_mutex_unlock(&mx_stat_rep);
    }
    MSG_INFO("[main] Exited main packet forwarder loop\n");

    /* wait for upstream thread to finish (1 fetch cycle max) */
    pthread_join(thrid_up, NULL);
    pthread_join(thrid_down, NULL); /* don't wait for downstream thread */
    /*pthread_join(thrid_jit, NULL); *//* don't wait for jit thread *//*No longer exists*/
    pthread_join(thrid_timersync, NULL); /* don't wait for timer sync thread */

    /* if an exit signal was received, try to quit properly */
    if (true) {
        /* stop the hardware */
        i = lgw_stop();
        if (i == LGW_HAL_SUCCESS) {
            MSG_INFO("[main] concentrator stopped successfully\n");
        } else {
            MSG_WARN("[main] failed to stop concentrator successfully\n");
        }
    }

    MSG_INFO("[main] Exiting packet forwarder program\n");
    exit(EXIT_SUCCESS);
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 1: RECEIVING PACKETS AND FORWARDING THEM ---------------------- */

void thread_up(void) {
    MSG_INFO("[up  ] start\n");
    int i, j; /* loop variables */
    unsigned pkt_in_dgram; /* nb on Lora packet in the current datagram */

    /* allocate memory for packet fetching and processing */
    struct lgw_pkt_rx_s rxpkt[NB_PKT_MAX]; /* array containing inbound packets + metadata */
    struct lgw_pkt_rx_s *p; /* pointer on a RX packet */
    int nb_pkt;

    /* data buffers */
    uint8_t buff_up[TX_BUFF_SIZE]; /* buffer to compose the upstream packet */
    int buff_index;
    uint8_t buff_ack[32]; /* buffer to receive acknowledges */

    /* protocol variables */
    uint8_t token_h = 0; /* random token for acknowledgement matching */
    uint8_t token_l = 0; /* random token for acknowledgement matching */
    static uint16_t token = 0;
    /* ping measurement variables */
    struct timeval send_time;
    struct timeval recv_time;

    /* report management variable */
    bool send_report = false;

    /* mote info variables */
    uint32_t mote_addr = 0;
    uint16_t mote_fcnt = 0;

    /* UDP packet structure */
    uint8_t sz;
    uint8_t udp_idx;
    uint8_t header_size;
    /*uint8_t buff_udp[1024];*/
    /*struct udp_packet_structure *udp_packet;*/


    while (!exit_sig && !quit_sig) {
        /* fetch packets */
        pthread_mutex_lock(&mx_concent);
        nb_pkt = lgw_receive(NB_PKT_MAX, rxpkt);  // Crashing here
        pthread_mutex_unlock(&mx_concent);
        if (nb_pkt == LGW_HAL_ERROR) {
            MSG_ERROR("[up  ] failed packet fetch, exiting\n");
            //exit(EXIT_FAILURE);
        }

        /* check if there are status report to send */
        send_report = report_ready; /* copy the variable so it doesn't change mid-function */
        /* no mutex, we're only reading */

        /* wait a short time if no packets, nor status report */
        if ((nb_pkt == 0) && (send_report == false)) {
            wait_ms ((FETCH_SLEEP_MS));
            continue;
        }

        /* tmp init buff_index */
        buff_index = 0;


        /* serialize Lora packets metadata and payload */
        pkt_in_dgram = 0;
        for (i = 0; i < nb_pkt; ++i) {
            /* Init udp buffer pointer */
            udp_idx = 0;
            header_size = 0;

            /* This is a LoRa packet */
            buff_udp[udp_idx] = LORA_PACKET;
            udp_idx++;

            p = &rxpkt[i];

            /* basic packet filtering */
            pthread_mutex_lock(&mx_meas_up);
            meas_nb_rx_rcv += 1;

            /* might be useful to send info of failed packets */

            switch(p->status) {
                case STAT_CRC_OK:
                    meas_nb_rx_ok += 1;

                    if (!fwd_valid_pkt) {
                        pthread_mutex_unlock(&mx_meas_up);
                        continue; /* skip that packet */
                    }
                    break;
                case STAT_CRC_BAD:
                    meas_nb_rx_bad += 1;
                    if (!fwd_error_pkt) {
                        pthread_mutex_unlock(&mx_meas_up);
                        continue; /* skip that packet */
                    }
                    break;
                case STAT_NO_CRC:
                    meas_nb_rx_nocrc += 1;
                    if (!fwd_nocrc_pkt) {
                        pthread_mutex_unlock(&mx_meas_up);
                        continue; /* skip that packet */
                    }
                    break;
                default:
                    MSG_WARN("[up  ] received packet with unknown status %u (size %u, modulation %u, BW %u, DR %u, RSSI %.1f)\n", p->status, p->size, p->modulation, p->bandwidth, p->datarate, p->rssi);
                    pthread_mutex_unlock(&mx_meas_up);
                    continue; /* skip that packet */
                    // exit(EXIT_FAILURE);
            }
            meas_up_pkt_fwd += 1;
            meas_up_payload_byte += p->size;
            pthread_mutex_unlock(&mx_meas_up);




            /* Insert size of payload */
            buff_udp[udp_idx] = p->size & 0xFF;
            MSG_ERROR("[main] Payload received is of size: %i\n", p->size);
            /* Skip header size for now */
            udp_idx += 2;

            /* Insert RSSI SNR info */
            j = snprintf((char *)(buff_udp + 2), 1022, "{\"rssi\":%.0f,\"snr\":%.0f", p->rssi, p->snr);
            if (j <= 0) {
                MSG_ERROR("[up  ] snprintf failed line %u\n", (__LINE__ - 4));
                quit_sig = true;
                machine_pygate_set_status(PYGATE_ERROR);
            }

            /* Increase header size */
            header_size += j;

            /* Shift buffer idx along by j */
            udp_idx += j;

            /* Packet concentrator channel, RF chain & RX frequency, 34-36 useful chars */
            j = snprintf((char *)(buff_udp + udp_idx), TX_BUFF_SIZE - buff_index, ",\"chan\":%1u,\"rfch\":%1u,\"freq\":%.6lf", p->if_chain, p->rf_chain, ((double)p->freq_hz / 1e6));
            if (j <= 0 ) {
                MSG_ERROR("[up  ] snprintf failed line %u\n", (__LINE__ - 4));
                quit_sig = true;
                machine_pygate_set_status(PYGATE_ERROR);
            }
            /* Increment UDP buffer pointer */
            udp_idx += j;
            /* Increase header size */
            header_size += j;

            /* Packet modulation, 13-14 useful chars */
            if (p->modulation == MOD_LORA) {

                /* Lora datarate & bandwidth, 16-19 useful chars */
                switch (p->datarate) {
                    case DR_LORA_SF7:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"datr\":\"SF7", 12);
                        udp_idx += 12;
                        header_size += 12;
                        break;
                    case DR_LORA_SF8:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"datr\":\"SF8", 12);
                        udp_idx += 12;
                        header_size += 12;
                        break;
                    case DR_LORA_SF9:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"datr\":\"SF9", 12);
                        udp_idx += 12;
                        header_size += 12;
                        break;
                    case DR_LORA_SF10:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"datr\":\"SF10", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    case DR_LORA_SF11:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"datr\":\"SF11", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    case DR_LORA_SF12:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"datr\":\"SF12", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    default:
                        MSG_ERROR("[up  ] lora packet with unknown datarate\n");
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"datr\":\"SF?", 12);
                        udp_idx += 13;
                        header_size += 13;
                        quit_sig = true;
                        machine_pygate_set_status(PYGATE_ERROR);
                }
                switch (p->bandwidth) {
                    case BW_125KHZ:
                        memcpy((void *)(buff_udp + udp_idx), (void *)"BW125\"", 6);
                        udp_idx += 6;
                        header_size += 6;
                        break;
                    case BW_250KHZ:
                        memcpy((void *)(buff_udp + udp_idx), (void *)"BW250\"", 6);
                        udp_idx += 6;
                        header_size += 6;
                        break;
                    case BW_500KHZ:
                        memcpy((void *)(buff_udp + udp_idx), (void *)"BW500\"", 6);
                        udp_idx += 6;
                        header_size += 6;
                        break;
                    default:
                        MSG_ERROR("[up  ] lora packet with unknown bandwidth\n");
                        memcpy((void *)(buff_udp + udp_idx), (void *)"BW?\"", 4);
                        udp_idx += 4;
                        header_size += 4;
                        quit_sig = true;
                        machine_pygate_set_status(PYGATE_ERROR);
                }

                /* Packet ECC coding rate, 11-13 useful chars */
                switch (p->coderate) {
                    case CR_LORA_4_5:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"codr\":\"4/5\"", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    case CR_LORA_4_6:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"codr\":\"4/6\"", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    case CR_LORA_4_7:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"codr\":\"4/7\"", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    case CR_LORA_4_8:
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"codr\":\"4/8\"", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    case 0: /* treat the CR0 case (mostly false sync) */
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"codr\":\"OFF\"", 13);
                        udp_idx += 13;
                        header_size += 13;
                        break;
                    default:
                        MSG_ERROR("[up  ] lora packet with unknown coderate\n");
                        memcpy((void *)(buff_udp + udp_idx), (void *)",\"codr\":\"?\"", 11);
                        udp_idx += 11;
                        header_size += 11;
                        quit_sig = true;
                        machine_pygate_set_status(PYGATE_ERROR);
                }


            } else if (p->modulation == MOD_FSK) {
                memcpy((void *)(buff_up + buff_index), (void *)",\"modu\":\"FSK\"", 13);
                buff_index += 13;

                /* FSK datarate, 11-14 useful chars */
                j = snprintf((char *)(buff_up + buff_index), TX_BUFF_SIZE - buff_index, ",\"datr\":%u", p->datarate);
                if (j > 0) {
                    buff_index += j;
                } else {
                    MSG_ERROR("[up  ] snprintf failed line %u\n", (__LINE__ - 4));
                    quit_sig = true;
                    machine_pygate_set_status(PYGATE_ERROR);
                }
            } else {
                MSG_ERROR("[up  ] received packet with unknown modulation\n");
                quit_sig = true;
                machine_pygate_set_status(PYGATE_ERROR);
            }


            /* Terminate JSON array with packet data */
            buff_udp[udp_idx] = '}';
            header_size++;
            udp_idx++;

            /* Insert header size */
            buff_udp[2] = header_size;

            /* Copy in payload */
            memcpy(&buff_udp[udp_idx], p->payload, p->size);

            /* Send packet */
            if (sendto(udp_sock_desc, buff_udp, 256, 0, (struct sockaddr *) &udp_server_addr, sizeof(udp_server_addr)) < 0){
                MSG_INFO("Couldn't send data\n");
            } else {
                MSG_INFO("Sent successfully\n");
            }

            ++pkt_in_dgram;
        }
        wait_ms (5);
    }
    MSG_INFO("[up  ] End of upstream thread\n\n");
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 2: POLLING SERVER AND ENQUEUING PACKETS IN JIT QUEUE ---------- */

void thread_down(void) {
    MSG_INFO("[down] start\n");
    int i; /* loop variables */

    /* configuration and metadata for an outbound packet */
    struct lgw_pkt_tx_s txpkt;
    bool sent_immediate = false; /* option to sent the packet immediately */

    /* local timekeeping variables */
    struct timeval send_time; /* time of the pull request */
    struct timeval recv_time; /* time of return from recv socket call */

    /* data buffers */
    static uint8_t buff_down[1000]; /* buffer to receive downstream packets */
    uint8_t buff_req[12]; /* buffer to compose pull requests */
    int msg_len;

    /* protocol variables */
    uint8_t token_h; /* random token for acknowledgement matching */
    uint8_t token_l; /* random token for acknowledgement matching */
    bool req_ack = false; /* keep track of whether PULL_DATA was acknowledged or not */

    /* JSON parsing variables */
    JSON_Value *root_val = NULL;
    JSON_Object *txpk_obj = NULL;
    JSON_Value *val = NULL; /* needed to detect the absence of some fields */
    const char *str; /* pointer to sub-strings in the JSON data */
    short x0, x1;

    /* auto-quit variable */
    uint32_t autoquit_cnt = 0; /* count the number of PULL_DATA sent since the latest PULL_ACK */

    /* UDP socket variables */
    uint8_t tx_status;
    int result = LGW_HAL_SUCCESS;

    /* TOA val */
    uint32_t time_on_air;
    uint8_t buff_tx_stat[8];

    /* Just In Time downlink */
    struct timeval current_unix_time;
    struct timeval current_concentrator_time;
    enum jit_error_e jit_result = JIT_ERROR_OK;
    enum jit_pkt_type_e downlink_type;

    /* pre-fill the pull request buffer with fixed fields */
    buff_req[0] = PROTOCOL_VERSION;
    buff_req[3] = PKT_PULL_DATA;
    *(uint32_t *)(buff_req + 4) = net_mac_h;
    *(uint32_t *)(buff_req + 8) = net_mac_l;

    while (!exit_sig && !quit_sig) {

        /* auto-quit if the threshold is crossed */
        if ((autoquit_threshold > 0) && (autoquit_cnt >= autoquit_threshold)) {
            exit_sig = true;
            MSG_INFO("[down] the last %u PULL_DATA were not ACKed, exiting application\n", autoquit_threshold);
            break;
        }
        /* generate random token for request */
        token_h = (uint8_t)rand(); /* random token */
        token_l = (uint8_t)rand(); /* random token */
        buff_req[1] = token_h;
        buff_req[2] = token_l;

        /* send PULL request and record time */
        req_ack = false;
        autoquit_cnt++;

        /* listen to packets and process them until a new PULL request must be sent */
        recv_time = send_time;
        while ((int)time_diff(send_time, recv_time) < keepalive_time) {

            /* try to receive a datagram */
            /*msg_len = recv(sock_down, (void *)buff_down, (sizeof buff_down) - 1, 0); OLD*/

            /* Receive UDP packet */
            msg_len = recv(udp_sock_desc_down, (void *)buff_down, (sizeof buff_down),0);

            gettimeofday(&recv_time, NULL);
            //clock_gettime(CLOCK_MONOTONIC, &recv_time);


            /* if no network message was received, got back to listening sock_down socket */
            if (msg_len == -1) {
                //MSG_WARN("[down] recv returned %s\n", strerror(errno));  too verbose */
                continue;
            }


            /* initialize TX struct and try to parse JSON */
            memset(&txpkt, 0, sizeof txpkt);
            root_val = json_parse_string_with_comments((const char *)(buff_down)); /* JSON offset = 0 now */
            if (root_val == NULL) {
                MSG_WARN("[down] invalid JSON, TX aborted\n");
                continue;
            }

            /* look for JSON sub-object 'txpk' */
            txpk_obj = json_object_get_object(json_value_get_object(root_val), "txpk");
            if (txpk_obj == NULL) {
                MSG_WARN("[down] no \"txpk\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }

            /* Parse "immediate" tag, or target timestamp */
            /* removed */

            /* Parse "No CRC" flag (optional field) */
           /* val = json_object_get_value(txpk_obj, "ncrc");
            if (val != NULL) {
                txpkt.no_crc = (bool)json_value_get_boolean(val);
            }*/


            /* parse target frequency (mandatory) */
            val = json_object_get_value(txpk_obj, "freq");
            if (val == NULL) {
                MSG_WARN("[down] no mandatory \"txpk.freq\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            txpkt.freq_hz = (uint32_t)((double)(1.0e6) * json_value_get_number(val));

            /* parse RF chain used for TX (mandatory) */
            val = json_object_get_value(txpk_obj, "rfch");
            if (val == NULL) {
                MSG_WARN("[down] no mandatory \"txpk.rfch\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            txpkt.rf_chain = (uint8_t)json_value_get_number(val);

            /* parse TX power (optional field) */
            val = json_object_get_value(txpk_obj, "powe");
            if (val != NULL) {
                txpkt.rf_power = (int8_t)json_value_get_number(val) - antenna_gain;
            }

            /* Parse modulation (mandatory) */
            /*str = json_object_get_string(txpk_obj, "modu");
            if (str == NULL) {
                MSG_WARN("[down] no mandatory \"txpk.modu\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }*/

            /* Lora modulation */
            txpkt.modulation = MOD_LORA;

            /* Parse Lora spreading-factor and modulation bandwidth (mandatory) */
            str = json_object_get_string(txpk_obj, "datr");
            if (str == NULL) {
                MSG_WARN("[down] no mandatory \"txpk.datr\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            i = sscanf(str, "SF%2hdBW%3hd", &x0, &x1);
            if (i != 2) {
                MSG_WARN("[down] format error in \"txpk.datr\", TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            switch (x0) {
                case  7:
                    txpkt.datarate = DR_LORA_SF7;
                    break;
                case  8:
                    txpkt.datarate = DR_LORA_SF8;
                    break;
                case  9:
                    txpkt.datarate = DR_LORA_SF9;
                    break;
                case 10:
                    txpkt.datarate = DR_LORA_SF10;
                    break;
                case 11:
                    txpkt.datarate = DR_LORA_SF11;
                    break;
                case 12:
                    txpkt.datarate = DR_LORA_SF12;
                    break;
                default:
                    MSG_WARN("[down] format error in \"txpk.datr\", invalid SF, TX aborted\n");
                    json_value_free(root_val);
                    continue;
            }
            switch (x1) {
                case 125:
                    txpkt.bandwidth = BW_125KHZ;
                    break;
                case 250:
                    txpkt.bandwidth = BW_250KHZ;
                    break;
                case 500:
                    txpkt.bandwidth = BW_500KHZ;
                    break;
                default:
                    MSG_WARN("[down] format error in \"txpk.datr\", invalid BW, TX aborted\n");
                    json_value_free(root_val);
                    continue;
            }

            /* Parse ECC coding rate (optional field) */
            str = json_object_get_string(txpk_obj, "codr");
            if (str == NULL) {
                MSG_WARN("[down] no mandatory \"txpk.codr\" object in json, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            if      (strcmp(str, "4/5") == 0) {
                txpkt.coderate = CR_LORA_4_5;
            } else if (strcmp(str, "4/6") == 0) {
                txpkt.coderate = CR_LORA_4_6;
            } else if (strcmp(str, "2/3") == 0) {
                txpkt.coderate = CR_LORA_4_6;
            } else if (strcmp(str, "4/7") == 0) {
                txpkt.coderate = CR_LORA_4_7;
            } else if (strcmp(str, "4/8") == 0) {
                txpkt.coderate = CR_LORA_4_8;
            } else if (strcmp(str, "1/2") == 0) {
                txpkt.coderate = CR_LORA_4_8;
            } else {
                MSG_WARN("[down] format error in \"txpk.codr\", TX aborted\n");
                json_value_free(root_val);
                continue;
            }

            /* Parse signal polarity switch (optional field) */
            val = json_object_get_value(txpk_obj, "ipol");
            if (val != NULL) {
                txpkt.invert_pol = (bool)json_value_get_boolean(val);
            }

            /* parse Lora preamble length (optional field, optimum min value enforced) */
            val = json_object_get_value(txpk_obj, "prea");
            if (val != NULL) {
                i = (int)json_value_get_number(val);
                if (i >= MIN_LORA_PREAMB) {
                    txpkt.preamble = (uint16_t)i;
                } else {
                    txpkt.preamble = (uint16_t)MIN_LORA_PREAMB;
                }
            } else {
                txpkt.preamble = (uint16_t)STD_LORA_PREAMB;
            }



            /* Parse payload length (mandatory) */
            val = json_object_get_value(txpk_obj, "size");
            if (val == NULL) {
                MSG_WARN("[down] no mandatory \"txpk.size\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            txpkt.size = (uint16_t)json_value_get_number(val);

            /* Parse payload data (mandatory) */
            str = json_object_get_string(txpk_obj, "data");
            if (str == NULL) {
                MSG_WARN("[down] no mandatory \"txpk.data\" object in JSON, TX aborted\n");
                json_value_free(root_val);
                continue;
            }
            MSG_ERROR("[main] Payload to send from JSON: %s\n", str);
            i = b64_to_bin(str, strlen(str), txpkt.payload, sizeof txpkt.payload);
            if (i != txpkt.size) {
                MSG_WARN("[down] mismatch between .size and .data size once converter to binary\n");
            }
            //memcpy(&txpkt.payload, str, strlen(str));

            /* free the JSON parse tree from memory */
            json_value_free(root_val);

            /* select TX mode */
            txpkt.tx_mode = IMMEDIATE;

            /* record measurement data */
            pthread_mutex_lock(&mx_meas_dw);
            meas_dw_dgram_rcv += 1; /* count only datagrams with no JSON errors */
            meas_dw_network_byte += msg_len; /* meas_dw_network_byte */
            meas_dw_payload_byte += txpkt.size;
            pthread_mutex_unlock(&mx_meas_dw);

            /* check TX parameter before trying to queue packet */
            jit_result = JIT_ERROR_OK;
            if ((txpkt.freq_hz < tx_freq_min[txpkt.rf_chain]) || (txpkt.freq_hz > tx_freq_max[txpkt.rf_chain])) {
                jit_result = JIT_ERROR_TX_FREQ;
                MSG_ERROR("[down] Packet REJECTED, unsupported frequency - %u (min:%u,max:%u)\n", txpkt.freq_hz, tx_freq_min[txpkt.rf_chain], tx_freq_max[txpkt.rf_chain]);
            }
            if (jit_result == JIT_ERROR_OK) {
                for (i = 0; i < txlut.size; i++) {
                    if (txlut.lut[i].rf_power == txpkt.rf_power) {
                        /* this RF power is supported, we can continue */
                        break;
                    }
                }
                if (i == txlut.size) {
                    /* this RF power is not supported */
                    jit_result = JIT_ERROR_TX_POWER;
                    MSG_ERROR("[down] Packet REJECTED, unsupported RF power for TX - %d\n", txpkt.rf_power);
                }
            }

            /** TRY SENDING PACKET DIRECT?? **/
            /* check if concentrator is free for sending new packet */
            result = lgw_status(TX_STATUS, &tx_status);
            if (result == LGW_HAL_ERROR) {
            } else {
                if (tx_status == TX_EMITTING) {
                    MSG_WARN("[jit ] concentrator is currently busy\n");
                    // continue;
                } else if (tx_status == TX_SCHEDULED) {
                    MSG_WARN("[jit ] a downlink was already scheduled, overwritting it...\n");
                } else {
                    /* Nothing to do */
                }
            }

            /* send packet to concentrator */
            MSG_WARN("SENDING PACKET...\n");
            pthread_mutex_lock(&mx_concent); /* may have to wait for a fetch to finish */
            result = lgw_send(&txpkt);
            pthread_mutex_unlock(&mx_concent); /* free concentrator ASAP */
            if (result == LGW_HAL_ERROR) {
                pthread_mutex_lock(&mx_meas_dw);
                meas_nb_tx_fail += 1;
                pthread_mutex_unlock(&mx_meas_dw);
                MSG_WARN("[jit ] lgw_send failed\n");
                continue;
            } else {
                MSG_WARN("Packet sent...\n");

                /* Send tx info packet to python */
                time_on_air = lgw_time_on_air(&txpkt);
                buff_tx_stat[0] = TX_INFO_PACKET;
                /* Little endian */
                buff_tx_stat[1] = time_on_air & 0xFF;
                buff_tx_stat[2] = (time_on_air >> (8 * 1)) & 0xFF;
                buff_tx_stat[3] = (time_on_air >> (8 * 2)) & 0xFF;
                buff_tx_stat[4] = (time_on_air >> (8 * 3)) & 0xFF;
                /* Send packet */
                if (sendto(udp_sock_desc, buff_tx_stat, 8, 0, (struct sockaddr *) &udp_server_addr, sizeof(udp_server_addr)) < 0){
                    MSG_WARN("Couldn't send tx data\n");
                }

                pthread_mutex_lock(&mx_meas_dw);
                meas_nb_tx_ok += 1;
                pthread_mutex_unlock(&mx_meas_dw);
                MSG_DEBUG("[jit ] lgw_send done: count_us=%u\n", pkt.count_us);
            }
        }
        wait_ms(5);
    }
    MSG_INFO("[down] End of downstream thread\n\n");
}

void print_tx_status(uint8_t tx_status) {
    switch (tx_status) {
        case TX_OFF:
            MSG_INFO("[jit ] lgw_status returned TX_OFF\n");
            break;
        case TX_FREE:
            MSG_INFO("[jit ] lgw_status returned TX_FREE\n");
            break;
        case TX_EMITTING:
            MSG_INFO("[jit ] lgw_status returned TX_EMITTING\n");
            break;
        case TX_SCHEDULED:
            MSG_INFO("[jit ] lgw_status returned TX_SCHEDULED\n");
            break;
        default:
            MSG_INFO("[jit ] lgw_status returned UNKNOWN (%d)\n", tx_status);
            break;
    }
}
/* --- EOF ------------------------------------------------------------------ */
