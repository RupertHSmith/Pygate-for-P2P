#include <stdint.h>     /* C99 types */

struct udp_packet_structure {
    uint8_t payload_size;
    float rssi;
    float snr;
    uint8_t channel;
    uint8_t sf;
    uint8_t payload[128];
};

void serialize_float(char *insert_location, float float_to_insert);

/* sort this */
void serialize_float(char *insert_location, float float_to_insert){
    unsigned int asInt = *((int*)&float_to_insert);
    int i;
    for (i = 0; i < 4; i++){
        *(insert_location + i) = (asInt >> 8 * i) & 0xFF;
    }
}