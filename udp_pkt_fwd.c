#include "udp_pkt_fwd.h"

void serialize_float(char *insert_location, float float_to_insert){
    unsigned int asInt = *((int*)&f);
    int i;
    for (i = 0; i < 4; i++){
        *(insert_location + i) = (asInt >> 8 * i) & 0xFF;
    }
}