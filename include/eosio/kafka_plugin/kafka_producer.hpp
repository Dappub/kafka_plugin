#pragma once

#include "rdkafka.h"

namespace eosio {
#define KAFKA_STATUS_OK 0
#define KAFKA_STATUS_INIT_FAIL 1
#define KAFKA_STATUS_MSG_INVALID 2
#define KAFKA_STATUS_QUEUE_FULL 3

#define KAFKA_TRX_ACCEPTED 0
#define KAFKA_TRX_APPLIED 1
#define KAFKA_BLOCK_ACCEPTED 2
#define KAFKA_BLOCK_IRREVERSIBLE 3

class kafka_producer {
    public:
        kafka_producer() {

            trx_accepted_rk = NULL;
            trx_applied_rk = NULL;
            block_accepted_rk = NULL;
            block_irreversible_rk = NULL;
            trx_accepted_rkt = NULL;
            trx_applied_rkt = NULL;
            block_accepted_rkt = NULL;
            block_irreversible_rkt = NULL;
            trx_accepted_conf = NULL;
            trx_applied_conf = NULL;
            block_accepted_conf = NULL;
            block_irreversible_conf = NULL;
        };

        int kafka_init(char *brokers, char *trx_accepted_topic, char *trx_applied_topic,
                char *block_accepted_topic, char *block_irreversible_topic);

        int kafka_sendmsg(int trxtype, char *msgstr);

        int kafka_destroy(void);

    private:
        rd_kafka_t *trx_accepted_rk;                /*Producer instance handle*/
        rd_kafka_t *trx_applied_rk;                 /*Producer instance handle*/
        rd_kafka_t *block_accepted_rk;              /*Producer instance handle*/
        rd_kafka_t *block_irreversible_rk;          /*Producer instance handle*/
        rd_kafka_topic_t *trx_accepted_rkt;         /*topic object*/
        rd_kafka_topic_t *trx_applied_rkt;          /*topic object*/
        rd_kafka_topic_t *block_accepted_rkt;       /*topic object*/
        rd_kafka_topic_t *block_irreversible_rkt;   /*topic object*/
        rd_kafka_conf_t *trx_accepted_conf;         /*kafka config*/
        rd_kafka_conf_t *trx_applied_conf;          /*kafka config*/
        rd_kafka_conf_t *block_accepted_conf;       /*kafka config*/
        rd_kafka_conf_t *block_irreversible_conf;   /*kafka config*/

        static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque){}
    };
}

