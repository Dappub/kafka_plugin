# EOSIO Kafka Plugin

Decouple the processing of eosio on-chain data, inspired by [mongo_db_plugin](https://github.com/EOSIO/eos/tree/master/plugins/mongo_db_plugin) and [TP-Lab/kafka_plugin](https://github.com/TP-Lab/kafka_plugin).

## Requirements
- EOSIO nodes v1.6.3 and up
- librdkafka
- mongo-cxx-driver
- mongodb

## Installation

###  Install librdkafka
```
cd /usr/local
git clone https://github.com/edenhill/librdkafka.git
cd librdkafka
./configure
make
sudo make install
```

### Install MongoDB and mongo-cxx-driver
See: https://developers.eos.io/eosio-nodeos/docs/clean-install-centos-7-and-higher

### Build and install Nodeos with kafka_plugin
```
cd /usr/local/eos/plugins/
git clone https://github.com/Dappub/kafka_plugin.git

edit /usr/local/eos/plugins/CMakeLists.txt:
add_subdirectory(kafka_plugin)

edit /usr/local/eos/programs/nodeos/CMakeLists.txt:
target_link_libraries( nodeos PRIVATE -Wl,${whole_archive_flag} kafka_plugin -Wl,${no_whole_archive_flag} )

edit /usr/local/eos/eosio_build.sh:
cmake -DBUILD_KAFKA_PLUGIN=true

./eosio_build.sh
sudo ./eosio_install.sh
```

## Nodeos configuration
Add the following to your Nodeos config.ini:
```
# kafka_plugin configs
plugin = eosio::kafka_plugin
kafka-uri = 127.0.0.1:9092
accepted_trx_topic = eos_accepted_trx
applied_trx_topic = eos_applied_trx
accepted_block_topic = eos_accepted_block
irreversible_block_topic = eos_irreversible_block
kafka-block-start = 0
kafka-queue-size = 4096
kafka-abi-cache-size = 40960
kafka-mongodb-uri = mongodb://localhost:27017/EOS-kafka-plugin
kafka-mongodb-wipe = false
abi-serializer-max-time-ms = 500000  #  a large enough value is recommended to serialize large blocks
# kafka-filter-on *
# kafka-filter-out blocktwitter::
```

## Message Examples
(todo)

## Change Log
v1.0.0:
- Initial release
- chain_plugin channels: 
    - accepted_block_connection
    - irreversible_block_connection
    - accepted_transaction_connection
    - applied_transaction_connection
- abi deserialized data
- filters same as [mongo_db_plugin](https://developers.eos.io/eosio-nodeos/docs/mongo_db_plugin#section-example-filters)
