# EOSIO Kafka Plugin
EOSIO Kafka Plugin

## Requirements
###  install librdkafka
```
#cd /usr/local
#git clone https://github.com/edenhill/librdkafka.git
#cd librdkafka
#./configure
#make
#sudo make install
```

## Building the plugin [Install on your nodeos server]
```
#cd /usr/local/eos/plugins/
#git clone https://github.com/Dappub/kafka_plugin.git

edit /usr/local/eos/plugins/CMakeLists.txt:
#add_subdirectory(kafka_plugin)

edit /usr/local/eos/programs/nodeos/CMakeLists.txt:
#target_link_libraries( nodeos PRIVATE -Wl,${whole_archive_flag} kafka_plugin -Wl,${no_whole_archive_flag} )

edit /usr/local/eos/eosio_build.sh:
cmake -DBUILD_KAFKA_PLUGIN=true 
```
## How to setup on your nodeos
Enable this plugin using --plugin option to nodeos or in your config.ini. Use nodeos --help to see options used by this plugin.

## Configuration
Add the following to config.ini to enable the plugin:
```
parmeters for kafka_plugin
# --plugin eosio::kafka_plugin
# --kafka-uri 127.0.0.1:9092
# --kafka-mongodb-uri 127.0.0.1:27017
# --accepted_trx_topic eos_accepted_trx_topic
# --applied_trx_topic eos_applied_trx_topic
# --irreversible_block_topic eos_irreversible_block_topic
# --kafka-block-start 0
# --kafka-queue-size 4096
# --kafka-abi-cache-size 4096
# --kafka-mongodb-wipe false
# --kafka-filter-on *
# --kafka-filter-out blocktwitter::
```
