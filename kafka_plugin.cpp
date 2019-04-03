/**
 *
 */
#include <stdlib.h>
#include <eosio/kafka_plugin/kafka_producer.hpp>
#include <eosio/kafka_plugin/kafka_plugin.hpp>

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <queue>

#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>

namespace fc { class variant; }

namespace eosio {

    using chain::account_name;
    using chain::action_name;
    using chain::block_id_type;
    using chain::permission_name;
    using chain::transaction;
    using chain::signed_transaction;
    using chain::signed_block;
    using chain::transaction_id_type;
    using chain::packed_transaction;

    static appbase::abstract_plugin& _kafka_plugin = app().register_plugin<kafka_plugin>();
    using kafka_producer_ptr = std::shared_ptr<class kafka_producer>;

    struct filter_entry {
        name receiver;
        name action;
        name actor;

        friend bool operator<( const filter_entry& a, const filter_entry& b ) {
            return std::tie( a.receiver, a.action, a.actor ) < std::tie( b.receiver, b.action, b.actor );
        }

        //            receiver          action       actor
        bool match( const name& rr, const name& an, const name& ar ) const {
            return (receiver.value == 0 || receiver == rr) &&
                   (action.value == 0 || action == an) &&
                   (actor.value == 0 || actor == ar);
        }
    };

    class kafka_plugin_impl {
    public:
        kafka_plugin_impl();
        ~kafka_plugin_impl();

        fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
        fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
        fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
        fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;
        chain_plugin *chain_plug;

        void consume_blocks();

        void accepted_block(const chain::block_state_ptr &);
        void applied_irreversible_block(const chain::block_state_ptr &);
        void accepted_transaction(const chain::transaction_metadata_ptr &);
        void applied_transaction(const chain::transaction_trace_ptr &);
        void process_accepted_transaction(const chain::transaction_metadata_ptr &);
        void _process_accepted_transaction(const chain::transaction_metadata_ptr &);
        void process_applied_transaction(const chain::transaction_trace_ptr &);
        void _process_applied_transaction(const chain::transaction_trace_ptr &);
        void process_accepted_block(const chain::block_state_ptr &);
        void _process_accepted_block(const chain::block_state_ptr &);
        void process_irreversible_block(const chain::block_state_ptr &);
        void _process_irreversible_block(const chain::block_state_ptr &);

        optional<abi_serializer> get_abi_serializer( account_name n );
        template<typename T> fc::variant to_variant_with_abi( const T& obj );

        void purge_abi_cache();

        bool add_action_trace( const chain::action_trace& atrace,
                               const chain::transaction_trace_ptr& t,
                               bool executed );

        void db_update_account(const chain::action& act);

        /// @return true if act should be added to mongodb, false to skip it
        bool filter_include( const account_name& receiver, const action_name& act_name,
                             const vector<chain::permission_level>& authorization ) const;
        bool filter_include( const transaction& trx ) const;

        void init();
        void wipe_database();

        bool configured{false};
        bool wipe_database_on_startup{false};
        uint32_t start_block_num = 0;
        std::atomic_bool start_block_reached{false};

        bool filter_on_star = true;
        std::set<filter_entry> filter_on;
        std::set<filter_entry> filter_out;
        bool send_accepted_block = false;
        bool send_irreversible_block = false;
        bool send_accepted_transaction = false;
        bool send_applied_transaction = false;

        std::string db_name;
        fc::optional<mongocxx::pool> mongo_pool;
        mongocxx::collection _accounts;

        size_t queue_size = 0;
        size_t abi_cache_size = 0;
        std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
        std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
        std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
        std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
        std::deque<chain::block_state_ptr> block_state_queue;
        std::deque<chain::block_state_ptr> block_state_process_queue;
        std::deque<chain::block_state_ptr> irreversible_block_state_queue;
        std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;
        boost::mutex mtx;
        boost::condition_variable condition;
        boost::thread consume_thread;
        boost::atomic<bool> done{false};
        boost::atomic<bool> startup{true};
        fc::optional<chain::chain_id_type> chain_id;
        fc::microseconds abi_serializer_max_time;

        struct by_account;
        struct by_last_access;

        struct abi_cache {
            account_name                     account;
            fc::time_point                   last_accessed;
            fc::optional<abi_serializer>     serializer;
        };

        typedef boost::multi_index_container<abi_cache,
            indexed_by<
                ordered_unique< tag<by_account>,  member<abi_cache,account_name,&abi_cache::account> >,
                ordered_non_unique< tag<by_last_access>,  member<abi_cache,fc::time_point,&abi_cache::last_accessed> >
            >
        > abi_cache_index_t;

        abi_cache_index_t abi_cache_index;

        static const action_name newaccount;
        static const action_name setabi;
        static const action_name updateauth;
        static const action_name deleteauth;
        static const permission_name owner;
        static const permission_name active;

        static const std::string block_states_col;
        static const std::string blocks_col;
        static const std::string trans_col;
        static const std::string trans_traces_col;
        static const std::string action_traces_col;
        static const std::string accounts_col;
        static const std::string pub_keys_col;
        static const std::string account_controls_col;
        kafka_producer_ptr producer;
    };

    const action_name kafka_plugin_impl::newaccount = chain::newaccount::get_name();
    const action_name kafka_plugin_impl::setabi = chain::setabi::get_name();
    const action_name kafka_plugin_impl::updateauth = chain::updateauth::get_name();
    const action_name kafka_plugin_impl::deleteauth = chain::deleteauth::get_name();
    const permission_name kafka_plugin_impl::owner = chain::config::owner_name;
    const permission_name kafka_plugin_impl::active = chain::config::active_name;

    const std::string kafka_plugin_impl::block_states_col = "block_states";
    const std::string kafka_plugin_impl::blocks_col = "blocks";
    const std::string kafka_plugin_impl::trans_col = "transactions";
    const std::string kafka_plugin_impl::trans_traces_col = "transaction_traces";
    const std::string kafka_plugin_impl::action_traces_col = "action_traces";
    const std::string kafka_plugin_impl::accounts_col = "accounts";
    const std::string kafka_plugin_impl::pub_keys_col = "pub_keys";
    const std::string kafka_plugin_impl::account_controls_col = "account_controls";

    bool kafka_plugin_impl::filter_include( const account_name& receiver, const action_name& act_name,
                                            const vector<chain::permission_level>& authorization ) const {
        bool include = false;
        if( filter_on_star ) {
            include = true;
        } else {
            auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name]( const auto& filter ) {
                return filter.match( receiver, act_name, 0 );
            } );
            if( itr != filter_on.cend() ) {
                include = true;
            } else {
                for( const auto& a : authorization ) {
                    auto itr = std::find_if( filter_on.cbegin(), filter_on.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
                        return filter.match( receiver, act_name, a.actor );
                    } );
                    if( itr != filter_on.cend() ) {
                        include = true;
                        break;
                    }
                }
            }
        }

        if( !include ) { return false; }
        if( filter_out.empty() ) { return true; }

        auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name]( const auto& filter ) {
            return filter.match( receiver, act_name, 0 );
        } );
        if( itr != filter_out.cend() ) { return false; }

        for( const auto& a : authorization ) {
            auto itr = std::find_if( filter_out.cbegin(), filter_out.cend(), [&receiver, &act_name, &a]( const auto& filter ) {
                return filter.match( receiver, act_name, a.actor );
            } );
            if( itr != filter_out.cend() ) { return false; }
        }

        return true;
    }

    bool kafka_plugin_impl::filter_include( const transaction& trx ) const
    {
        if( !filter_on_star || !filter_out.empty() ) {
            bool include = false;
            for( const auto& a : trx.actions ) {
                if( filter_include( a.account, a.name, a.authorization ) ) {
                    include = true;
                    break;
                }
            }
            if( !include ) {
                for( const auto& a : trx.context_free_actions ) {
                    if( filter_include( a.account, a.name, a.authorization ) ) {
                        include = true;
                        break;
                    }
                }
            }
            return include;
        }
        return true;
    }

    namespace {

        template<typename Queue, typename Entry>
        void queue(boost::mutex &mtx, boost::condition_variable &condition, Queue &queue, const Entry &e,
                   size_t queue_size) {
            int sleep_time = 100;
            size_t last_queue_size = 0;
            boost::mutex::scoped_lock lock(mtx);
            if (queue.size() > queue_size) {
                lock.unlock();
                condition.notify_one();
                if (last_queue_size < queue.size()) {
                    sleep_time += 100;
                } else {
                    sleep_time -= 100;
                    if (sleep_time < 0) sleep_time = 100;
                }
                last_queue_size = queue.size();
                boost::this_thread::sleep_for(boost::chrono::milliseconds(sleep_time));
                lock.lock();
            }
            queue.emplace_back(e);
            lock.unlock();
            condition.notify_one();
        }

    }

    void kafka_plugin_impl::accepted_transaction(const chain::transaction_metadata_ptr &t) {
        try {
            if ( send_accepted_transaction ) {
                queue(mtx, condition, transaction_metadata_queue, t, queue_size);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while accepted_transaction");
        }
    }

    void kafka_plugin_impl::applied_transaction(const chain::transaction_trace_ptr &t) {
        try {
            if( !t->producer_block_id.valid() )
                return;
            queue(mtx, condition, transaction_trace_queue, t, queue_size);
        } catch (fc::exception &e) {
            elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while applied_transaction");
        }
    }

    void kafka_plugin_impl::applied_irreversible_block(const chain::block_state_ptr &bs) {
        try {
            if ( send_irreversible_block ) {
                queue(mtx, condition, irreversible_block_state_queue, bs, queue_size);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while applied_irreversible_block");
        }
    }

    void kafka_plugin_impl::accepted_block(const chain::block_state_ptr &bs) {
        try {
            if( !start_block_reached ) {
                if( bs->block_num >= start_block_num ) {
                    start_block_reached = true;
                }
            }
            if ( send_accepted_block ) {
                queue(mtx, condition, block_state_queue, bs, queue_size);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while accepted_block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while accepted_block");
        }
    }

    void kafka_plugin_impl::consume_blocks() {
        try {
            auto mongo_client = mongo_pool->acquire();
            auto& mongo_conn = *mongo_client;

            _accounts = mongo_conn[db_name][accounts_col];

            while (true) {
                boost::mutex::scoped_lock lock(mtx);
                while (transaction_metadata_queue.empty() &&
                       transaction_trace_queue.empty() &&
                       block_state_queue.empty() &&
                       irreversible_block_state_queue.empty() &&
                       !done) {
                    condition.wait(lock);
                }
                // capture for processing
                size_t transaction_metadata_size = transaction_metadata_queue.size();
                if (transaction_metadata_size > 0) {
                    transaction_metadata_process_queue = move(transaction_metadata_queue);
                    transaction_metadata_queue.clear();
                }
                size_t transaction_trace_size = transaction_trace_queue.size();
                if (transaction_trace_size > 0) {
                    transaction_trace_process_queue = move(transaction_trace_queue);
                    transaction_trace_queue.clear();
                }

                size_t block_state_size = block_state_queue.size();
                if (block_state_size > 0) {
                    block_state_process_queue = move(block_state_queue);
                    block_state_queue.clear();
                }
                size_t irreversible_block_size = irreversible_block_state_queue.size();
                if (irreversible_block_size > 0) {
                    irreversible_block_state_process_queue = move(irreversible_block_state_queue);
                    irreversible_block_state_queue.clear();
                }

                lock.unlock();

                // warn if queue size greater than 75%
                if (transaction_metadata_size > (queue_size * 0.75) ||
                        transaction_trace_size > (queue_size * 0.75) ||
                        block_state_size > (queue_size * 0.75) ||
                        irreversible_block_size > (queue_size * 0.75)) {
                    wlog("transaction_metadata queue size: ${q}", ("q", transaction_metadata_size));
                    wlog("transaction_trace queue size: ${q}", ("q", transaction_trace_size));
                    wlog("block_state queue size: ${q}", ("q", block_state_size));
                    wlog("irreversible_block queue size: ${q}", ("q", irreversible_block_size));
                } else if (done) {
                    ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
                }

                // process transactions
                while (!transaction_metadata_process_queue.empty()) {
                    const auto &t = transaction_metadata_process_queue.front();
                    process_accepted_transaction(t);
                    transaction_metadata_process_queue.pop_front();
                }

                while (!transaction_trace_process_queue.empty()) {
                    const auto &t = transaction_trace_process_queue.front();
                    process_applied_transaction(t);
                    transaction_trace_process_queue.pop_front();
                }

                // process blocks
                while (!block_state_process_queue.empty()) {
                    const auto &bs = block_state_process_queue.front();
                    process_accepted_block(bs);
                    block_state_process_queue.pop_front();
                }

                // process irreversible blocks
                while (!irreversible_block_state_process_queue.empty()) {
                    const auto &bs = irreversible_block_state_process_queue.front();
                    process_irreversible_block(bs);
                    irreversible_block_state_process_queue.pop_front();
                }

                if (transaction_metadata_size == 0 &&
                    transaction_trace_size == 0 &&
                    block_state_size == 0 &&
                    irreversible_block_size == 0 &&
                    done) {
                    break;
                }
            }
            ilog("kafka_plugin consume thread shutdown gracefully");
        } catch (fc::exception &e) {
            elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while consuming block ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while consuming block");
        }
    }

    namespace {

        auto find_account( mongocxx::collection& accounts, const account_name& name ) {
            using bsoncxx::builder::basic::make_document;
            using bsoncxx::builder::basic::kvp;
            return accounts.find_one( make_document( kvp( "name", name.to_string())));
        }

        void handle_kafka_exception( const std::string& desc, int line_num ) {
            bool shutdown = true;
            try {
                try {
                    throw;
                } catch( mongocxx::logic_error& e) {
                    // logic_error on invalid key, do not shutdown
                    wlog( "mongo logic error, ${desc}, line ${line}, code ${code}, ${what}",
                          ("desc", desc)( "line", line_num )( "code", e.code().value() )( "what", e.what() ));
                    shutdown = false;
                } catch( mongocxx::operation_exception& e) {
                    elog( "mongo exception, ${desc}, line ${line}, code ${code}, ${details}",
                          ("desc", desc)( "line", line_num )( "code", e.code().value() )( "details", e.code().message() ));
                    if (e.raw_server_error()) {
                        elog( "  raw_server_error: ${e}", ( "e", bsoncxx::to_json(e.raw_server_error()->view())));
                    }
                } catch( mongocxx::exception& e) {
                    elog( "mongo exception, ${desc}, line ${line}, code ${code}, ${what}",
                          ("desc", desc)( "line", line_num )( "code", e.code().value() )( "what", e.what() ));
                } catch( bsoncxx::exception& e) {
                    elog( "bsoncxx exception, ${desc}, line ${line}, code ${code}, ${what}",
                          ("desc", desc)( "line", line_num )( "code", e.code().value() )( "what", e.what() ));
                } catch( fc::exception& er ) {
                    elog( "kafka fc exception, ${desc}, line ${line}, ${details}",
                          ("desc", desc)( "line", line_num )( "details", er.to_detail_string()));
                } catch( const std::exception& e ) {
                    elog( "kafka std exception, ${desc}, line ${line}, ${what}",
                          ("desc", desc)( "line", line_num )( "what", e.what()));
                } catch( ... ) {
                    elog( "kafka unknown exception, ${desc}, line ${line_nun}", ("desc", desc)( "line_num", line_num ));
                }
            } catch (...) {
                std::cerr << "Exception attempting to handle exception for " << desc << " " << line_num << std::endl;
            }

            if( shutdown ) {
                // shutdown if failed to provide opportunity to fix issue and restart
                app().quit();
            }
        }

    } // anonymous namespace

    void kafka_plugin_impl::purge_abi_cache() {
        if( abi_cache_index.size() < abi_cache_size ) return;

        // remove the oldest (smallest) last accessed
        auto& idx = abi_cache_index.get<by_last_access>();
        auto itr = idx.begin();
        if( itr != idx.end() ) {
            idx.erase( itr );
        }
    }

    optional<abi_serializer> kafka_plugin_impl::get_abi_serializer( account_name n ) {
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;
        if( n.good()) {
            try {

                auto itr = abi_cache_index.find( n );
                if( itr != abi_cache_index.end() ) {
                    abi_cache_index.modify( itr, []( auto& entry ) {
                        entry.last_accessed = fc::time_point::now();
                    });

                    return itr->serializer;
                }

                auto account = _accounts.find_one( make_document( kvp("name", n.to_string())) );
                if(account) {
                    auto view = account->view();
                    abi_def abi;
                    if( view.find( "abi" ) != view.end()) {
                        try {
                            abi = fc::json::from_string( bsoncxx::to_json( view["abi"].get_document())).as<abi_def>();
                        } catch (...) {
                            ilog( "Unable to convert account abi to abi_def for ${n}", ( "n", n ));
                            return optional<abi_serializer>();
                        }

                        purge_abi_cache(); // make room if necessary
                        abi_cache entry;
                        entry.account = n;
                        entry.last_accessed = fc::time_point::now();
                        abi_serializer abis;
                        if( n == chain::config::system_account_name ) {
                            // redefine eosio setabi.abi from bytes to abi_def
                            // Done so that abi is stored as abi_def in mongo instead of as bytes
                            auto itr = std::find_if( abi.structs.begin(), abi.structs.end(),
                                                     []( const auto& s ) { return s.name == "setabi"; } );
                            if( itr != abi.structs.end() ) {
                                auto itr2 = std::find_if( itr->fields.begin(), itr->fields.end(),
                                                          []( const auto& f ) { return f.name == "abi"; } );
                                if( itr2 != itr->fields.end() ) {
                                    if( itr2->type == "bytes" ) {
                                        itr2->type = "abi_def";
                                        // unpack setabi.abi as abi_def instead of as bytes
                                        abis.add_specialized_unpack_pack( "abi_def",
                                                                          std::make_pair<abi_serializer::unpack_function, abi_serializer::pack_function>(
                                                                                  []( fc::datastream<const char*>& stream, bool is_array, bool is_optional ) -> fc::variant {
                                                                                      EOS_ASSERT( !is_array && !is_optional, chain::mongo_db_exception, "unexpected abi_def");
                                                                                      chain::bytes temp;
                                                                                      fc::raw::unpack( stream, temp );
                                                                                      return fc::variant( fc::raw::unpack<abi_def>( temp ) );
                                                                                  },
                                                                                  []( const fc::variant& var, fc::datastream<char*>& ds, bool is_array, bool is_optional ) {
                                                                                      EOS_ASSERT( false, chain::mongo_db_exception, "never called" );
                                                                                  }
                                                                          ) );
                                    }
                                }
                            }
                        }
                        // mongo does not like empty json keys
                        // make abi_serializer use empty_name instead of "" for the action data
                        for( auto& s : abi.structs ) {
                            if( s.name.empty() ) {
                                s.name = "empty_struct_name";
                            }
                            for( auto& f : s.fields ) {
                                if( f.name.empty() ) {
                                    f.name = "empty_field_name";
                                }
                            }
                        }
                        abis.set_abi( abi, abi_serializer_max_time );
                        entry.serializer.emplace( std::move( abis ) );
                        abi_cache_index.insert( entry );
                        return entry.serializer;
                    }
                }
            } FC_CAPTURE_AND_LOG((n))
        }
        return optional<abi_serializer>();
    }

    template<typename T>
    fc::variant kafka_plugin_impl::to_variant_with_abi( const T& obj ) {
        fc::variant pretty_output;
        abi_serializer::to_variant( obj, pretty_output,
                                    [&]( account_name n ) { return get_abi_serializer( n ); },
                                    abi_serializer_max_time );
        return pretty_output;
    }

    void kafka_plugin_impl::process_accepted_transaction(const chain::transaction_metadata_ptr &t) {
        try {
            if ( start_block_reached ) {
                _process_accepted_transaction( t );
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing accepted transaction metadata");
        }
    }

    void kafka_plugin_impl::process_applied_transaction(const chain::transaction_trace_ptr &t) {
        try {
            // always call since we need to capture setabi on accounts even if not storing transaction traces
            _process_applied_transaction( t );
        } catch (fc::exception &e) {
            elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing applied transaction trace");
        }
    }

    void kafka_plugin_impl::process_irreversible_block(const chain::block_state_ptr &bs) {
        try {
            if (start_block_reached) {
                _process_irreversible_block(bs);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing irreversible block");
        }
    }

    void kafka_plugin_impl::process_accepted_block(const chain::block_state_ptr &bs) {
        try {
            if (start_block_reached) {
                _process_accepted_block(bs);
            }
        } catch (fc::exception &e) {
            elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
        } catch (std::exception &e) {
            elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
        } catch (...) {
            elog("Unknown exception while processing accepted block trace");
        }
    }

    void kafka_plugin_impl::_process_accepted_transaction(const chain::transaction_metadata_ptr &t)
    {
        const signed_transaction& trx = t->packed_trx->get_signed_transaction();

        if( !filter_include( trx ) ) return;

        auto v = to_variant_with_abi( trx );
        string trx_json = fc::json::to_string( v );

        try {
            producer->kafka_sendmsg(KAFKA_TRX_ACCEPTED, (char*)trx_json.c_str());
        } catch (...) {
            handle_kafka_exception("send accepted_trx: ", __LINE__);
        }
    }

    bool kafka_plugin_impl::add_action_trace( const chain::action_trace& atrace, const chain::transaction_trace_ptr& t, bool executed )
    {
        if( executed && atrace.receipt.receiver == chain::config::system_account_name ) {
            db_update_account( atrace.act );
        }

        bool added = false;
        if( start_block_reached && filter_include( atrace.receipt.receiver, atrace.act.name, atrace.act.authorization ) ) {
            added = true;
        }

        for( const auto& inline_atrace : atrace.inline_traces ) {
            added |= add_action_trace( inline_atrace, t, executed );
        }

        return added;
    }

    void kafka_plugin_impl::_process_applied_transaction(const chain::transaction_trace_ptr &t)
    {
        bool write_atraces = false;
        bool executed = t->receipt.valid() && t->receipt->status == chain::transaction_receipt_header::executed;

        for( const auto& atrace : t->action_traces ) {
            try {
                write_atraces |= add_action_trace( atrace, t, executed );
            } catch(...) {
                handle_kafka_exception("add action traces", __LINE__);
            }
        }

        if( !start_block_reached ) return; //< add_action_trace calls db_update_account which must be called always
        if( !write_atraces ) return; //< do not send applied_transaction message if all action_traces filtered out

        if ( send_applied_transaction ) {
            auto v = to_variant_with_abi( *t );
            string trx_json = fc::json::to_string( v );
            try {
                producer->kafka_sendmsg(KAFKA_TRX_APPLIED, (char*)trx_json.c_str());
            } catch (...) {
                handle_kafka_exception("send applied_trx: ", __LINE__);
            }
        }
    }

    void kafka_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs )
    {
        const auto block_id = bs->block->id();
        const auto block_id_str = block_id.str();
        const auto block_num = bs->block->block_num();

        auto v = to_variant_with_abi( *bs->block );
        auto block_json = fc::json::to_string( v );

        string accepted_block_json = "{\"block_id\":\"" + block_id_str + "\"" +
                                     ",\"block_num\":" + std::to_string(block_num) +
                                     ",\"block\":" + block_json +
                                     "}";

        try {
            producer->kafka_sendmsg(KAFKA_BLOCK_ACCEPTED, (char*)accepted_block_json.c_str());
        } catch (...) {
            handle_kafka_exception("send accepted_block: ", __LINE__);
        }
    }

    void kafka_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs)
    {
        const auto block_id = bs->block->id();
        const auto block_id_str = block_id.str();
        const auto block_num = bs->block->block_num();

        auto v = to_variant_with_abi( *bs->block );
        auto block_json = fc::json::to_string( v );

        string irreversible_block_json = "{\"block_id\":\"" + block_id_str + "\"" +
                                         ",\"block_num\":" + std::to_string(block_num) +
                                         ",\"block\":" + block_json +
                                         "}";

        try {
            producer->kafka_sendmsg(KAFKA_BLOCK_IRREVERSIBLE, (char*)irreversible_block_json.c_str());
        } catch (...) {
            handle_kafka_exception("send irreversible_block: ", __LINE__);
        }
    }

    namespace {

        void create_account( mongocxx::collection& accounts, const name& name, std::chrono::milliseconds& now ) {
            using namespace bsoncxx::types;
            using bsoncxx::builder::basic::kvp;
            using bsoncxx::builder::basic::make_document;

            mongocxx::options::update update_opts{};
            update_opts.upsert( true );

            const string name_str = name.to_string();
            auto update = make_document(
                    kvp( "$set", make_document( kvp( "name", name_str),
                                                kvp( "createdAt", b_date{now} ))));
            try {
                if( !accounts.update_one( make_document( kvp( "name", name_str )), update.view(), update_opts )) {
                    EOS_ASSERT( false, chain::mongo_db_update_fail, "Failed to insert account ${n}", ("n", name));
                }
            } catch (...) {
                handle_kafka_exception( "create_account", __LINE__ );
            }
        }

    }

    void kafka_plugin_impl::db_update_account(const chain::action& act)
    {
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;
        using namespace bsoncxx::types;

        if (act.account != chain::config::system_account_name)
            return;

        try {
            if( act.name == newaccount ) {
                std::chrono::milliseconds now = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );
                auto newacc = act.data_as<chain::newaccount>();

                create_account( _accounts, newacc.name, now );

            } else if( act.name == setabi ) {
                auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );
                auto setabi = act.data_as<chain::setabi>();

                abi_cache_index.erase( setabi.account );

                auto account = find_account( _accounts, setabi.account );
                if( !account ) {
                    create_account( _accounts, setabi.account, now );
                    account = find_account( _accounts, setabi.account );
                }
                if( account ) {
                    abi_def abi_def = fc::raw::unpack<chain::abi_def>( setabi.abi );
                    const string json_str = fc::json::to_string( abi_def );

                    try{
                        auto update_from = make_document(
                                kvp( "$set", make_document( kvp( "abi", bsoncxx::from_json( json_str )),
                                                            kvp( "updatedAt", b_date{now} ))));

                        try {
                            if( !_accounts.update_one( make_document( kvp( "_id", account->view()["_id"].get_oid())),
                                                       update_from.view())) {
                                EOS_ASSERT( false, chain::mongo_db_update_fail, "Failed to udpdate account ${n}", ("n", setabi.account));
                            }
                        } catch( ... ) {
                            handle_kafka_exception( "account update", __LINE__ );
                        }
                    } catch( bsoncxx::exception& e ) {
                        elog( "Unable to convert abi JSON to MongoDB JSON: ${e}", ("e", e.what()));
                        elog( "  JSON: ${j}", ("j", json_str));
                    }
                }
            }
        } catch( fc::exception& e ) {
            // if unable to unpack native type, skip account creation
        }
    }

    kafka_plugin_impl::kafka_plugin_impl()
    :producer(new kafka_producer)
    {
    }

    kafka_plugin_impl::~kafka_plugin_impl() {
       if (!startup) {
          try {
             ilog( "kafka_db_plugin shutdown in process please be patient this can take a few minutes" );
             done = true;
             condition.notify_one();

             consume_thread.join();

             mongo_pool.reset();
             producer->kafka_destroy();
          } catch( std::exception& e ) {
             elog( "Exception on kafka_plugin shutdown of consume thread: ${e}", ("e", e.what()));
          }
       }
    }

    void kafka_plugin_impl::wipe_database() {
        ilog("mongo db wipe_database");

        auto client = mongo_pool->acquire();
        auto& mongo_conn = *client;

        auto accounts = mongo_conn[db_name][accounts_col];

        accounts.drop();
        ilog("done wipe_database");
    }

    void kafka_plugin_impl::init() {
        using namespace bsoncxx::types;
        using bsoncxx::builder::basic::make_document;
        using bsoncxx::builder::basic::kvp;
        // Create the native contract accounts manually; sadly, we can't run their contracts to make them create themselves
        // See native_contract_chain_initializer::prepare_database()

        ilog("init kafka-mongo");
        try {
            auto client = mongo_pool->acquire();
            auto& mongo_conn = *client;

            auto accounts = mongo_conn[db_name][accounts_col];
            if( accounts.count( make_document()) == 0 ) {
                auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::microseconds{fc::time_point::now().time_since_epoch().count()} );

                auto doc = make_document( kvp( "name", name( chain::config::system_account_name ).to_string()),
                                          kvp( "createdAt", b_date{now} ));

                try {
                    if( !accounts.insert_one( doc.view())) {
                        EOS_ASSERT( false, chain::mongo_db_insert_fail, "Failed to insert account ${n}",
                                    ("n", name( chain::config::system_account_name ).to_string()));
                    }
                } catch (...) {
                    handle_kafka_exception( "account insert", __LINE__ );
                }

                try {
                    // accounts indexes
                    accounts.create_index( bsoncxx::from_json( R"xxx({ "name" : 1 })xxx" ));

                } catch (...) {
                    handle_kafka_exception( "create indexes", __LINE__ );
                }
            }
        } catch (...) {
            handle_kafka_exception( "kafka-mongo init", __LINE__ );
        }

        ilog("starting kafka plugin thread");

        consume_thread = boost::thread([this] { consume_blocks(); });

        startup = false;
    }

////////////
// kafka_plugin
////////////

    kafka_plugin::kafka_plugin()
            : my(new kafka_plugin_impl) {
    }

    kafka_plugin::~kafka_plugin() {
    }

    void kafka_plugin::set_program_options(options_description &cli, options_description &cfg) {
        cfg.add_options()
                ("accepted_trx_topic", bpo::value<std::string>(),
                 "The topic for accepted transaction.")
                ("applied_trx_topic", bpo::value<std::string>(),
                 "The topic for applied transaction.")
                ("accepted_block_topic", bpo::value<std::string>(),
                 "The topic for accepted block.")
                ("irreversible_block_topic", bpo::value<std::string>(),
                 "The topic for irreversible block.")
                ("kafka-uri,k", bpo::value<std::string>(),
                 "the kafka brokers uri"
                 " Example: 127.0.0.1:9092")
                ("kafka-queue-size", bpo::value<uint32_t>()->default_value(4096),
                 "The target queue size between nodeos and kafka plugin thread.")
                ("kafka-abi-cache-size", bpo::value<uint32_t>()->default_value(40960),
                 "The maximum size of the abi cache for serializing data.")
                ("kafka-mongodb-uri", bpo::value<std::string>(),
                 "MongoDB URI connection string, see: https://docs.mongodb.com/master/reference/connection-string/."
                 " If not specified then plugin is disabled. Default database 'EOS-Kafka' is used if not specified in URI."
                 " Example: mongodb://127.0.0.1:27017/EOS-kafka-plugin")
                ("kafka-mongodb-wipe", bpo::bool_switch()->default_value(false),
                 "Required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe kafka mongo db."
                 "This option required to prevent accidental wipe of kafka mongo db.")
                ("kafka-block-start", bpo::value<uint32_t>()->default_value(0),
                 "If specified then only abi data pushed to kafka until specified block is reached.")
                ("kafka-filter-on", bpo::value<vector<string>>()->composing(),
                 "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
                ("kafka-filter-out", bpo::value<vector<string>>()->composing(),
                 "Do not track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to exclude all.")
                ;
    }

    void kafka_plugin::plugin_initialize(const variables_map &options) {
        char *brokers_str = NULL;
        char *accepted_trx_topic = NULL;
        char *applied_trx_topic = NULL;
        char *accepted_block_topic = NULL;
        char *irreversible_block_topic = NULL;

        try {
            if (options.count("kafka-uri") && options.count("kafka-mongodb-uri") ) {
                ilog("initializing kafka_plugin");
                my->configured = true;

                brokers_str = (char *) (options.at("kafka-uri").as<std::string>().c_str());
                if (options.count("accepted_trx_topic") != 0) {
                    accepted_trx_topic = (char *) (options.at("accepted_trx_topic").as<std::string>().c_str());
                    my->send_accepted_transaction = true;
                }
                if (options.count("applied_trx_topic") != 0) {
                    applied_trx_topic = (char *) (options.at("applied_trx_topic").as<std::string>().c_str());
                    my->send_applied_transaction = true;
                }
                if (options.count("accepted_block_topic") != 0) {
                    accepted_block_topic = (char *) (options.at("accepted_block_topic").as<std::string>().c_str());
                    my->send_accepted_block = true;
                }
                if (options.count("irreversible_block_topic") != 0) {
                    irreversible_block_topic = (char *) (options.at("irreversible_block_topic").as<std::string>().c_str());
                    my->send_irreversible_block = true;
                }

                if( options.count( "abi-serializer-max-time-ms") == 0 ) {
                    EOS_ASSERT(false, chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
                }
                my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

                if( options.at( "replay-blockchain" ).as<bool>() || options.at( "hard-replay-blockchain" ).as<bool>() || options.at( "delete-all-blocks" ).as<bool>() ) {
                    if( options.at( "kafka-mongodb-wipe" ).as<bool>()) {
                        ilog( "Wiping kafka mongo database on startup" );
                        my->wipe_database_on_startup = true;
                    } else if( options.count( "kafka-block-start" ) == 0 ) {
                        EOS_ASSERT( false, chain::plugin_config_exception, "--kafka-mongodb-wipe required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks"
                                                                           " --kafka-mongodb-wipe will remove all EOS-Kafka collections from mongodb." );
                    }
                }

                if( options.count( "kafka-queue-size" )) {
                    my->queue_size = options.at( "kafka-queue-size" ).as<uint32_t>();
                }
                if( options.count( "kafka-abi-cache-size" )) {
                    my->abi_cache_size = options.at("kafka-abi-cache-size").as<uint32_t>();
                    EOS_ASSERT(my->abi_cache_size > 0, chain::plugin_config_exception, "kafka-abi-cache-size > 0 required");
                }
                if( options.count( "kafka-block-start" )) {
                    my->start_block_num = options.at("kafka-block-start").as<uint32_t>();
                }
                if( options.count( "kafka-filter-on" )) {
                    auto fo = options.at( "kafka-filter-on" ).as<vector<string>>();
                    my->filter_on_star = false;
                    for( auto& s : fo ) {
                        if( s == "*" ) {
                            my->filter_on_star = true;
                            break;
                        }
                        std::vector<std::string> v;
                        boost::split( v, s, boost::is_any_of( ":" ));
                        EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --kafka-filter-on", ("s", s));
                        filter_entry fe{v[0], v[1], v[2]};
                        my->filter_on.insert( fe );
                    }
                } else {
                    my->filter_on_star = true;
                }
                if( options.count( "kafka-filter-out" )) {
                    auto fo = options.at( "kafka-filter-out" ).as<vector<string>>();
                    for( auto& s : fo ) {
                        std::vector<std::string> v;
                        boost::split( v, s, boost::is_any_of( ":" ));
                        EOS_ASSERT( v.size() == 3, fc::invalid_arg_exception, "Invalid value ${s} for --kafka-filter-out", ("s", s));
                        filter_entry fe{v[0], v[1], v[2]};
                        my->filter_out.insert( fe );
                    }
                }

                if( my->start_block_num == 0 ) {
                    my->start_block_reached = true;
                }

                std::string uri_str = options.at( "kafka-mongodb-uri" ).as<std::string>();
                ilog( "connecting to kafka-mongodb: ${u}", ("u", uri_str));
                mongocxx::uri uri = mongocxx::uri{uri_str};
                my->db_name = uri.database();
                if( my->db_name.empty())
                    my->db_name = "EOS-Kafka";
                my->mongo_pool.emplace(uri);

                // hook up to signals on controller
                my->chain_plug = app().find_plugin<chain_plugin>();
                EOS_ASSERT(my->chain_plug, chain::missing_chain_plugin_exception, "");
                auto &chain = my->chain_plug->chain();
                my->chain_id.emplace(chain.get_chain_id());

                my->accepted_block_connection.emplace( chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
                            my->accepted_block(bs);
                        }));
                my->irreversible_block_connection.emplace(
                        chain.irreversible_block.connect([&](const chain::block_state_ptr &bs) {
                            my->applied_irreversible_block(bs);
                        }));
                my->accepted_transaction_connection.emplace(
                        chain.accepted_transaction.connect([&](const chain::transaction_metadata_ptr &t) {
                            my->accepted_transaction(t);
                        }));
                my->applied_transaction_connection.emplace(
                        chain.applied_transaction.connect([&](const chain::transaction_trace_ptr &t) {
                            my->applied_transaction(t);
                        }));

                if( my->wipe_database_on_startup ) {
                    my->wipe_database();
                }

                ilog("brokers_str:${j}", ("j", brokers_str));
                ilog("accepted_trx_topic:${j}", ("j", accepted_trx_topic));
                ilog("applied_trx_topic:${j}", ("j", applied_trx_topic));
                ilog("accepted_block_topic:${j}", ("j", accepted_block_topic));
                ilog("irreversible_block_topic:${j}", ("j", irreversible_block_topic));

                if ( 0 != my->producer->kafka_init(brokers_str, accepted_trx_topic, applied_trx_topic, accepted_block_topic, irreversible_block_topic) ){
                    elog("kafka_init failed");
                    EOS_ASSERT( false, chain::plugin_config_exception, "kafka init failed" );
                } else {
                    ilog("kafka_init succeeded");
                }

                my->init();
            } else {
                wlog( "eosio::kafka_plugin configured, but no --kafka-uri or --kafka-mongodb-uri specified." );
                wlog( "kafka_plugin disabled." );
            }

        }

        FC_LOG_AND_RETHROW()
    }

    void kafka_plugin::plugin_startup() {
    }

    void kafka_plugin::plugin_shutdown() {

        my->accepted_block_connection.reset();
        my->irreversible_block_connection.reset();
        my->accepted_transaction_connection.reset();
        my->applied_transaction_connection.reset();
        my.reset();

    }

} // namespace eosio
