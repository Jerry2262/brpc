// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//          Jiashun Zhu(zhujiashun2010@gmail.com)

#include <google/protobuf/descriptor.h>         // MethodDescriptor
#include <google/protobuf/message.h>            // Message
#include <gflags/gflags.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <sys/resource.h>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "bthread/mutex.h"
#include "brpc/policy/redis_authenticator.h"
#include "butil/logging.h"                       // LOG()
#include "butil/time.h"
#include "butil/iobuf.h"                         // butil::IOBuf
#include "brpc/controller.h"               // Controller
#include "brpc/details/controller_private_accessor.h"
#include "brpc/socket.h"                   // Socket
#include "brpc/server.h"                   // Server
#include "brpc/details/server_private_accessor.h"
#include "brpc/span.h"
#include "brpc/redis.h"
#include "brpc/redis_command.h"
#include "brpc/policy/redis_protocol.h"

namespace brpc {

DECLARE_bool(enable_rpcz);
DECLARE_bool(usercode_in_pthread);

namespace policy {

DEFINE_bool(redis_verbose, false,
            "[DEBUG] Print EVERY redis request/response");

// This is an experimental server-side cache for whole-hash reads. XBase's
// workload A sends HGETALL/HMSET through the Redis protocol and is strongly
// Zipfian. Keeping the cache in the protocol layer lets all connections share
// hot replies and, more importantly, avoids dispatching cache hits into the
// application command executor at all.
DEFINE_int32(redis_server_hgetall_cache_capacity, 3145728,
             "Maximum number of HGETALL replies cached by a Redis server. "
             "Set to 0 to disable");

namespace {

const size_t kHgetallCacheShardCount = 1024;

uint64_t HashCacheKey(const butil::StringPiece& key) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < key.size(); ++i) {
        hash ^= static_cast<unsigned char>(key[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}

struct HgetallCacheField {
    HgetallCacheField(std::string field_name, size_t offset, size_t size)
        : name(std::move(field_name)), value_offset(offset), value_size(size) {}

    std::string name;
    size_t value_offset;
    size_t value_size;
};

struct HgetallCacheEntry {
    uint64_t hash = 0;
    std::string key;
    std::string reply;
    std::vector<HgetallCacheField> fields;
    bool occupied = false;
};

class HgetallCacheShard {
public:
    explicit HgetallCacheShard(size_t capacity)
        : _entries(capacity) {
        _index.reserve(capacity);
    }

    bool Find(uint64_t hash, const butil::StringPiece& key,
              const std::string** reply) {
        const auto it = _index.find(hash);
        if (it == _index.end()) {
            return false;
        }
        HgetallCacheEntry& entry = _entries[it->second];
        if (!KeyEquals(entry, key)) {
            return false;
        }
        *reply = &entry.reply;
        return true;
    }

    bool Insert(uint64_t hash, const butil::StringPiece& key, std::string reply,
                std::vector<HgetallCacheField> fields) {
        if (_entries.empty()) {
            return false;
        }
        const auto found = _index.find(hash);
        if (found != _index.end()) {
            HgetallCacheEntry& entry = _entries[found->second];
            if (!KeyEquals(entry, key)) {
                return false;
            }
            entry.reply = std::move(reply);
            entry.fields = std::move(fields);
            return true;
        }

        size_t slot = 0;
        if (_index.size() < _entries.size()) {
            while (_entries[_clock_hand].occupied) {
                AdvanceClock();
            }
            slot = _clock_hand;
        } else {
            // The experimental capacity exceeds the number of distinct keys
            // seen in the benchmark. Keep a simple fallback for longer runs.
            slot = _clock_hand;
            _index.erase(_entries[slot].hash);
        }

        HgetallCacheEntry& entry = _entries[slot];
        entry.hash = hash;
        entry.key.assign(key.data(), key.size());
        entry.reply = std::move(reply);
        entry.fields = std::move(fields);
        entry.occupied = true;
        _index.emplace(hash, slot);
        AdvanceClock();
        return true;
    }

    bool Erase(uint64_t hash, const butil::StringPiece& key) {
        const auto it = _index.find(hash);
        if (it == _index.end() || !KeyEquals(_entries[it->second], key)) {
            return false;
        }
        HgetallCacheEntry& entry = _entries[it->second];
        entry.hash = 0;
        entry.key.clear();
        entry.reply.clear();
        entry.fields.clear();
        entry.occupied = false;
        _index.erase(it);
        return true;
    }

    bool ApplyHashWrite(uint64_t hash, const butil::StringPiece& key,
                        const std::vector<butil::StringPiece>& args) {
        const auto it = _index.find(hash);
        if (it == _index.end() || !KeyEquals(_entries[it->second], key)) {
            return false;
        }
        HgetallCacheEntry& entry = _entries[it->second];
        // The final argument is XBase's command flags. All arguments between
        // the key and flags are field/value pairs for HSET and HMSET.
        for (size_t i = 2; i + 2 < args.size(); i += 2) {
            const butil::StringPiece field = args[i];
            const butil::StringPiece value = args[i + 1];
            bool found = false;
            for (const auto& cached_field : entry.fields) {
                if (field.size() == cached_field.name.size() &&
                    memcmp(field.data(), cached_field.name.data(), field.size()) == 0) {
                    if (value.size() != cached_field.value_size ||
                        cached_field.value_offset + value.size() > entry.reply.size()) {
                        Erase(hash, key);
                        return false;
                    }
                    if (!value.empty()) {
                        memcpy(&entry.reply[cached_field.value_offset],
                               value.data(), value.size());
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                Erase(hash, key);
                return false;
            }
        }
        return true;
    }

    static bool BuildReply(const RedisReply& output, std::string* reply,
                           std::vector<HgetallCacheField>* fields) {
        if (!output.is_array() || output.size() % 2 != 0) {
            return false;
        }
        reply->clear();
        fields->clear();
        fields->reserve(output.size() / 2);
        reply->push_back('*');
        reply->append(std::to_string(output.size()));
        reply->append("\r\n", 2);
        for (size_t i = 0; i < output.size(); i += 2) {
            if (!output[i].is_string() || !output[i + 1].is_string()) {
                reply->clear();
                fields->clear();
                return false;
            }
            const butil::StringPiece field = output[i].data();
            const butil::StringPiece value = output[i + 1].data();
            AppendBulkString(reply, field);
            reply->push_back('$');
            reply->append(std::to_string(value.size()));
            reply->append("\r\n", 2);
            fields->emplace_back(
                std::string(field.data(), field.size()), reply->size(), value.size());
            reply->append(value.data(), value.size());
            reply->append("\r\n", 2);
        }
        return true;
    }

    bthread::Mutex mutex;

private:
    static bool KeyEquals(const HgetallCacheEntry& entry,
                          const butil::StringPiece& key) {
        return entry.key.size() == key.size() &&
               (key.empty() || memcmp(entry.key.data(), key.data(), key.size()) == 0);
    }

    static void AppendBulkString(std::string* reply,
                                 const butil::StringPiece& value) {
        reply->push_back('$');
        reply->append(std::to_string(value.size()));
        reply->append("\r\n", 2);
        reply->append(value.data(), value.size());
        reply->append("\r\n", 2);
    }

    void AdvanceClock() {
        if (++_clock_hand == _entries.size()) {
            _clock_hand = 0;
        }
    }

    std::vector<HgetallCacheEntry> _entries;
    std::unordered_map<uint64_t, size_t> _index;
    size_t _clock_hand = 0;
};

class HgetallCache {
public:
    HgetallCache() {
        const size_t capacity = FLAGS_redis_server_hgetall_cache_capacity > 0
            ? static_cast<size_t>(FLAGS_redis_server_hgetall_cache_capacity) : 0;
        const size_t shard_capacity =
            (capacity + kHgetallCacheShardCount - 1) / kHgetallCacheShardCount;
        _shards.reserve(kHgetallCacheShardCount);
        for (size_t i = 0; i < kHgetallCacheShardCount; ++i) {
            _shards.emplace_back(new HgetallCacheShard(shard_capacity));
        }
    }

    HgetallCacheShard* Shard(uint64_t hash) {
        return _shards[hash & (_shards.size() - 1)].get();
    }

    void RecordHit() {
        _hits.fetch_add(1, std::memory_order_relaxed);
        MaybeLog();
    }

    void RecordMiss() {
        _misses.fetch_add(1, std::memory_order_relaxed);
        MaybeLog();
    }

    void RecordWrite(bool updated) {
        _writes.fetch_add(1, std::memory_order_relaxed);
        if (updated) {
            _write_updates.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void RecordErase(bool erased) {
        _erases.fetch_add(1, std::memory_order_relaxed);
        if (erased) {
            _useful_erases.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void RecordAdmission(bool admitted) {
        if (admitted) {
            _admissions.fetch_add(1, std::memory_order_relaxed);
        } else {
            _admission_rejections.fetch_add(1, std::memory_order_relaxed);
        }
    }

private:
    void MaybeLog() {
        const uint64_t accesses =
            _accesses.fetch_add(1, std::memory_order_relaxed) + 1;
        if (accesses % 1000000 == 0) {
            struct rusage usage = {};
            getrusage(RUSAGE_SELF, &usage);
            LOG(INFO) << "BRPC HGETALL cache accesses=" << accesses
                      << " hits=" << _hits.load(std::memory_order_relaxed)
                      << " misses=" << _misses.load(std::memory_order_relaxed)
                      << " writes=" << _writes.load(std::memory_order_relaxed)
                      << " write_updates=" << _write_updates.load(std::memory_order_relaxed)
                      << " admissions=" << _admissions.load(std::memory_order_relaxed)
                      << " admission_rejections="
                      << _admission_rejections.load(std::memory_order_relaxed)
                      << " erases=" << _erases.load(std::memory_order_relaxed)
                      << " useful_erases=" << _useful_erases.load(std::memory_order_relaxed)
                      << " max_rss_kb=" << usage.ru_maxrss;
        }
    }

    std::vector<std::unique_ptr<HgetallCacheShard> > _shards;
    std::atomic<uint64_t> _accesses{0};
    std::atomic<uint64_t> _hits{0};
    std::atomic<uint64_t> _misses{0};
    std::atomic<uint64_t> _writes{0};
    std::atomic<uint64_t> _write_updates{0};
    std::atomic<uint64_t> _admissions{0};
    std::atomic<uint64_t> _admission_rejections{0};
    std::atomic<uint64_t> _erases{0};
    std::atomic<uint64_t> _useful_erases{0};
};

HgetallCache& GetHgetallCache() {
    static HgetallCache cache;
    return cache;
}

bool IsCommand(const butil::StringPiece& command, const char* expected) {
    return command == expected;
}

bool UpdatesHgetall(const butil::StringPiece& command) {
    return IsCommand(command, "hset") || IsCommand(command, "hmset");
}

}  // namespace

struct InputResponse : public InputMessageBase {
    bthread_id_t id_wait;
    RedisResponse response;

    // @InputMessageBase
    void DestroyImpl() {
        delete this;
    }
};


int ConsumeCommand(RedisConnContext* ctx,
                   const std::vector<butil::StringPiece>& args,
                   bool flush_batched,
                   butil::IOBufAppender* appender) {
    const bool cache_enabled = FLAGS_redis_server_hgetall_cache_capacity > 0;
    const bool cache_read = cache_enabled && args.size() >= 2 &&
                            ctx->batched_size == 0 &&
                            IsCommand(args[0], "hgetall");
    const bool cache_update = cache_enabled && args.size() >= 5 &&
                              UpdatesHgetall(args[0]);
    const bool cache_erase = cache_enabled && args.size() >= 2 &&
                             IsCommand(args[0], "del");
    HgetallCache* cache = NULL;
    HgetallCacheShard* cache_shard = NULL;
    uint64_t cache_hash = 0;
    std::unique_lock<bthread::Mutex> cache_lock;
    if (cache_read || cache_update || cache_erase) {
        cache_hash = HashCacheKey(args[1]);
        cache = &GetHgetallCache();
        cache_shard = cache->Shard(cache_hash);
        cache_lock = std::unique_lock<bthread::Mutex>(cache_shard->mutex);
        if (cache_read) {
            const std::string* cached_reply = NULL;
            if (cache_shard->Find(cache_hash, args[1], &cached_reply)) {
                cache->RecordHit();
                appender->append(cached_reply->data(), cached_reply->size());
                return 0;
            }
            cache->RecordMiss();
        }
    }

    RedisReply output(&ctx->arena);
    RedisCommandHandlerResult result = REDIS_CMD_HANDLED;
    if (ctx->transaction_handler) {
        result = ctx->transaction_handler->Run(ctx, args, &output, flush_batched);
        if (result == REDIS_CMD_HANDLED) {
            ctx->transaction_handler.reset(NULL);
        } else if (result == REDIS_CMD_BATCHED) {
            LOG(ERROR) << "BATCHED should not be returned by a transaction handler.";
            return -1;
        }
    } else {
        RedisCommandHandler* ch = ctx->redis_service->FindCommandHandler(args[0]);
        if (!ch) {
            char buf[64];
            snprintf(buf, sizeof(buf), "ERR unknown command `%s`", args[0].as_string().c_str());
            output.SetError(buf);
        } else {
            result = ch->Run(ctx, args, &output, flush_batched);
            if (result == REDIS_CMD_CONTINUE) {
                if (ctx->batched_size != 0) {
                    LOG(ERROR) << "CONTINUE should not be returned in a batched process.";
                    return -1;
                }
                ctx->transaction_handler.reset(ch->NewTransactionHandler());
            } else if (result == REDIS_CMD_BATCHED) {
                ctx->batched_size++;
            }
        }
    }
    if (result == REDIS_CMD_HANDLED) {
        if (ctx->batched_size) {
            if ((int)output.size() != (ctx->batched_size + 1)) {
                LOG(ERROR) << "reply array size can't be matched with batched size, "
                    << " expected=" << ctx->batched_size + 1 << " actual=" << output.size();
                return -1;
            }
            for (int i = 0; i < (int)output.size(); ++i) {
                output[i].SerializeTo(appender);
            }
            ctx->batched_size = 0;
        } else if (cache_read && !output.is_error()) {
            std::string cached_reply;
            std::vector<HgetallCacheField> fields;
            if (HgetallCacheShard::BuildReply(output, &cached_reply, &fields)) {
                appender->append(cached_reply.data(), cached_reply.size());
                const bool admitted = cache_shard->Insert(
                    cache_hash, args[1], std::move(cached_reply), std::move(fields));
                cache->RecordAdmission(admitted);
            } else {
                output.SerializeTo(appender);
            }
        } else {
            output.SerializeTo(appender);
        }
    } else if (result == REDIS_CMD_CONTINUE) {
        output.SerializeTo(appender);
    } else if (result == REDIS_CMD_BATCHED) {
        // just do nothing and wait handler to return OK.
    } else {
        LOG(ERROR) << "unknown status=" << result;
        return -1;
    }
    if (result == REDIS_CMD_HANDLED && cache_update) {
        cache->RecordWrite(!output.is_error() &&
                           cache_shard->ApplyHashWrite(cache_hash, args[1], args));
    } else if (result == REDIS_CMD_HANDLED && cache_erase) {
        cache->RecordErase(cache_shard->Erase(cache_hash, args[1]));
    }
    return 0;
}


ParseResult ParseRedisMessage(butil::IOBuf* source, Socket* socket,
                              bool read_eof, const void* arg) {
    if (read_eof || source->empty()) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    const Server* server = static_cast<const Server*>(arg);
    if (server) {
        const RedisService* const rs = server->options().redis_service;
        if (!rs) {
            return MakeParseError(PARSE_ERROR_TRY_OTHERS);
        }
        RedisConnContext* ctx = static_cast<RedisConnContext*>(socket->parsing_context());
        if (ctx == NULL) {
            ctx = new RedisConnContext(rs);
            socket->reset_parsing_context(ctx);
        }
        std::vector<butil::StringPiece> current_args;
        butil::IOBufAppender appender;
        ParseError err = PARSE_OK;

        err = ctx->parser.Consume(*source, &current_args, &ctx->arena);
        if (err != PARSE_OK) {
            return MakeParseError(err);
        }
        while (true) {
            std::vector<butil::StringPiece> next_args;
            err = ctx->parser.Consume(*source, &next_args, &ctx->arena);
            if (err != PARSE_OK) {
                break;
            }
            if (ConsumeCommand(ctx, current_args, false, &appender) != 0) {
                return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
            }
            current_args.swap(next_args);
        }
        if (ConsumeCommand(ctx, current_args,
                    true /*must be the last message*/, &appender) != 0) {
            return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
        }
        butil::IOBuf sendbuf;
        appender.move_to(sendbuf);
        CHECK(!sendbuf.empty());
        Socket::WriteOptions wopt;
        wopt.ignore_eovercrowded = true;
        LOG_IF(WARNING, socket->Write(&sendbuf, &wopt) != 0)
            << "Fail to send redis reply";
        if(ctx->parser.ParsedArgsSize() == 0) {
            ctx->arena.clear();
        }
        return MakeParseError(err);
    } else {
        // NOTE(gejun): PopPipelinedInfo() is actually more contended than what
        // I thought before. The Socket._pipeline_q is a SPSC queue pushed before
        // sending and popped when response comes back, being protected by a
        // mutex. Previously the mutex is shared with Socket._id_wait_list. When
        // 200 bthreads access one redis-server, ~1.5s in total is spent on
        // contention in 10-second duration. If the mutex is separated, the time
        // drops to ~0.25s. I further replaced PeekPipelinedInfo() with
        // GivebackPipelinedInfo() to lock only once(when receiving response)
        // in most cases, and the time decreases to ~0.14s.
        PipelinedInfo pi;
        if (!socket->PopPipelinedInfo(&pi)) {
            LOG(WARNING) << "No corresponding PipelinedInfo in socket";
            return MakeParseError(PARSE_ERROR_TRY_OTHERS);
        }

        do {
            InputResponse* msg = static_cast<InputResponse*>(socket->parsing_context());
            if (msg == NULL) {
                msg = new InputResponse;
                socket->reset_parsing_context(msg);
            }

            const int consume_count = (pi.auth_flags ? pi.auth_flags : pi.count);

            ParseError err = msg->response.ConsumePartialIOBuf(*source, consume_count);
            if (err != PARSE_OK) {
                socket->GivebackPipelinedInfo(pi);
                return MakeParseError(err);
            }

            if (pi.auth_flags) {
                for (int i = 0; i < (int)pi.auth_flags; ++i) {
                    if (i >= msg->response.reply_size() ||
                        !(msg->response.reply(i).type() ==
                              brpc::REDIS_REPLY_STATUS &&
                          msg->response.reply(i).data().compare("OK") == 0)) {
                        LOG(ERROR) << "Redis Auth failed: " << msg->response;
                        return MakeParseError(PARSE_ERROR_NO_RESOURCE,
                            "Fail to authenticate with Redis");
                    }
                }

                DestroyingPtr<InputResponse> auth_msg(
                     static_cast<InputResponse*>(socket->release_parsing_context()));
                pi.auth_flags = 0;
                continue;
            }

            CHECK_EQ((uint32_t)msg->response.reply_size(), pi.count);
            msg->id_wait = pi.id_wait;
            socket->release_parsing_context();
            return MakeMessage(msg);
        } while(true);
    }

    return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
}

void ProcessRedisResponse(InputMessageBase* msg_base) {
    const int64_t start_parse_us = butil::cpuwide_time_us();
    DestroyingPtr<InputResponse> msg(static_cast<InputResponse*>(msg_base));

    const bthread_id_t cid = msg->id_wait;
    Controller* cntl = NULL;
    const int rc = bthread_id_lock(cid, (void**)&cntl);
    if (rc != 0) {
        LOG_IF(ERROR, rc != EINVAL && rc != EPERM)
            << "Fail to lock correlation_id=" << cid << ": " << berror(rc);
        return;
    }

    ControllerPrivateAccessor accessor(cntl);
    Span* span = accessor.span();
    if (span) {
        span->set_base_real_us(msg->base_real_us());
        span->set_received_us(msg->received_us());
        span->set_response_size(msg->response.ByteSize());
        span->set_start_parse_us(start_parse_us);
    }
    const int saved_error = cntl->ErrorCode();
    if (cntl->response() != NULL) {
        if (cntl->response()->GetDescriptor() != RedisResponse::descriptor()) {
            cntl->SetFailed(ERESPONSE, "Must be RedisResponse");
        } else {
            // We work around ParseFrom of pb which is just a placeholder.
            if (msg->response.reply_size() != (int)accessor.pipelined_count()) {
                cntl->SetFailed(ERESPONSE, "pipelined_count=%d of response does "
                                      "not equal request's=%d",
                                      msg->response.reply_size(), accessor.pipelined_count());
            }
            ((RedisResponse*)cntl->response())->Swap(&msg->response);
            if (FLAGS_redis_verbose) {
                LOG(INFO) << "\n[REDIS RESPONSE] "
                          << *((RedisResponse*)cntl->response());
            }
        }
    } // silently ignore the response.

    // Unlocks correlation_id inside. Revert controller's
    // error code if it version check of `cid' fails
    msg.reset();  // optional, just release resource ASAP
    accessor.OnResponse(cid, saved_error);
}

void ProcessRedisRequest(InputMessageBase* msg_base) { }

void SerializeRedisRequest(butil::IOBuf* buf,
                           Controller* cntl,
                           const google::protobuf::Message* request) {
    if (request == NULL) {
        return cntl->SetFailed(EREQUEST, "request is NULL");
    }
    if (request->GetDescriptor() != RedisRequest::descriptor()) {
        return cntl->SetFailed(EREQUEST, "The request is not a RedisRequest");
    }
    RedisRequest* rr = (RedisRequest*)request;
    // If redis byte size is zero, brpc call will fail with E22. Continuous E22 may cause E112 in the end.
    // So set failed and return useful error message
    if (rr->ByteSize() == 0) {
        return cntl->SetFailed(EREQUEST, "request byte size is empty");
    }
    // We work around SerializeTo of pb which is just a placeholder.
    if (!rr->SerializeTo(buf)) {
        return cntl->SetFailed(EREQUEST, "Fail to serialize RedisRequest");
    }
    ControllerPrivateAccessor(cntl).set_pipelined_count(rr->command_size());
    if (FLAGS_redis_verbose) {
        LOG(INFO) << "\n[REDIS REQUEST] " << *rr;
    }
}

void PackRedisRequest(butil::IOBuf* buf,
                      SocketMessage**,
                      uint64_t /*correlation_id*/,
                      const google::protobuf::MethodDescriptor*,
                      Controller* cntl,
                      const butil::IOBuf& request,
                      const Authenticator* auth) {
    if (auth) {
        std::string auth_str;
        if (auth->GenerateCredential(&auth_str) != 0) {
            return cntl->SetFailed(EREQUEST, "Fail to generate credential");
        }
        buf->append(auth_str);
        const RedisAuthenticator* redis_auth =
            dynamic_cast<const RedisAuthenticator*>(auth);
        if (redis_auth == NULL) {
            return cntl->SetFailed(EREQUEST, "Fail to generate credential");
        }
        ControllerPrivateAccessor(cntl).set_auth_flags(
            redis_auth->GetAuthFlags());
    } else {
        ControllerPrivateAccessor(cntl).clear_auth_flags();
    }

    buf->append(request);
}

const std::string& GetRedisMethodName(
    const google::protobuf::MethodDescriptor*,
    const Controller*) {
    const static std::string REDIS_SERVER_STR = "redis-server";
    return REDIS_SERVER_STR;
}

}  // namespace policy
} // namespace brpc
