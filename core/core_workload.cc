//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"
#include "core_workload.h"
#include "client.h"
#include "random_byte_generator.h"
#include "utils/utils.h"

#include <algorithm>
#include <random>
#include <string>
#include <iostream>
using ycsbc::CoreWorkload;
using std::string;

const char *ycsbc::kOperationString[ycsbc::MAXOPTYPE] = {
  "INSERT",
  "READ",
  "MULTI_READ",
  "UPDATE",
  "SCAN",
  "READMODIFYWRITE",
  "DELETE",
  "RANDOM_INSERT",
  "INSERT_BATCH",
  "INSERT-FAILED",
  "READ-FAILED",
  "MULTI_READ-FAILED",
  "UPDATE-FAILED",
  "SCAN-FAILED",
  "READMODIFYWRITE-FAILED",
  "DELETE-FAILED",
  "INSERT_BATCH-FAILED"
};

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY = "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::RANDOM_INSERT_PROPORTION_PROPERTY = "randominsertproportion";
const string CoreWorkload::RANDOM_INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::INSERT_BATCH_PROPORTION_PROPERTY = "insertbatchproportion";
const string CoreWorkload::INSERT_BATCH_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform,uniform,uniform,uniform"; // -

const string CoreWorkload::ZERO_PADDING_PROPERTY = "zeropadding";
const string CoreWorkload::ZERO_PADDING_DEFAULT = "1";

const string CoreWorkload::MIN_SCAN_LENGTH_PROPERTY = "minscanlength";
const string CoreWorkload::MIN_SCAN_LENGTH_DEFAULT = "1";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "ordered";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const std::string CoreWorkload::FIELD_NAME_PREFIX = "fieldnameprefix";
const std::string CoreWorkload::FIELD_NAME_PREFIX_DEFAULT = "field";

const std::string CoreWorkload::ZIPFIAN_CONST_PROPERTY = "zipfian_const"; //

const std::string CoreWorkload::OP_MODE_PROPERTY = "real_op_mode";
const std::string CoreWorkload::OP_MODE_DEFAULT = "true";

const std::string CoreWorkload::BURST_GAP_S = "burst_gap_s";
const std::string CoreWorkload::BURST_GAP_S_DEFAULT = "0";

const std::string CoreWorkload::BURST_SIZE_OPS = "burst_size_ops";
const std::string CoreWorkload::BURST_SIZE_OPS_DEFAULT = "0";

const std::string CoreWorkload::CLIENT_TO_CF_MAP = "client_to_cf_map";
const std::string CoreWorkload::CLIENT_TO_CF_MAP_DEFAULT = "default,cf2,cf3,cf4";

const std::string CoreWorkload::CLIENT_TO_CF_OFFSET = "client_to_cf_offset";
const std::string CoreWorkload::CLIENT_TO_CF_OFFSET_DEFAULT = "0,0,0,0";

const std::string CoreWorkload::CLIENT_TO_OP_MAP = "client_to_op_map";
const std::string CoreWorkload::CLIENT_TO_OP_MAP_DEFAULT = "READ,READ,READ,READ";

namespace ycsbc {

std::vector<std::string> Prop2vector(const utils::Properties &props, const std::string& prop, const std::string& default_val) {
  std::string vals = props.GetProperty(prop, default_val);
  std::vector<std::string> output;
  std::string val;

  std::istringstream stream(vals);
  while (std::getline(stream, val, ',')) {
    output.push_back(val);
  }
  return output;
}

std::vector<int> Prop2vectorInt (const utils::Properties &props, const std::string& prop, const std::string& default_val) {
  std::string vals = props.GetProperty(prop, default_val);
  std::vector<int> output;
  std::string val;

  std::istringstream stream(vals);
  while (std::getline(stream, val, ',')) {
    output.push_back(std::stoi(val));
  }
  return output;
}

Operation stringToOperation(const std::string& operationName) {
    static const std::unordered_map<std::string, Operation> operationMap = {
        {"INSERT", INSERT},
        {"READ", READ},
        {"UPDATE", UPDATE},
        {"SCAN", SCAN},
        {"READMODIFYWRITE", READMODIFYWRITE},
        {"DELETE", DELETE},
        {"RANDOM_INSERT", RANDOM_INSERT},
        {"INSERT_BATCH", INSERT_BATCH},
        {"INSERT_FAILED", INSERT_FAILED},
        {"READ_FAILED", READ_FAILED},
        {"MULTI_READ", MULTI_READ},
        {"MULTI_READ_FAILED", MULTI_READ_FAILED},
        {"UPDATE_FAILED", UPDATE_FAILED},
        {"SCAN_FAILED", SCAN_FAILED},
        {"READMODIFYWRITE_FAILED", READMODIFYWRITE_FAILED},
        {"DELETE_FAILED", DELETE_FAILED},
        {"INSERT_BATCH_FAILED", INSERT_BATCH_FAILED},
        {"MAXOPTYPE", MAXOPTYPE}
    };

    auto it = operationMap.find(operationName);
    if (it != operationMap.end()) {
        return it->second;
    } else {
        throw std::invalid_argument("Invalid operation name: " + operationName);
    }
}

void CoreWorkload::Init(const utils::Properties &p) {
  table_name_ = p.GetProperty(TABLENAME_PROPERTY,TABLENAME_DEFAULT);
  op_mode_real_ = p.GetProperty(OP_MODE_PROPERTY, OP_MODE_DEFAULT) == "true";

  client_to_cf_ = Prop2vector(p, CLIENT_TO_CF_MAP, CLIENT_TO_CF_MAP_DEFAULT);
  client_to_cf_offset = Prop2vector(p, CLIENT_TO_CF_OFFSET, CLIENT_TO_CF_OFFSET_DEFAULT);

  std::vector<std::string> client_to_op_string = Prop2vector(p, CLIENT_TO_OP_MAP, CLIENT_TO_OP_MAP_DEFAULT);
  for (const auto& op_string : client_to_op_string) {
    client_to_op_.push_back(stringToOperation(op_string));
  }

  // const size_t num_threads = std::stoi(p.GetProperty("threadcount", "1"));
  // if (num_threads != client_to_op_.size() || num_threads != client_to_cf_.size()) {
  //   throw utils::Exception("Inconsistent thread counts and thread to CF and OP mappings");
  // }

  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT));
  field_prefix_ = p.GetProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
  field_len_generator_ = GetFieldLenGenerator(p);

  double read_proportion = std::stod(p.GetProperty(READ_PROPORTION_PROPERTY,
                                                   READ_PROPORTION_DEFAULT));
  double update_proportion = std::stod(p.GetProperty(UPDATE_PROPORTION_PROPERTY,
                                                     UPDATE_PROPORTION_DEFAULT));
  double insert_proportion = std::stod(p.GetProperty(INSERT_PROPORTION_PROPERTY,
                                                     INSERT_PROPORTION_DEFAULT));
  double scan_proportion = std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY,
                                                   SCAN_PROPORTION_DEFAULT));
  double readmodifywrite_proportion = std::stod(p.GetProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));

  double randominsert_proportion = std::stod(p.GetProperty(
      RANDOM_INSERT_PROPORTION_PROPERTY, RANDOM_INSERT_PROPORTION_DEFAULT));

  record_counts_ = Prop2vectorInt(p, RECORD_COUNT_PROPERTY, "0,0,0,0");
  std::vector<std::string> request_dist = Prop2vector (p, REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_DEFAULT);
  int min_scan_len = std::stoi(p.GetProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_DEFAULT));
  int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT));
  std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                            SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  int insert_start = std::stoi(p.GetProperty(INSERT_START_PROPERTY, INSERT_START_DEFAULT));

  zero_padding_ = std::stoi(p.GetProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_DEFAULT));

  read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                    READ_ALL_FIELDS_DEFAULT));
  write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                     WRITE_ALL_FIELDS_DEFAULT));

  // if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
  //   ordered_inserts_ = false;
  // } else {
  //   ordered_inserts_ = true;
  // }

  // No hashing.
  ordered_inserts_ = true;

  if (read_proportion > 0) {
    op_chooser_.AddValue(READ, read_proportion);
  }
  if (update_proportion > 0) {
    op_chooser_.AddValue(UPDATE, update_proportion);
  }
  if (insert_proportion > 0) {
    op_chooser_.AddValue(INSERT, insert_proportion);
  }
  if (scan_proportion > 0) {
    op_chooser_.AddValue(SCAN, scan_proportion);
  }
  if (readmodifywrite_proportion > 0) {
    op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
  }
  if (randominsert_proportion > 0) {
    op_chooser_.AddValue(RANDOM_INSERT, randominsert_proportion);
  }
  // if (insertbatch_proportion > 0) {
  //   op_chooser_.AddValue(INSERT_BATCH, insertbatch_proportion);
  // }

  insert_key_sequence_ = new CounterGenerator(insert_start);

  for (size_t client_i = 0; client_i < request_dist.size(); client_i++) {
    transaction_insert_key_sequence_.push_back(new AcknowledgedCounterGenerator(record_counts_[0]));
    std::string req_dist_single = request_dist[client_i];
    std::cout << "[FAIRDB_LOG] " << "Initialising request type " << req_dist_single << std::endl;
    Generator<uint64_t>* key_chooser_single;
    if (req_dist_single == "uniform") {
      std::cout << "[FAIRDB_LOG] Uniform distribution over " << std::to_string(record_counts_[client_i]) << " records." << std::endl;
      key_chooser_single = new UniformGenerator(0, record_counts_[client_i] - 1);
    } else if (req_dist_single == "zipfian") {
      std::cout << "[FAIRDB_LOG] Zipfian distribution." << std::endl;
      // If the number of keys changes, we don't want to change popular keys.
      // So we construct the scrambled zipfian generator with a keyspace
      // that is larger than what exists at the beginning of the test.
      // If the generator picks a key that is not inserted yet, we just ignore it
      // and pick another key.
      int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
      int new_keys = (int)(op_count * insert_proportion * 2); // a fudge factor
      if (p.ContainsKey(ZIPFIAN_CONST_PROPERTY)) {
        double zipfian_const = std::stod(p.GetProperty(ZIPFIAN_CONST_PROPERTY));
        key_chooser_single = new ScrambledZipfianGenerator(0, record_counts_[client_i] + new_keys - 1, zipfian_const);
      } else {
        key_chooser_single = new ScrambledZipfianGenerator(record_counts_[client_i] + new_keys);
      }
    } else if (req_dist_single == "latest") {
      key_chooser_single = new SkewedLatestGenerator(*transaction_insert_key_sequence_[client_i]);
    } else {
      throw utils::Exception("Unknown request distribution: " + req_dist_single);
    }
    key_chooser_.push_back(key_chooser_single);
  }

  field_chooser_ = new UniformGenerator(0, field_count_ - 1);

  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = new UniformGenerator(min_scan_len, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = new ZipfianGenerator(min_scan_len, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " + scan_len_dist);
  }
}

ycsbc::Generator<uint64_t> *CoreWorkload::GetFieldLenGenerator(
    const utils::Properties &p) {
  string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                        FIELD_LENGTH_DISTRIBUTION_DEFAULT);
  int field_len = std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
  if(field_len_dist == "constant") {
    return new ConstGenerator(field_len);
  } else if(field_len_dist == "uniform") {
    return new UniformGenerator(1, field_len);
  } else if(field_len_dist == "zipfian") {
    return new ZipfianGenerator(1, field_len);
  } else {
    throw utils::Exception("Unknown field length distribution: " + field_len_dist);
  }
}

std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  // if (!ordered_inserts_) {
  //   key_num = utils::Hash(key_num);
  // }
  std::string prekey = "user";
  std::string value = std::to_string(key_num);
  // int fill = std::max(0, zero_padding_ - static_cast<int>(value.size()));
  // return prekey.append(fill, '0').append(value);
  return prekey.append(value);
}

void CoreWorkload::BuildValues(std::vector<ycsbc::DB::Field> &values) {
  for (int i = 0; i < field_count_; ++i) {
    values.push_back(DB::Field());
    ycsbc::DB::Field &field = values.back();
    field.name.append(field_prefix_).append(std::to_string(i));
    uint64_t len = field_len_generator_->Next();
    field.value.reserve(len);
    RandomByteGenerator byte_generator;
    std::generate_n(std::back_inserter(field.value), len, [&]() { return byte_generator.Next(); } );
  }
}

void CoreWorkload::BuildSingleValue(std::vector<ycsbc::DB::Field> &values) {
  values.push_back(DB::Field());
  ycsbc::DB::Field &field = values.back();
  field.name.append(NextFieldName());
  uint64_t len = field_len_generator_->Next();
  field.value.reserve(len);
  RandomByteGenerator byte_generator;
  std::generate_n(std::back_inserter(field.value), len, [&]() { return byte_generator.Next(); } );
}

uint64_t CoreWorkload::NextTransactionKeyNum(int client_id) {
  return key_chooser_[client_id]->Next();;
}

std::string CoreWorkload::NextFieldName() {
  return std::string(field_prefix_).append(std::to_string(field_chooser_->Next()));
}

bool CoreWorkload::DoInsert(DB &db) {
  const std::string key = BuildKeyName(insert_key_sequence_->Next());
  std::vector<DB::Field> fields;
  BuildValues(fields);
  return db.Insert(table_name_, key, fields) == DB::kOK;
}

void EnforceClientRateLimit(long op_start_time_ns, long target_ops_per_s, long target_ops_tick_ns, int op_num) {
  if (target_ops_per_s > 0) {
    long deadline = op_start_time_ns + target_ops_tick_ns;
    std::chrono::nanoseconds deadline_ns(deadline);
    std::chrono::time_point<std::chrono::high_resolution_clock, std::chrono::nanoseconds> target_time_point(deadline_ns);

    while (std::chrono::high_resolution_clock::now() < target_time_point) {}
  }
}

std::string CoreWorkload::GetKey (int client_id, size_t i) {
  uint64_t key_num = i;
  int offset = std::stoi(client_to_cf_offset[client_id]);
  uint64_t client_key_num = i + offset;
  return BuildKeyName(client_key_num);
}

void CoreWorkload::ReadBurstRecords (DB *db, int client_id, size_t read_burst_num_records) {
  size_t record_count = record_counts_[client_id];
  std::string table_name = client_to_cf_[client_id];
  std::vector<std::string> keys;
  int start_index;
  if (read_burst_num_records > record_count) {
    start_index = 0;
  } else {
    start_index = record_count-read_burst_num_records;
  }
  for (int i = start_index; i < record_count; i++) {
    keys.push_back(GetKey(client_id, i));
  }
  std::vector<DB::Field> result;
  db->MultiRead(table_name, keys, NULL, result, client_id);
}

void CoreWorkload::DoWarmup(DB &db, int client_id, long int target_ops_tick_ns, int target_ops_per_s, ThreadPool* threadpool) {
  size_t record_count = record_counts_[client_id];
  std::string table_name = client_to_cf_[client_id];
  for (size_t record_i = 0; record_i < record_count; record_i ++) {
    auto op_start_time = std::chrono::high_resolution_clock::now();
    auto op_start_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(op_start_time.time_since_epoch()).count();
    std::string key = GetKey(client_id, record_i);
    auto txn_lambda = [&db, client_id, key, table_name]() {
      std::vector<DB::Field> result;
      db.Read(table_name, key, NULL, result, client_id);
      return nullptr;
    };
    
    // Submit operation and do not wait for a return. 
    threadpool->async_dispatch(client_id, txn_lambda);

    EnforceClientRateLimit(op_start_time_ns, target_ops_per_s, target_ops_tick_ns, record_i+1);
  }
}

bool CoreWorkload::DoTransaction(DB &db, int client_id) {
  std::string table_name = client_to_cf_[client_id];

  DB::Status status;
  if (op_mode_real_) {
    auto op_choice = op_chooser_.Next();
    switch (op_choice) {
      case READ:
        status = TransactionRead(db, client_id, table_name);
        break;
      case UPDATE:
        status = TransactionUpdate(db, client_id, table_name);
        break;
      case INSERT:
        status = TransactionInsert(db);
        break;
      case SCAN:
        status = TransactionScan(db, client_id, table_name);
        break;
      case READMODIFYWRITE:
        status = TransactionReadModifyWrite(db);
        break;
      case RANDOM_INSERT:
        status = TransactionRandomInsert(db, client_id, table_name);
        break;
      case INSERT_BATCH:
        status = TransactionInsertBatch(db, client_id, table_name);
        break;
      default:
        std::cout << "[TGRIGGS_LOG] Unknown op: " << op_choice << std::endl;
        throw utils::Exception("Operation request is not recognized!");
    }
  } else {
    Operation op = client_to_op_[client_id];
    switch (op) {
      case READ:
        status = TransactionRead(db, client_id, table_name);
        break;
      case UPDATE:
        status = TransactionUpdate(db, client_id, table_name);
        break;
      case INSERT:
        status = TransactionInsert(db);
        break;
      case SCAN:
        status = TransactionScan(db, client_id, table_name);
        break;
      case READMODIFYWRITE:
        status = TransactionReadModifyWrite(db);
        break;
      case RANDOM_INSERT:
        status = TransactionRandomInsert(db, client_id, table_name);
        break;
      case INSERT_BATCH:
        status = TransactionInsertBatch(db, client_id, table_name);
        break;
      default:
        std::cout << "[FAIRDB_LOG] Unknown op: " << op << std::endl;
        throw utils::Exception("Operation request is not recognized!");
    }
  }

  return (status == DB::kOK);
}

DB::Status CoreWorkload::TransactionRead(DB &db, int client_id, std::string table_name) {
  uint64_t key_num = NextTransactionKeyNum(client_id);
  int offset = std::stoi(client_to_cf_offset[client_id]);

  uint64_t client_key_num = key_num + offset;
  // uint64_t client_key_num = key_num + (client_id%2) * (6250000 / 4);


  const std::string key = BuildKeyName(client_key_num);
  std::vector<DB::Field> result;
  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    return db.Read(table_name, key, &fields, result, client_id);
  } else {
    return db.Read(table_name, key, NULL, result, client_id);
  }
}

DB::Status CoreWorkload::TransactionReadModifyWrite(DB &db) {
  uint64_t key_num = NextTransactionKeyNum(0);
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> result;

  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    db.Read(table_name_, key, &fields, result, 0);
  } else {
    db.Read(table_name_, key, NULL, result, 0);
  }

  std::vector<DB::Field> values;
  if (write_all_fields()) {
    BuildValues(values);
  } else {
    BuildSingleValue(values);
  }
  return db.Update(table_name_, key, values);
}

DB::Status CoreWorkload::TransactionScan(DB &db, int client_id, std::string table_name) {
  uint64_t key_num = NextTransactionKeyNum(client_id);
  int len = 100;
  uint64_t client_key_num = std::min(key_num, uint64_t(3125000 - len));
  // uint64_t client_key_num = key_num + (client_id%2) * (6250000 / 4);

  const std::string key = BuildKeyName(client_key_num);
  // int len = scan_len_chooser_->Next();
  std::vector<std::vector<DB::Field>> result;
  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    return db.Scan(table_name, key, len, &fields, result, client_id);
  } else {
    return db.Scan(table_name, key, len, NULL, result, client_id);
  }
}

DB::Status CoreWorkload::TransactionUpdate(DB &db, int client_id, std::string table_name) {
  uint64_t key_num = NextTransactionKeyNum(client_id);
  // For multi-cf
  uint64_t client_key_num = key_num;
  // uint64_t client_key_num = key_num + (client_id) * (6250000 / 4);

  const std::string key = BuildKeyName(client_key_num);
  std::vector<DB::Field> values;
  if (write_all_fields()) {
    BuildValues(values);
  } else {
    BuildSingleValue(values);
  }
  return db.Update(table_name, key, values, client_id);
}

DB::Status CoreWorkload::TransactionRandomInsert(DB &db, int client_id, std::string table_name) {
  uint64_t key_num = NextTransactionKeyNum(client_id);
  uint64_t client_key_num = key_num;
  // uint64_t client_key_num = key_num + (client_id) * (6250000 / 4);

  const std::string key = BuildKeyName(client_key_num);
  std::vector<DB::Field> values;
  BuildValues(values);
  return db.Insert(table_name, key, values, client_id);
}

DB::Status CoreWorkload::TransactionInsertBatch(DB &db, int client_id, std::string table_name) {
  uint64_t key_num = NextTransactionKeyNum(client_id);
  // uint64_t key_num = transaction_insert_key_sequence_[client_id]->Next();
  int batch_size = 500;
  uint64_t client_key_num = key_num;
  client_key_num = std::min(key_num, uint64_t(3125000 - batch_size));

  // const std::string key = BuildKeyName(client_key_num);
  std::vector<DB::Field> values;
  BuildValues(values);
  return db.InsertBatch(table_name, client_key_num, values, batch_size, client_id);
}

DB::Status CoreWorkload::TransactionInsert(DB &db) {
  uint64_t key_num = transaction_insert_key_sequence_[0]->Next();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> values;
  BuildValues(values);
  DB::Status s = db.Insert(table_name_, key, values);
  transaction_insert_key_sequence_[0]->Acknowledge(key_num);
  return s;
}

} // ycsbc
