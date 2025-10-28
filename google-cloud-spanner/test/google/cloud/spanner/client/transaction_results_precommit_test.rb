# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "helper"

describe Google::Cloud::Spanner::Client, :transaction, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:tx_selector_inline_begin) do
      Google::Cloud::Spanner::V1::TransactionSelector.new(
        begin: Google::Cloud::Spanner::V1::TransactionOptions.new(
          read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new(
            read_lock_mode: :READ_LOCK_MODE_UNSPECIFIED
          )
        )
      )
  end

  let (:tx_id) {"$abc123"}
  let(:tx) do
    {
      id: tx_id,
    }
  end

  let(:tx_selector_with_id) { Google::Cloud::Spanner::V1::TransactionSelector.new id: tx_id }
  
  let(:client) { spanner.client instance_id, database_id }

  let(:precommit_token_0) {"hello"}
  let(:precommit_token_1) {"goodbye"}


  describe :read do
    let :results_hash1_tx do
      {
        metadata: {
          row_type: {
            fields: [
              { name: "id",          type: { code: :INT64 } },
              { name: "name",        type: { code: :STRING } },
              { name: "active",      type: { code: :BOOL } },
              { name: "age",         type: { code: :INT64 } },
              { name: "score",       type: { code: :FLOAT64 } },
              { name: "updated_at",  type: { code: :TIMESTAMP } },
              { name: "birthday",    type: { code: :DATE} },
              { name: "avatar",      type: { code: :BYTES } },
              { name: "project_ids", type: { code: :ARRAY,
                                            array_element_type: { code: :INT64 } } }
            ]
          },
          transaction: tx
        },
        precommit_token: {
          precommit_token: precommit_token_0,
          seq_num: 0,
        }
      }
    end
    let :results_hash2 do
      {
        values: [
          { string_value: "1" },
          { string_value: "Charlie" }
        ],
      }
    end
    let :results_hash3 do
      {
        values: [
          { bool_value: true},
          { string_value: "29" }
        ]
      }
    end
    let :results_hash4 do
      {
        values: [
          { number_value: 0.9 },
          { string_value: "2017-01-02T03:04:05.060000000Z" }
        ],
      }
    end
    let :results_hash5 do
      {
        values: [
          { string_value: "1950-01-01" },
          { string_value: "aW1hZ2U=" },
        ]
      }
    end
    let :results_hash6 do
      {
        values: [
          { list_value: { values: [ { string_value: "1"},
                                  { string_value: "2"},
                                  { string_value: "3"} ]}}
        ],
        precommit_token: {
          precommit_token: precommit_token_1,
          seq_num: 1,
        }
      }
    end
    let :partial_results6 do
      # need this to grab a protobuf form of precommit token for commit mock
      Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash6)
    end
    let(:results_enum) do
      [
        Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash1_tx),
        Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash2),
        Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash3),
        Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash4),
        Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash5),
        partial_results6
      ].to_enum
    end

    let(:commit_resp) do
      Google::Cloud::Spanner::V1::CommitResponse.new(
        commit_timestamp: Google::Cloud::Spanner::Convert.time_to_timestamp(Time.now),
        commit_stats: Google::Cloud::Spanner::V1::CommitResponse::CommitStats.new(
          mutation_count: 5
        )
      )
    end

    it "read updates precommit token on transaction" do
      columns = [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]

      service_mock = Minitest::Mock.new
      service_mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
      
      streaming_read_request = [{
        session: session_grpc.name,
        table: "my-table",
        columns: ["id", "name", "active", "age", "score", "updated_at", "birthday", "avatar", "project_ids"],
        key_set: Google::Cloud::Spanner::V1::KeySet.new(all: true), 
        transaction: tx_selector_inline_begin,
        index: nil, limit: nil, resume_token: nil, partition_token: nil,
        request_options: nil,
        order_by: nil, lock_hint: nil
      }, default_options]

      service_mock.expect :streaming_read, RaiseableEnumerator.new(results_enum), streaming_read_request

      commit_request = [{
        session: session_grpc.name, 
        transaction_id: tx_id,
        single_use_transaction: nil,
        mutations: [],
        request_options: nil,
        precommit_token: partial_results6.precommit_token
      }, default_options]

      service_mock.expect :commit, commit_resp, commit_request

      spanner.service.mocked_service = service_mock

      # @type [::Google::Cloud::Spanner::Client]
      sp_client = client
      
      sp_client.transaction do |tx|
        results = tx.read("my-table", columns)
        _(tx.precommit_token.precommit_token).must_equal precommit_token_0
        _(tx.precommit_token.seq_num).must_equal 0
        results.rows.to_a
        _(tx.precommit_token.precommit_token).must_equal precommit_token_1
        _(tx.precommit_token.seq_num).must_equal 1
      end

      shutdown_client! sp_client

      service_mock.verify
    end
  end

  describe :execute_sql do
    let(:sql_query) { "SELECT * FROM users" }

    let :metadata_result do
    {
      metadata: {
        row_type: {
          fields: [
            { name: "id",          type: { code: :INT64 } },
            { name: "name",        type: { code: :STRING } },
            { name: "active",      type: { code: :BOOL } },
            { name: "age",         type: { code: :INT64 } },
            { name: "score",       type: { code: :FLOAT64 } },
            { name: "updated_at",  type: { code: :TIMESTAMP } },
            { name: "birthday",    type: { code: :DATE} },
            { name: "avatar",      type: { code: :BYTES } },
            { name: "project_ids", type: { code: :ARRAY,
                                           array_element_type: { code: :INT64 } } }
          ]
        },
        transaction: tx
      },
      precommit_token: {
        precommit_token: precommit_token_0,
        seq_num: 0,
      }
    }
    end
    let :partial_row_1 do
      {
        values: [
          { string_value: "1" },
          { string_value: "Charlie" }
        ],
      }
    end
    let :partial_row_2 do
      {
        values: [
          { bool_value: true},
          { string_value: "29" }
        ]
      }
    end
    let :partial_row_3 do
      {
        values: [
          { number_value: 0.9 },
          { string_value: "2017-01-02T03:04:05.060000000Z" }
        ],
      }
    end
    let :partial_row_4 do
      {
        values: [
          { string_value: "1950-01-01" },
          { string_value: "aW1hZ2U=" },
        ]
      }
    end
    let :partial_row_5 do
      {
        values: [
          { list_value: { values: [ { string_value: "1"},
                                  { string_value: "2"},
                                  { string_value: "3"} ]}}
        ],
        precommit_token: {
          precommit_token: precommit_token_1,
          seq_num: 1,
        }
      }
    end

    let(:partial_results_5) do
      # need this to grab a protobuf form of precommit token for commit mock
       Google::Cloud::Spanner::V1::PartialResultSet.new(partial_row_5)
    end

    let(:commit_resp) do
      Google::Cloud::Spanner::V1::CommitResponse.new(
        commit_timestamp: Google::Cloud::Spanner::Convert.time_to_timestamp(Time.now),
      )
    end

    it "execute_query updates precommit token on transaction" do
      resulting_stream = [
        Google::Cloud::Spanner::V1::PartialResultSet.new(metadata_result),
        Google::Cloud::Spanner::V1::PartialResultSet.new(partial_row_1),
        Google::Cloud::Spanner::V1::PartialResultSet.new(partial_row_2),
        Google::Cloud::Spanner::V1::PartialResultSet.new(partial_row_3),
        Google::Cloud::Spanner::V1::PartialResultSet.new(partial_row_4),
        partial_results_5
      ].to_enum

      service_mock = Minitest::Mock.new
      service_mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]

      execute_streaming_sql_request = [{
        session: session_grpc.name,
        sql: sql_query,
        transaction: tx_selector_inline_begin,
        params: nil, param_types: nil,
        resume_token: nil, partition_token: nil,
        seqno: 1,
        query_options: nil, request_options: nil, directed_read_options: nil
      }, default_options]

      service_mock.expect :execute_streaming_sql, RaiseableEnumerator.new(resulting_stream), execute_streaming_sql_request
      
      commit_request = [{
        session: session_grpc.name, 
        transaction_id: tx_id,
        single_use_transaction: nil,
        mutations: [],
        request_options: nil,
        precommit_token: partial_results_5.precommit_token
      }, default_options]

      service_mock.expect :commit, commit_resp, commit_request

      spanner.service.mocked_service = service_mock

      # @type [::Google::Cloud::Spanner::Client]
      sp_client = client
      
      sp_client.transaction do |tx|
        results = tx.execute_query sql_query
        _(tx.precommit_token.precommit_token).must_equal precommit_token_0
        _(tx.precommit_token.seq_num).must_equal 0
        results.rows.to_a
                _(tx.precommit_token.precommit_token).must_equal precommit_token_1
        _(tx.precommit_token.seq_num).must_equal 1
      end

      shutdown_client! sp_client
      service_mock.verify
    end
  end
end
