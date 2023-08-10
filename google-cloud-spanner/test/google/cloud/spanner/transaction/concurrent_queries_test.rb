# Copyright 2023 Google LLC
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

describe Google::Cloud::Spanner::Transaction, :execute_query, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:transaction_id) { "tx789" }
  let(:transaction_grpc) { Google::Cloud::Spanner::V1::Transaction.new id: transaction_id }
  # let(:transaction) { Google::Cloud::Spanner::Transaction.from_grpc transaction_grpc, session }
  let(:transaction) { Google::Cloud::Spanner::Transaction.from_grpc nil, session }
  let(:tx_selector) { Google::Cloud::Spanner::V1::TransactionSelector.new id: transaction_id }
  let(:tx_selector_begin) do
    Google::Cloud::Spanner::V1::TransactionSelector.new(
      begin: Google::Cloud::Spanner::V1::TransactionOptions.new(
        read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new
      )
    )
  end
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let :results_hash do
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
        transaction: { id: transaction_id },
      },
      values: [
        { string_value: "1" },
        { string_value: "Charlie" },
        { bool_value: true},
        { string_value: "29" },
        { number_value: 0.9 },
        { string_value: "2017-01-02T03:04:05.060000000Z" },
        { string_value: "1950-01-01" },
        { string_value: "aW1hZ2U=" },
        { list_value: { values: [ { string_value: "1"},
                                 { string_value: "2"},
                                 { string_value: "3"} ]}}
      ]
    }
  end
  let(:results_grpc) { Google::Cloud::Spanner::V1::PartialResultSet.new results_hash }
  let(:results_enum) { Array(results_grpc).to_enum }

  let(:results_enum_tx_1) do
    rh = results_hash
    rh[:metadata][:transaction][:id] = "tx123"
    Array( Google::Cloud::Spanner::V1::PartialResultSet.new rh ).to_enum 
  end
  let(:results_enum_tx_2) do
    rh = results_hash
    rh[:metadata][:transaction][:id] = "tx456"
    Array( Google::Cloud::Spanner::V1::PartialResultSet.new rh ).to_enum 
  end

  # focus
  it "tests concurrent queries in a transaction" do
    mock = Minitest::Mock.new
    session.service.mocked_service = mock

    mock.expect :execute_streaming_sql, results_enum do |values|
      sleep 2 # simulate delayed response of rpc 
      values[:transaction] == tx_selector_begin
    end

    mock.expect :execute_streaming_sql, results_enum do |values|
      values[:transaction] == tx_selector
    end

    results_1 = nil
    results_2 = nil
    begin
      t1 = Thread.new do
        results_1 = transaction.execute_query "SELECT * FROM users"
      end
      sleep 1 # Ensure t1 initiates "begin" selector instead of t2
      t2 = Thread.new do
        results_2 = transaction.execute_query "SELECT * FROM users"
      end
    ensure
      t1.join
      t2.join
    end

    mock.verify
  end

  # focus
  it "throws exception for first operation, so second operation initiates inline" do

    mock = Minitest::Mock.new
    session.service.mocked_service = mock

    mock.expect :execute_streaming_sql, results_enum do |values|
      sleep 2 # simulate delayed response of rpc
      values[:transaction] == tx_selector_begin
      raise Google::Cloud::InvalidArgumentError
    end

    mock.expect :execute_streaming_sql, results_enum do |values|
      values[:transaction] == tx_selector_begin
    end

    results_1 = nil
    results_2 = nil
    begin
      t1 = Thread.new do
        assert_raises Google::Cloud::InvalidArgumentError do
          results_1 = transaction.execute_query "SELECT * FROM users"
        end
        _(transaction.no_existing_transaction?).must_equal true
      end
      sleep 1 # Ensure t1 initiates "begin" selector before t2
      t2 = Thread.new do
        results_2 = transaction.execute_query "SELECT * FROM users"
        _(transaction.transaction_id).must_equal transaction_id
      end
    ensure
      t1.join
      t2.join
    end

    mock.verify
  end

end
