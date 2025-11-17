# Copyright 2017 Google LLC
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

require "google/rpc/error_details_pb"
require "helper"

describe Google::Cloud::Spanner::Client, :transaction, :retry, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:transaction_id) { "tx789" }
  let(:transaction_grpc) { Google::Cloud::Spanner::V1::Transaction.new id: transaction_id }
  let(:transaction) { Google::Cloud::Spanner::Transaction.from_grpc transaction_grpc, session }
  let(:tx_opts) { Google::Cloud::Spanner::V1::TransactionOptions.new(read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new) }
  let(:tx_selector_id) { Google::Cloud::Spanner::V1::TransactionSelector.new id: transaction_id }
  let(:tx_selector_begin) do
    Google::Cloud::Spanner::V1::TransactionSelector.new(
      begin: Google::Cloud::Spanner::V1::TransactionOptions.new(
        read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new
      )
    )
  end
  let(:tx_selector_begin_retry_1) do
    Google::Cloud::Spanner::V1::TransactionSelector.new(
      begin: Google::Cloud::Spanner::V1::TransactionOptions.new(
        read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new(
          multiplexed_session_previous_transaction_id: transaction_id
        )
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
  let(:client) { spanner.client instance_id, database_id }
  let(:tx_opts) { Google::Cloud::Spanner::V1::TransactionOptions.new(read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new) }

  it "retries aborted transactions without retry metadata" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options

    def mock.commit *args
      # first time called this will raise
      if @called == nil
        @called = true
        gax_error = Google::Cloud::AbortedError.new "aborted"
        gax_error.instance_variable_set :@cause, GRPC::BadStatus.new(10, "aborted")
        raise gax_error
      end
      # second call will return correct response
      Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new()
    end
    mock.expect :sleep, nil, [1.3]

    client.define_singleton_method :sleep do |count|
      # call the mock to satisfy the expectation
      mock.sleep count
    end

    results = nil
    client.transaction do |tx|
      _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
      results = tx.execute_query "SELECT * FROM users"
      tx.update "users", [{ id: 1, name: "Charlie", active: false }]
    end

    assert_results results

    shutdown_client! client

    mock.verify
  end

  it "retries aborted transactions with retry metadata seconds" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options

    def mock.commit *args
      # first time called this will raise
      if @called == nil
        @called = true
        raise GRPC::Aborted.new "aborted", create_retry_info_metadata(60, 0)
      end
      # second call will return correct response
      Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new()
    end
    mock.expect :sleep, nil, [60]

    client.define_singleton_method :sleep do |count|
      # call the mock to satisfy the expectation
      mock.sleep count
    end

    results = nil
    client.transaction do |tx|
      _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
      results = tx.execute_query "SELECT * FROM users"
      tx.update "users", [{ id: 1, name: "Charlie", active: false }]
    end

    assert_results results

    shutdown_client! client

    mock.verify
  end

  it "retries aborted transactions with retry metadata seconds and nanos" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options

    def mock.commit *args
      # first time called this will raise
      if @called == nil
        @called = true
        raise GRPC::Aborted.new "aborted", create_retry_info_metadata(123, 456000000)
      end
      # second call will return correct response
      Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new()
    end
    mock.expect :sleep, nil, [123.456]

    client.define_singleton_method :sleep do |count|
      # call the mock to satisfy the expectation
      mock.sleep count
    end

    results = nil
    client.transaction do |tx|
      _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
      results = tx.execute_query "SELECT * FROM users"
      tx.update "users", [{ id: 1, name: "Charlie", active: false }]
    end

    assert_results results

    shutdown_client! client

    mock.verify
  end

  it "retries multiple aborted transactions" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options

    def mock.commit *args
      # first time called this will raise
      if @called == nil
        @called = false
        raise GRPC::Aborted.new "aborted"
      end
      if @called == false
        @called = true
        raise GRPC::Aborted.new "aborted", create_retry_info_metadata(30, 0)
      end
      # third call will return correct response
      Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new()
    end
    mock.expect :sleep, nil, [1.3]
    mock.expect :sleep, nil, [30]

    client.define_singleton_method :sleep do |count|
      # call the mock to satisfy the expectation
      mock.sleep count
    end

    results = nil
    client.transaction do |tx|
      _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
      results = tx.execute_query "SELECT * FROM users"
      tx.update "users", [{ id: 1, name: "Charlie", active: false }]
    end

    assert_results results

    shutdown_client! client

    mock.verify
  end

  it "sets previous_transaction_id when retrying multiple aborted transactions after transaction is created" do
    # Other test methods in this file elide the details of multiple retries for the sake of directness.
    # So e.g. they have the second call return the results object with the same transaction as the first call.
    # This method takes pains to model things a bit closer to actual flow.

    columns = [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    
    mock.expect :streaming_read, results_enum, [{
      session: session_grpc.name, table: "my-table",
      columns: ["id", "name", "active", "age", "score", "updated_at", "birthday", "avatar", "project_ids"],
      key_set: Google::Cloud::Spanner::V1::KeySet.new(keys: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1]).list_value, Google::Cloud::Spanner::Convert.object_to_grpc_value([2]).list_value, Google::Cloud::Spanner::Convert.object_to_grpc_value([3]).list_value]),
      transaction: tx_selector_begin, index: nil, limit: nil, resume_token: nil, partition_token: nil,
      request_options: nil,
      order_by: nil, lock_hint: nil
    }, default_options]
    # first execute_streaming_sql is not a retry, it is a request that runs on a transaction that streaming_read created
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_id, seqno: 2, options: default_options


    # On the first retry, the initial transaction is `previous` in inline-begin of `streaming_read`
    # And the first retry transaction is created, returned from `streaming_read` and then used in the `execute_streaming_sql`
    transaction_id_retry_1 = "tx_retry1"
    tx_selector_retry_1 = Google::Cloud::Spanner::V1::TransactionSelector.new id: "tx_retry1"

    hash_retry1 = results_hash.clone
    hash_retry1[:metadata][:transaction][:id] = "tx_retry1"
    results_enum_retry_1 = Array(Google::Cloud::Spanner::V1::PartialResultSet.new(hash_retry1)).to_enum

    mock.expect :streaming_read, results_enum_retry_1, [{
      session: session_grpc.name, table: "my-table",
      columns: ["id", "name", "active", "age", "score", "updated_at", "birthday", "avatar", "project_ids"],
      key_set: Google::Cloud::Spanner::V1::KeySet.new(keys: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1]).list_value, Google::Cloud::Spanner::Convert.object_to_grpc_value([2]).list_value, Google::Cloud::Spanner::Convert.object_to_grpc_value([3]).list_value]),
      transaction: tx_selector_begin_retry_1, index: nil, limit: nil, resume_token: nil, partition_token: nil,
      request_options: nil,
      order_by: nil, lock_hint: nil
    }, default_options]
    expect_execute_streaming_sql results_enum_retry_1, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_retry_1, seqno: 2, options: default_options

    # On the second retry, the first retry transaction is `previous` in inline-begin of `streaming_read`
    tx_selector_begin_retry_2 =Google::Cloud::Spanner::V1::TransactionSelector.new(
      begin: Google::Cloud::Spanner::V1::TransactionOptions.new(
        read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new(multiplexed_session_previous_transaction_id: transaction_id_retry_1)
      )
    )
    # And the second retry transaction is created, returned from `streaming_read` and then used in the `execute_streaming_sql`
    transaction_id_retry_2 = "tx_retry2"
    tx_selector_retry_2 = Google::Cloud::Spanner::V1::TransactionSelector.new id: "tx_retry2"

    hash_retry2 = results_hash.clone
    hash_retry2[:metadata][:transaction][:id] = transaction_id_retry_2
    results_enum_retry_2 = Array(Google::Cloud::Spanner::V1::PartialResultSet.new(hash_retry2)).to_enum

    mock.expect :streaming_read, results_enum_retry_2, [{
      session: session_grpc.name, table: "my-table",
      columns: ["id", "name", "active", "age", "score", "updated_at", "birthday", "avatar", "project_ids"],
      key_set: Google::Cloud::Spanner::V1::KeySet.new(keys: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1]).list_value, Google::Cloud::Spanner::Convert.object_to_grpc_value([2]).list_value, Google::Cloud::Spanner::Convert.object_to_grpc_value([3]).list_value]),
      transaction: tx_selector_begin_retry_2, index: nil, limit: nil, resume_token: nil, partition_token: nil,
      request_options: nil,
      order_by: nil, lock_hint: nil
    }, default_options]
  
    expect_execute_streaming_sql results_enum_retry_2, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_retry_2, seqno: 2, options: default_options
    # after this the second retry transaction will be committed
   
    def mock.commit *args
      # first time called this will raise
      if @called == nil
        @called = false
        raise GRPC::Aborted.new "aborted"
      end
      if @called == false
        @called = true
        raise GRPC::Aborted.new "aborted", create_retry_info_metadata(30, 0)
      end

      # we are committing the second retry transaction
      raise unless args[0][:transaction_id] == "tx_retry2"

      # third call will return correct response
      Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new()
    end
    mock.expect :sleep, nil, [1.3]
    mock.expect :sleep, nil, [30]

    client.define_singleton_method :sleep do |count|
      # call the mock to satisfy the expectation
      mock.sleep count
    end

    results = nil
    client.transaction do |tx|
      _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
      # this tx_read creates a transaction, id of which we should see as `previous` in the retries
      _read_res = tx.read "my-table", columns, keys: [1, 2, 3]
      results = tx.execute_query "SELECT * FROM users"
      tx.update "users", [{ id: 1, name: "Charlie", active: false }]
    end

    assert_results results

    shutdown_client! client

    mock.verify
  end

  it "retries with incremental backoff until deadline has passed" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options

    def mock.commit *args
      raise GRPC::Aborted.new "aborted"
    end
    mock.expect :sleep, nil, [1.3]
    mock.expect :sleep, nil, [1.6900000000000002]
    mock.expect :sleep, nil, [2.1970000000000005]
    mock.expect :sleep, nil, [2.856100000000001]

    mock.expect :current_time, Time.now, []
    mock.expect :current_time, Time.now, []
    mock.expect :current_time, Time.now + 30, []
    mock.expect :current_time, Time.now + 60, []
    mock.expect :current_time, Time.now + 90, []
    mock.expect :current_time, Time.now + 125, []

    client.define_singleton_method :sleep do |count|
      # call the mock to satisfy the expectation
      mock.sleep count
    end
    client.define_singleton_method :current_time do
      # call the mock to satisfy the expectation
      mock.current_time
    end

    assert_raises Google::Cloud::AbortedError do
      client.transaction do |tx|
        _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
        results = tx.execute_query "SELECT * FROM users"
        tx.update "users", [{ id: 1, name: "Charlie", active: false }]
      end
    end

    shutdown_client! client

    mock.verify
  end

  it "retries internal error with rst stream error" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin, seqno: 1, options: default_options
    expect_execute_streaming_sql results_enum, session_grpc.name, "SELECT * FROM users", transaction: tx_selector_begin_retry_1, seqno: 1, options: default_options

    def mock.commit *args
      # first time called this will raise
      if @called == nil
        @called = true
        raise GRPC::Internal.new "Received RST_STREAM error"
      end
      # second call will return correct response
      Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new()
    end
    mock.expect :sleep, nil, [1.3]

    client.define_singleton_method :sleep do |count|
      # call the mock to satisfy the expectation
      mock.sleep count
    end

    results = nil
    client.transaction do |tx|
      _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
      results = tx.execute_query "SELECT * FROM users"
      tx.update "users", [{ id: 1, name: "Charlie", active: false }]
    end

    assert_results results

    shutdown_client! client

    mock.verify
  end

  it "raises internal error if non retryable" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      )
    ]

    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    mock.expect :begin_transaction, transaction_grpc, [{
        session: session_grpc.name, 
        options: tx_opts, 
        request_options: nil,
        mutation_key: nil
      }, default_options]

    def mock.commit *args
      # first time called this will raise
      if @called == nil
        @called = true
        raise GRPC::Internal.new "Other error"
      end
      # second call will return correct response expect to be not called twice
      Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: Google::Protobuf::Timestamp.new()
    end
    
    assert_raises GRPC::Internal do
      client.transaction do |tx|
        _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
      end
    end

    shutdown_client! client

    mock.verify
  end

  def assert_results results
    _(results).must_be_kind_of Google::Cloud::Spanner::Results

    _(results.fields).wont_be :nil?
    _(results.fields).must_be_kind_of Google::Cloud::Spanner::Fields
    _(results.fields.keys.count).must_equal 9
    _(results.fields[:id]).must_equal          :INT64
    _(results.fields[:name]).must_equal        :STRING
    _(results.fields[:active]).must_equal      :BOOL
    _(results.fields[:age]).must_equal         :INT64
    _(results.fields[:score]).must_equal       :FLOAT64
    _(results.fields[:updated_at]).must_equal  :TIMESTAMP
    _(results.fields[:birthday]).must_equal    :DATE
    _(results.fields[:avatar]).must_equal      :BYTES
    _(results.fields[:project_ids]).must_equal [:INT64]

    rows = results.rows.to_a # grab them all from the enumerator
    _(rows.count).must_equal 1
    row = rows.first
    _(row).must_be_kind_of Google::Cloud::Spanner::Data
    _(row.keys).must_equal [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]
    _(row[:id]).must_equal 1
    _(row[:name]).must_equal "Charlie"
    _(row[:active]).must_equal true
    _(row[:age]).must_equal 29
    _(row[:score]).must_equal 0.9
    _(row[:updated_at]).must_equal Time.parse("2017-01-02T03:04:05.060000000Z")
    _(row[:birthday]).must_equal Date.parse("1950-01-01")
    _(row[:avatar]).must_be_kind_of StringIO
    _(row[:avatar].read).must_equal "image"
    _(row[:project_ids]).must_equal [1, 2, 3]
  end
end

def create_retry_info_metadata seconds, nanos
  metadata = {}
  retry_info = Google::Rpc::RetryInfo.new(retry_delay: Google::Protobuf::Duration.new(seconds: seconds, nanos: nanos))
  metadata["google.rpc.retryinfo-bin"] = Google::Rpc::RetryInfo.encode(retry_info)
  metadata
end
