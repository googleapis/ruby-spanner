# Copyright 2026 Google LLC
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
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id), multiplexed: true }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:transaction_id) { "tx789" }
  let(:transaction_grpc) { Google::Cloud::Spanner::V1::Transaction.new id: transaction_id }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:client) { spanner.client instance_id, database_id }
  let(:tx_opts) { Google::Cloud::Spanner::V1::TransactionOptions.new(read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new) }
  let(:commit_time) { Time.now }
  let(:commit_timestamp) { Google::Cloud::Spanner::Convert.time_to_timestamp commit_time }
  let(:commit_resp) { Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: commit_timestamp }

  it "passes transaction tag to BeginTransaction when transaction_id is called lazily" do
    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    
    mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]

    # This is the call we are testing. It should have the transaction_tag.
    expected_tx_opts = Google::Cloud::Spanner::V1::TransactionOptions.new(
      read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new(
        read_lock_mode: :READ_LOCK_MODE_UNSPECIFIED,
        multiplexed_session_previous_transaction_id: ""
      ),
      exclude_txn_from_change_streams: false,
      isolation_level: :ISOLATION_LEVEL_UNSPECIFIED
    )

    mock.expect :begin_transaction, transaction_grpc, [{
        session: session_grpc.name, 
        options: expected_tx_opts, 
        request_options: { transaction_tag: "Tag-1" },
        mutation_key: nil
      }, default_options]

    mock.expect :commit, commit_resp, [{
      session: session_grpc.name, 
      mutations: [], 
      transaction_id: transaction_id, 
      single_use_transaction: nil,
      request_options: { transaction_tag: "Tag-1" },
      precommit_token: nil
    }, default_options]

    client.transaction request_options: { tag: "Tag-1" } do |tx|
      # Calling transaction_id triggers safe_begin_transaction!
      id = tx.transaction_id
      _(id).must_equal transaction_id
    end

    mock.verify
  end

  it "passes transaction tag when calling Session#create_transaction explicitly" do
    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    
    expected_tx_opts = Google::Cloud::Spanner::V1::TransactionOptions.new(
      read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new(
        read_lock_mode: :READ_LOCK_MODE_UNSPECIFIED,
        multiplexed_session_previous_transaction_id: ""
      ),
      exclude_txn_from_change_streams: false,
      isolation_level: :ISOLATION_LEVEL_UNSPECIFIED
    )

    mock.expect :begin_transaction, transaction_grpc, [{
        session: session_grpc.name, 
        options: expected_tx_opts, 
        request_options: { transaction_tag: "Tag-1" },
        mutation_key: nil
      }, default_options]

    tx = session.create_transaction request_options: { tag: "Tag-1" }
    _(tx).must_be_kind_of Google::Cloud::Spanner::Transaction
    _(tx.transaction_id).must_equal transaction_id

    mock.verify
  end
end
