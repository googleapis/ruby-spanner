# Copyright 2024 Google LLC
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

describe Google::Cloud::Spanner::Session, :read, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:commit_time) { Time.now }
  let(:commit_timestamp) { Google::Cloud::Spanner::Convert.time_to_timestamp commit_time }
  let(:commit_resp) { Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: commit_timestamp }
  let(:tx_opts) { Google::Cloud::Spanner::V1::TransactionOptions.new(read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new) }
  let(:tx_opts_with_change_stream_exclusion) {
    Google::Cloud::Spanner::V1::TransactionOptions.new read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new,
                                                       exclude_txn_from_change_streams: true
  }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }

  users = {
      1 => Spanner::Testing::Data::User.new(id: 1, name: "Charlie", active: false),
      2 => Spanner::Testing::Data::User.new(id: 2, name: "Harvey", active: true),
      3 => Spanner::Testing::Data::User.new(id: 3, name: "Marley", active: false),
      4 => Spanner::Testing::Data::User.new(id: 4, name: "Henry", active: true)
  }

  it "commits using a block" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[1], :PROTO)])]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[2], :PROTO)])]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        insert_or_update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[3], :PROTO)])]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        replace: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[4], :PROTO)])]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        delete: Google::Cloud::Spanner::V1::Mutation::Delete.new(
          table: "users", key_set: Google::Cloud::Spanner::V1::KeySet.new(
            keys: [1, 2, 3, 4, 5].map do |i|
              Google::Cloud::Spanner::Convert.object_to_grpc_value([i]).list_value
            end
          )
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]

    session.service.mocked_service = mock

    timestamp = session.commit do |c|
      c.update "users", [users[1]]
      c.insert "users", [users[2]]
      c.upsert "users", [users[3]]
      c.replace "users", [users[4]]
      c.delete "users", [1, 2, 3, 4, 5]
    end
    _(timestamp).must_equal commit_time

    mock.verify
  end



  it "updates directly" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[1], :PROTO)])]
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]
    session.service.mocked_service = mock
    timestamp = session.update "users", [users[1]]
    _(timestamp).must_equal commit_time

    mock.verify
  end

  it "inserts directly" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[2], :PROTO)])]
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]
    session.service.mocked_service = mock

    timestamp = session.insert "users", [users[2]]
    _(timestamp).must_equal commit_time

    mock.verify
  end

  it "upserts directly" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        insert_or_update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[3], :PROTO)])]
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]
    session.service.mocked_service = mock

    timestamp = session.upsert "users", [users[3]]
    _(timestamp).must_equal commit_time

    mock.verify
  end

  it "replaces directly" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        replace: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Protobuf::ListValue.new(values: [Google::Cloud::Spanner::Convert.object_to_grpc_value(users[3], :PROTO)])]
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]
    session.service.mocked_service = mock

    timestamp = session.replace "users", [users[3]]
    _(timestamp).must_equal commit_time

    mock.verify
  end

  it "deletes multiple rows of key ranges directly" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        delete: Google::Cloud::Spanner::V1::Mutation::Delete.new(
          table: "users", key_set: Google::Cloud::Spanner::V1::KeySet.new(
            ranges: [Google::Cloud::Spanner::Convert.to_key_range(1..100)]
          )
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]
    session.service.mocked_service = mock

    timestamp = session.delete "users", 1..100
    _(timestamp).must_equal commit_time

    mock.verify
  end

  it "deletes a single rows directly" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        delete: Google::Cloud::Spanner::V1::Mutation::Delete.new(
          table: "users", key_set: Google::Cloud::Spanner::V1::KeySet.new(
            keys: [5].map do |i|
              Google::Cloud::Spanner::Convert.object_to_grpc_value([i]).list_value
            end
          )
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]
    session.service.mocked_service = mock

    timestamp = session.delete "users", 5
    _(timestamp).must_equal commit_time

    mock.verify
  end

  it "deletes all rows directly" do
    mutations = [
      Google::Cloud::Spanner::V1::Mutation.new(
        delete: Google::Cloud::Spanner::V1::Mutation::Delete.new(
          table: "users", key_set: Google::Cloud::Spanner::V1::KeySet.new(all: true)
        )
      )
    ]

    mock = Minitest::Mock.new
    mock.expect :commit, commit_resp, [{ session: session.path, mutations: mutations, transaction_id: nil, single_use_transaction: tx_opts, request_options: nil, precommit_token: nil }, default_options]
    session.service.mocked_service = mock

    timestamp = session.delete "users"
    _(timestamp).must_equal commit_time

    mock.verify
  end
end
