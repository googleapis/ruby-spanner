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

describe Google::Cloud::Spanner::Client, :batch_write, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:commit_time) { Time.now }
  let(:commit_timestamp) { Google::Cloud::Spanner::Convert.time_to_timestamp commit_time }
  let(:batch_write_resp) {
    Google::Cloud::Spanner::V1::BatchWriteResponse.new(
      status: Google::Rpc::Status.new(code: 0),
      indexes: [5, 5]
    )
  }
  let(:results_grpc) { Array(batch_write_resp).to_enum }
  let(:commit_stats_grpc) {
    Google::Cloud::Spanner::V1::CommitResponse::CommitStats.new(
      mutation_count: 5
    )
  }
  let(:commit_stats_resp_grpc) {
    Google::Cloud::Spanner::V1::CommitResponse.new(
      commit_timestamp: commit_timestamp, commit_stats: commit_stats_grpc
    )
  }
  let(:tx_opts) { Google::Cloud::Spanner::V1::TransactionOptions.new(read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new) }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:client) { spanner.client instance_id, database_id, pool: { min: 0 } }
  let(:user_mutations) {
    [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Charlie", false]).list_value]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([2, "Harvey", true]).list_value]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        insert_or_update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([3, "Marley", false]).list_value]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        replace: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "users", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([4, "Henry", true]).list_value]
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
  }
  let(:admin_mutations) {
    [
      Google::Cloud::Spanner::V1::Mutation.new(
        update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "admins", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([1, "Lucas", false]).list_value]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        insert: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "admins", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([2, "James", true]).list_value]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        insert_or_update: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "admins", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([3, "Benjamin", false]).list_value]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        replace: Google::Cloud::Spanner::V1::Mutation::Write.new(
          table: "admins", columns: %w(id name active),
          values: [Google::Cloud::Spanner::Convert.object_to_grpc_value([4, "Jordan", true]).list_value]
        )
      ),
      Google::Cloud::Spanner::V1::Mutation.new(
        delete: Google::Cloud::Spanner::V1::Mutation::Delete.new(
          table: "admins", key_set: Google::Cloud::Spanner::V1::KeySet.new(
            keys: [1, 2, 3, 4, 5].map do |i|
              Google::Cloud::Spanner::Convert.object_to_grpc_value([i]).list_value
            end
          )
        )
      )
    ]
  }
  let(:mutation_groups) {
    [
      Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(mutations: user_mutations),
      Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(mutations: admin_mutations)
    ]
  }

  it "batch writes using groups of mutations" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc, [{database: database_path(instance_id, database_id), session: nil}, default_options]
    mock.expect :batch_write, batch_write_resp, [{ session: session_grpc.name, mutation_groups: mutation_groups, request_options: nil }, default_options]
    spanner.service.mocked_service = mock

    results = client.batch_write do |b|
      b.mutation_group do |mg|
        mg.update "users", [{ id: 1, name: "Charlie", active: false }]
        mg.insert "users", [{ id: 2, name: "Harvey",  active: true }]
        mg.upsert "users", [{ id: 3, name: "Marley",  active: false }]
        mg.replace "users", [{ id: 4, name: "Henry",  active: true }]
        mg.delete "users", [1, 2, 3, 4, 5]
      end
      b.mutation_group do |mg|
        mg.update "admins", [{ id: 1, name: "Lucas", active: false }]
        mg.insert "admins", [{ id: 2, name: "James",  active: true }]
        mg.upsert "admins", [{ id: 3, name: "Benjamin",  active: false }]
        mg.replace "admins", [{ id: 4, name: "Jordan",  active: true }]
        mg.delete "admins", [1, 2, 3, 4, 5]
      end
    end

    _(results).must_be_kind_of Google::Protobuf::RepeatedField

    shutdown_client! client

    mock.verify
  end

  it "raises ArgumentError if no block is provided" do
    expect do
      client.batch_write
    end.must_raise ArgumentError
  end
end
