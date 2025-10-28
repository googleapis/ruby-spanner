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

describe Google::Cloud::Spanner::Transaction, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session1" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:tx_id) { "tx789" }
  let(:client) { spanner.client instance_id, database_id, pool: { min: 0, max: 4 } }
  let(:commit_time) { Time.now }
  let(:commit_timestamp) { Google::Cloud::Spanner::Convert.time_to_timestamp commit_time }
  let(:commit_resp) { Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: commit_timestamp }

  after do
    shutdown_client! client
  end

  it "does not send header x-goog-spanner-route-to-leader when LAR is disabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    mock.expect :execute_batch_dml, batch_response_grpc do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    mock.expect :commit, commit_resp do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    session.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = false

    client.transaction do |tx|
      tx.batch_update do |b|
        b.batch_update "UPDATE users SET active = true"
      end
    end

    mock.verify
  end

  
 it "sends header x-goog-spanner-route-to-leader when LAR is enabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :execute_batch_dml, batch_response_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :commit, commit_resp do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    session.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = true

    client.transaction do |tx|
      tx.batch_update do |b|
        b.batch_update "UPDATE users SET active = true"
      end
    end

    mock.verify
  end

  def batch_result_sets_metadata_grpc begin_transaction
    if begin_transaction
      Google::Cloud::Spanner::V1::ResultSetMetadata.new(
        transaction: Google::Cloud::Spanner::V1::Transaction.new(
          id: tx_id
        )
      )
    else
      nil
    end
  end

  def batch_result_sets_grpc count, row_count_exact: 1
    count.times.map.with_index do |_, index|
      Google::Cloud::Spanner::V1::ResultSet.new(
        metadata: batch_result_sets_metadata_grpc(index == 0), # include transaction in first result set
        stats: Google::Cloud::Spanner::V1::ResultSetStats.new(
          row_count_exact: row_count_exact
        )
      )
    end
  end

  def batch_response_grpc count = 1
    Google::Cloud::Spanner::V1::ExecuteBatchDmlResponse.new(
      result_sets: batch_result_sets_grpc(count),
      status: Google::Rpc::Status.new(code: 0)
    )
  end
end
