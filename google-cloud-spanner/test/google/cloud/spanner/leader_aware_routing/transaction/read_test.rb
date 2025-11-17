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
  let(:tx) do
    {
      id: tx_id,
    }
  end
  let(:client) { spanner.client instance_id, database_id, pool: { min: 0, max: 4 } }
  let(:commit_time) { Time.now }
  let(:commit_timestamp) { Google::Cloud::Spanner::Convert.time_to_timestamp commit_time }
  let(:commit_resp) { Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: commit_timestamp }
  let :results_hash do
    {
      metadata: {
        row_type: {
          fields: [
            { type: { code: :INT64 } }
          ]
        },
        transaction: tx
      },
      values: [
        { string_value: "1" }
      ]
    }
  end
  let(:results_grpc) { Google::Cloud::Spanner::V1::PartialResultSet.new results_hash }
  let(:results_enum) { Array(results_grpc).to_enum }

  after do
    shutdown_client! client
  end

  it "does not send header x-goog-spanner-route-to-leader when LAR is disabled" do
    columns = [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]
    mock = Minitest::Mock.new
    session.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = false
    mock.expect :create_session, session_grpc do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    mock.expect :streaming_read, results_enum do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    mock.expect :commit, commit_resp do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end

    client.transaction do |tx|
      tx.read "my-table", columns,
                          request_options: { tag: "Tag-1-1" }
    end

    mock.verify
  end

  it "sends header x-goog-spanner-route-to-leader when LAR is enabled" do
    columns = [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]
    mock = Minitest::Mock.new
    session.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = true
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :streaming_read, results_enum do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :commit, commit_resp do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end

    client.transaction do |tx|
      tx.read "my-table", columns,
                                  request_options: { tag: "Tag-1-1" }
    end
    mock.verify
  end
end
