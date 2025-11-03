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

describe Google::Cloud::Spanner::Pool, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session1" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:transaction) { Google::Cloud::Spanner::Transaction.from_grpc nil, session }
  let(:transaction_id) { "tx789" }
  let(:client) { spanner.client instance_id, database_id, pool: { min: 0, max: 4 } }
  let :results_hash do
    {
      metadata: {
        row_type: {
          fields: [
            { type: { code: :INT64 } }
          ]
        }
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
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end    
    mock.expect :execute_streaming_sql, results_enum do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    spanner.service.mocked_service = mock

    spanner.service.enable_leader_aware_routing = false
    results = client.execute_query "SELECT 1"

    mock.verify
  end

  it "sends header x-goog-spanner-route-to-leader with true for queries without single_use when LAR is enabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end    
    mock.expect :execute_streaming_sql, results_enum do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    spanner.service.mocked_service = mock

    spanner.service.enable_leader_aware_routing = true
    results = client.execute_query "SELECT 1"

    mock.verify
  end

  it "sends header x-goog-spanner-route-to-leader with false for queries with single_use strong option when LAR is enabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :execute_streaming_sql, results_enum do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'false'
    end
    spanner.service.mocked_service = mock

    spanner.service.enable_leader_aware_routing = true
    results = client.execute_query "SELECT 1", single_use: { strong: true }

    mock.verify
  end

  it "sends header x-goog-spanner-route-to-leader with false for queries with single_use staleness option when LAR is enabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :execute_streaming_sql, results_enum do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'false'
    end
    spanner.service.mocked_service = mock

    spanner.service.enable_leader_aware_routing = true
    results = client.execute_query "SELECT 1", single_use: { staleness: 60 }

    mock.verify
  end

  it "sends header x-goog-spanner-route-to-leader with false for queries with single_use timestamp option when LAR is enabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :execute_streaming_sql, results_enum do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'false'
    end
    spanner.service.mocked_service = mock

    spanner.service.enable_leader_aware_routing = true
    timestamp = Time.now - 60
    results = client.execute_query "SELECT 1", single_use: { timestamp: timestamp }

    mock.verify
  end
end
