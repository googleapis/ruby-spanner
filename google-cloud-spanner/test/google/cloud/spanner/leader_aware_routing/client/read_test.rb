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

describe Google::Cloud::Spanner::Client, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session1" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:transaction) { Google::Cloud::Spanner::Transaction.from_grpc nil, session }
  let(:transaction_id) { "tx789" }
  let(:client) { spanner.client instance_id, database_id, pool: { min: 0, max: 4 } }
  let :results_hash1 do
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
        }
      }
    }
  end
  let :results_hash2 do
    {
      values: [
        { string_value: "1" },
        { string_value: "Charlie" },
        { bool_value: true},
        { string_value: "29" },
        { number_value: 0.9 },
        { string_value: "2017-01-02T03:04:05.060000000Z" },
        { string_value: "1950-01-01" },
        { string_value: "aW1hZ2U=" }
      ]
    }
  end
  let :results_hash3 do
    {
      values: [
        { list_value: { values: [ { string_value: "1"},
                                 { string_value: "2"},
                                 { string_value: "3"} ]}}
      ]
    }
  end
  let(:results_enum) do
    [Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash1),
     Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash2),
     Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash3)].to_enum
  end

  after do
    shutdown_client! client
  end

  it "does not send header x-goog-spanner-route-to-leader when LAR is disabled" do
    columns = [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    mock.expect :streaming_read, results_enum do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    spanner.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = false

    results = client.read "my-table", columns
    shutdown_client! client

    mock.verify
  end

  
 it "sends header x-goog-spanner-route-to-leader when LAR is enabled" do
    columns = [:id, :name, :active, :age, :score, :updated_at, :birthday, :avatar, :project_ids]
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :streaming_read, results_enum do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'false'
    end
    spanner.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = true

    results = client.read "my-table", columns
    shutdown_client! client

    mock.verify
  end
end
