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

describe Google::Cloud::Spanner::Client, :read, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:commit_time) { Time.now }
  let(:commit_timestamp) { Google::Cloud::Spanner::Convert.time_to_timestamp commit_time }
  let(:commit_resp) { Google::Cloud::Spanner::V1::CommitResponse.new commit_timestamp: commit_timestamp }
  let(:client) { spanner.client instance_id, database_id }

  after do
    shutdown_client! client
  end

  it "does not send header x-goog-spanner-route-to-leader when LAR is disabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    mock.expect :commit, commit_resp do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    spanner.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = false

    client.commit do |c|
      c.update "users", [{ id: 1, name: "Charlie", active: false }]
      c.insert "users", [{ id: 2, name: "Harvey",  active: true }]
      c.upsert "users", [{ id: 3, name: "Marley",  active: false }]
      c.replace "users", [{ id: 4, name: "Henry",  active: true }]
      c.delete "users", [1, 2, 3, 4, 5]
    end

    mock.verify
  end

  it "sends header x-goog-spanner-route-to-leader when LAR is enabled" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    mock.expect :commit, commit_resp do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    spanner.service.mocked_service = mock
    spanner.service.enable_leader_aware_routing = true

    client.commit do |c|
      c.update "users", [{ id: 1, name: "Charlie", active: false }]
      c.insert "users", [{ id: 2, name: "Harvey",  active: true }]
      c.upsert "users", [{ id: 3, name: "Marley",  active: false }]
      c.replace "users", [{ id: 4, name: "Henry",  active: true }]
      c.delete "users", [1, 2, 3, 4, 5]
    end

    mock.verify
  end
end
