# Copyright 2016 Google LLC
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

describe Google::Cloud::Spanner::Project, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) {
    Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id)
  }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:batch_create_sessions_grpc) {
    Google::Cloud::Spanner::V1::BatchCreateSessionsResponse.new session: [session_grpc]
  }

  it "knows the project identifier" do
    _(spanner).must_be_kind_of Google::Cloud::Spanner::Project
    _(spanner.project_id).must_equal project
  end

  it "returns the default universe domain" do
    _(spanner.universe_domain).must_equal "googleapis.com"
  end

  it "returns a custom universe domain" do
    universe = "myuniverse.com"
    service = Google::Cloud::Spanner::Service.new project, credentials, universe_domain: universe
    spanner = Google::Cloud::Spanner::Project.new service
    _(spanner.universe_domain).must_equal universe
  end

  it "creates client with multiplexed pool by default" do
    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock

    client = spanner.client instance_id, database_id
    _(client.instance_variable_get :@pool).must_be_kind_of ::Google::Cloud::Spanner::SessionCache
  end

  it "creates client with database role" do
    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock

    client = spanner.client instance_id, database_id, database_role: "test-role"
    _(client.database_role).must_equal "test-role"
  end
end
