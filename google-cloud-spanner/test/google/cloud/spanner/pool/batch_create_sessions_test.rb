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

require "helper"

describe Google::Cloud::Spanner::Pool, :batch_create_sessions, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:client) { spanner.client instance_id, database_id, pool: { min: 0, max: 4 } }
  let(:tx_opts) { Google::Cloud::Spanner::V1::TransactionOptions.new(read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new) }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:client_pool) do
    session.instance_variable_set :@last_updated_at, Time.now
    p = client.instance_variable_get :@pool
    p.sessions_available = [session]
    p.sessions_in_use = []
    p
  end

  after do
    shutdown_client! client
  end

  it "calls batch_create_sessions until min number of sessions are returned" do
    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock
    sessions_1 = Google::Cloud::Spanner::V1::BatchCreateSessionsResponse.new(
      session: [
        Google::Cloud::Spanner::V1::Session.new(name: session_path(instance_id, database_id, "session-001")),
      ]
    )
    sessions_2 = Google::Cloud::Spanner::V1::BatchCreateSessionsResponse.new(
      session: [
        Google::Cloud::Spanner::V1::Session.new(name: session_path(instance_id, database_id, "session-002")),
      ]
    )
    mock.expect :batch_create_sessions, sessions_1, [{ database: database_path(instance_id, database_id), session_count: 2, session_template: nil }, default_options]
    mock.expect :batch_create_sessions, sessions_2, [{ database: database_path(instance_id, database_id), session_count: 1, session_template: nil }, default_options]

    pool = Google::Cloud::Spanner::Pool.new client, min: 2

    shutdown_pool! pool

    _(pool.sessions_available.size).must_equal 2
    _(pool.sessions_in_use.size).must_equal 0

    mock.verify
  end
end
