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

describe Google::Cloud::Spanner::Pool, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session1" }
  let(:session_id_2) { "session2" }
  let(:session_id_3) { "session3" }
  let(:session_id_4) { "session4" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session_grpc_2) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id_2) }
  let(:session_grpc_3) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id_3) }
  let(:session_grpc_4) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id_4) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:session_creation_options) { ::Google::Cloud::Spanner::SessionCreationOptions.new database_path: database_path(instance_id, database_id)}
  let(:pool) do
    session.instance_variable_set :@last_updated_at, Time.now
    p = Google::Cloud::Spanner::Pool.new(spanner.service, session_creation_options, min: 0, max: 4)
    p.sessions_available = [session]
    p.sessions_in_use = {}
    p
  end

  it "can checkout and checkin a session" do
    _(pool.sessions_available.size).must_equal 1
    _(pool.sessions_in_use.size).must_equal 0

    s = pool.checkout_session

    _(pool.sessions_available.size).must_equal 0
    _(pool.sessions_in_use.size).must_equal 1

    pool.checkin_session s

    shutdown_pool! pool

    _(pool.sessions_available.size).must_equal 1
    _(pool.sessions_in_use.size).must_equal 0
  end

  it "creates new sessions when needed" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc_2, [{ database: database_path(instance_id, database_id), session: nil }, default_options]
    spanner.service.mocked_service = mock

    _(pool.sessions_available.size).must_equal 1
    _(pool.sessions_in_use.size).must_equal 0

    s1 = pool.checkout_session
    s2 = pool.checkout_session

    _(pool.sessions_available.size).must_equal 0
    _(pool.sessions_in_use.size).must_equal 2

    pool.checkin_session s1
    pool.checkin_session s2

    shutdown_pool! pool

    _(pool.sessions_available.size).must_equal 2
    _(pool.sessions_in_use.size).must_equal 0

    mock.verify
  end

  it "raises when checking out more than MAX sessions" do
    mock = Minitest::Mock.new
    mock.expect :create_session, session_grpc_2, [{ database: database_path(instance_id, database_id), session: nil }, default_options]
    mock.expect :create_session, session_grpc_3, [{ database: database_path(instance_id, database_id), session: nil }, default_options]
    mock.expect :create_session, session_grpc_4, [{ database: database_path(instance_id, database_id), session: nil }, default_options]
    spanner.service.mocked_service = mock

    _(pool.sessions_available.size).must_equal 1
    _(pool.sessions_in_use.size).must_equal 0

    s1 = pool.checkout_session
    s2 = pool.checkout_session
    s3 = pool.checkout_session
    s4 = pool.checkout_session

    assert_raises Google::Cloud::Spanner::SessionLimitError do
      pool.checkout_session
    end

    _(pool.sessions_available.size).must_equal 0
    _(pool.sessions_in_use.size).must_equal 4

    pool.checkin_session s1
    pool.checkin_session s2
    pool.checkin_session s3
    pool.checkin_session s4

    shutdown_pool! pool

    _(pool.sessions_available.size).must_equal 4
    _(pool.sessions_in_use.size).must_equal 0

    mock.verify
  end

  it "raises when checking in a session that does not belong" do
    outside_session = Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service

    checkin_error = assert_raises ArgumentError do
      pool.checkin_session outside_session
    end
    _(checkin_error.message).must_equal "Cannot checkin session"
  end
end
