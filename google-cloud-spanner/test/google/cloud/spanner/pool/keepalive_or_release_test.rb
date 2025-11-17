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
require "google/cloud/spanner/pool"

describe Google::Cloud::Spanner::Pool, :keepalive_or_release, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session1" }
  let(:session_id_2) { "session2" }
  let(:session_id_3) { "session3" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session_grpc_2) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id_2) }
  let(:session_grpc_3) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id_3) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:session2) { Google::Cloud::Spanner::Session.from_grpc session_grpc_2, spanner.service }
  let(:session3) { Google::Cloud::Spanner::Session.from_grpc session_grpc_3, spanner.service }
  let(:transaction_id) { "tx789" }
  let(:transaction_grpc) { Google::Cloud::Spanner::V1::Transaction.new id: transaction_id }
  let(:transaction) { Google::Cloud::Spanner::Transaction.from_grpc transaction_grpc, session }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:tx_opts) { Google::Cloud::Spanner::V1::TransactionOptions.new(read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new) }
  let(:session_creation_options) { ::Google::Cloud::Spanner::SessionCreationOptions.new database_path: database_path(instance_id, database_id)}
  let(:pool) do
    session.instance_variable_set :@last_updated_at, Time.now
    p = Google::Cloud::Spanner::Pool.new(spanner.service, session_creation_options, min: 0, max: 4)
    p.sessions_available = [session]
    p.sessions_in_use = {}
    p
  end
  let :results_hash do
    {
      metadata: {
        row_type: {
          fields: [
            { type: { code: :INT64 } }
          ]
        }
      },
      values: [ { string_value: "1" }]
    }
  end
  let(:results_grpc) { Google::Cloud::Spanner::V1::PartialResultSet.new results_hash }
  let(:results_enum) { Array(results_grpc).to_enum }

  before do
    # kill the background thread before starting the tests
    pool.instance_variable_get(:@keepalive_task).shutdown
  end

  it "calls keepalive on the sessions that need it" do
    # update the session so it was last updated an hour ago
    session.instance_variable_set :@last_updated_at, Process::clock_gettime(Process::CLOCK_MONOTONIC) - 60*60
    # set the session in the pool
    pool.sessions_available = [session]
    pool.sessions_in_use = {}
    pool.instance_variable_set :@min, 1

    mock = Minitest::Mock.new
    session.service.mocked_service = mock
    expect_execute_streaming_sql results_enum, session.path, "SELECT 1", options: default_options

    pool.keepalive_or_release!

    shutdown_pool! pool

    mock.verify
  end

  it "doesn't call keepalive on sessions that don't need it" do
    # update the session so it was last updated now
    session.instance_variable_set :@last_updated_at, Process::clock_gettime(Process::CLOCK_MONOTONIC)
    # set the session in the pool
    pool.sessions_available = [session]
    pool.sessions_in_use = {}
    pool.instance_variable_set :@min, 1

    mock = Minitest::Mock.new
    session.service.mocked_service = mock

    pool.keepalive_or_release!

    shutdown_pool! pool

    mock.verify
  end

  it "releases sessions above the min" do
    # Set up 3 sessions: session is recently used, but session2 and session3
    # are eligible for release/keepalive.
    # Because the min is 2, we expect one of [session2, session3] to be
    # released and the other to be pinged.
    pool.sessions_available = [session, session2, session3]
    pool.instance_variable_set :@thread_pool, Concurrent::ImmediateExecutor.new
    pool.instance_variable_set :@min, 2
    session.instance_variable_set :@last_updated_at, Process::clock_gettime(Process::CLOCK_MONOTONIC)
    session2.instance_variable_set :@last_updated_at, Process::clock_gettime(Process::CLOCK_MONOTONIC) - 2000
    session3.instance_variable_set :@last_updated_at, Process::clock_gettime(Process::CLOCK_MONOTONIC) - 2000

    keepalive_log = []
    release_log = []
    session2.stub(:keepalive!, proc { keepalive_log << session2 }) do
      session3.stub(:keepalive!, proc { keepalive_log << session3 }) do
        session2.stub(:release!, proc { release_log << session2 }) do
          session3.stub(:release!, proc { release_log << session3 }) do
            pool.keepalive_or_release!
          end
        end
      end
    end

    _(keepalive_log.count).must_equal 1
    _([session2, session3]).must_include(keepalive_log.first)
    _(release_log.count).must_equal 1
    _([session2, session3]).must_include(release_log.first)
    _(release_log.first).wont_equal keepalive_log.first
    _(pool.sessions_available.count).must_equal 2
    _(pool.sessions_available).must_include session
    _([session2, session3]).must_include((pool.sessions_available - [session]).first)

    shutdown_pool! pool
  end

  it "doesn't release sessions under the min" do
    # Set up 2 sessions: session is recently used, but session2 is stale and
    # eligible for keepalive. Min is 3 so it won't be released. If something
    # is released incorrectly this will error because the release will fail due
    # to no credentials.
    pool.sessions_available = [session, session2]
    pool.instance_variable_set :@thread_pool, Concurrent::ImmediateExecutor.new
    pool.instance_variable_set :@min, 3
    session.instance_variable_set :@last_updated_at, Process::clock_gettime(Process::CLOCK_MONOTONIC)
    session2.instance_variable_set :@last_updated_at, Process::clock_gettime(Process::CLOCK_MONOTONIC) - 2000

    keepalive_log = []
    session2.stub(:keepalive!, proc { keepalive_log << session2 }) do
      pool.keepalive_or_release!
    end

    _(keepalive_log.count).must_equal 1
    _(keepalive_log).must_include session2
    _(pool.sessions_available.count).must_equal 2
    _(pool.sessions_available).must_include session
    _(pool.sessions_available).must_include session2

    shutdown_pool! pool
  end
end
