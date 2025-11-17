# Copyright 2025 Google LLC
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

describe Google::Cloud::Spanner::SessionCache, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session1" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id), multiplexed: true }
  let(:new_session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, "new-session"), multiplexed: true }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:session_creation_options) { ::Google::Cloud::Spanner::SessionCreationOptions.new database_path: database_path(instance_id, database_id) }

  it "creates a new session with with_session if one does not exist" do
    mock = Minitest::Mock.new
    mock.expect :create_session, new_session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request}, default_options]
    spanner.service.mocked_service = mock

    cache = Google::Cloud::Spanner::SessionCache.new spanner.service, session_creation_options
    _(cache.instance_variable_get :@session).must_be_nil

    cache.with_session do |yielded_session|
      _(yielded_session.session_id).must_equal "new-session"
    end

    mock.verify
  end

  it "creates a new session with #session if one does not exist" do
    mock = Minitest::Mock.new
    mock.expect :create_session, new_session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request}, default_options]
    spanner.service.mocked_service = mock
    
    cache = Google::Cloud::Spanner::SessionCache.new spanner.service, session_creation_options
    _(cache.instance_variable_get :@session).must_be_nil
    
    new_session = cache.session

    mock.verify
    _(new_session.session_id).must_equal "new-session"
  end

  it "uses the existing session with with_session if it is not old" do
    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock # No calls expected

    cache = Google::Cloud::Spanner::SessionCache.new spanner.service, session_creation_options
    cache.instance_variable_set :@session, session

    cache.with_session do |yielded_session|
     _(yielded_session.session_id).must_equal session_id
    end

    mock.verify
  end

  it "creates a new session with with_session if the existing one is old" do
    mock = Minitest::Mock.new
    mock.expect :create_session, new_session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request }, default_options]
    spanner.service.mocked_service = mock

    # Make session seem old
    old_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - Google::Cloud::Spanner::SessionCache::SESSION_REFRESH_SEC - 1
    session.instance_variable_set :@created_time, old_time
    
    cache = Google::Cloud::Spanner::SessionCache.new spanner.service, session_creation_options
    cache.instance_variable_set :@session, session

    yielded_session = nil
    cache.with_session do |yielded_session|
     _(yielded_session.session_id).must_equal "new-session"
    end

    mock.verify
  end

  it "resets the session" do
    mock = Minitest::Mock.new
    mock.expect :create_session, new_session_grpc, [{ database: database_path(instance_id, database_id), session: default_session_request}, default_options]
    spanner.service.mocked_service = mock

    cache = Google::Cloud::Spanner::SessionCache.new spanner.service, session_creation_options
    cache.instance_variable_set :@session, session
    _(cache.reset!).must_equal true
    cache.with_session do |yielded_session|
     _(yielded_session.session_id).must_equal "new-session"
    end

    mock.verify
  end

  it "close is a no-op" do
    mock = Minitest::Mock.new
    spanner.service.mocked_service = mock

    cache = Google::Cloud::Spanner::SessionCache.new spanner.service, session_creation_options
    _(cache.close).must_equal true
    mock.verify
  end
end
