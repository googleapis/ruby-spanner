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

describe Google::Cloud::Spanner::Database, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:database_grpc) do
    Google::Cloud::Spanner::Admin::Database::V1::Database.new \
      database_hash(instance_id: instance_id, database_id: database_id)
  end
  let(:database) { Google::Cloud::Spanner::Database.from_grpc database_grpc, spanner.service }

  def sessions_hash count: 3, instance_id: "my-instance-id", database_id: "my-database-id"
    sessions = count.times.map do |i|
      { name: session_path(instance_id, database_id, "session-#{i}") }
    end
    { sessions: sessions }
  end

  let(:first_page) do
    h = sessions_hash instance_id: instance_id, database_id: database_id
    h[:next_page_token] = "next_page_token"
    Google::Cloud::Spanner::V1::ListSessionsResponse.new h
  end
  let(:last_page) do
    h = sessions_hash instance_id: instance_id, database_id: database_id
    h[:sessions].pop
    Google::Cloud::Spanner::V1::ListSessionsResponse.new h
  end

  it "lists sessions" do
    get_sessions_resp = MockPagedEnumerable.new(
      [first_page]
    )
    mock = Minitest::Mock.new
    mock.expect :list_sessions, get_sessions_resp, [{ database: database_path(instance_id, database_id), page_size: nil, page_token: nil }, ::Gapic::CallOptions]
    database.service.mocked_service = mock

    sessions = database.sessions

    mock.verify

    _(sessions.count).must_equal 3
    sessions.each do |session|
      _(session).must_be_kind_of Google::Cloud::Spanner::Session
    end
  end

  it "paginates sessions" do
    get_sessions_resp = MockPagedEnumerable.new(
      [first_page, last_page]
    )
    mock = Minitest::Mock.new
    mock.expect :list_sessions, get_sessions_resp, [{ database: database_path(instance_id, database_id), page_size: nil, page_token: nil }, ::Gapic::CallOptions]
    database.service.mocked_service = mock

    sessions = database.sessions

    mock.verify

    _(sessions.count).must_equal 3
    sessions.each do |session|
      _(session).must_be_kind_of Google::Cloud::Spanner::Session
    end
    _(sessions.next?).must_equal true
  end

  it "paginates sessions with page size" do
    get_sessions_resp = MockPagedEnumerable.new(
      [first_page, last_page]
    )
    mock = Minitest::Mock.new
    mock.expect :list_sessions, get_sessions_resp, [{ database: database_path(instance_id, database_id), page_size: 3, page_token: nil }, ::Gapic::CallOptions]
    database.service.mocked_service = mock

    sessions = database.sessions page_size: 3

    mock.verify

    _(sessions.count).must_equal 3
    sessions.each do |session|
      _(session).must_be_kind_of Google::Cloud::Spanner::Session
    end
    _(sessions.next?).must_equal true
  end
end
