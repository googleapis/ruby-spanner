# Copyright 2026 Google LLC
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

require "spanner_helper"

# An ack without ignore_not_found fails with NOT_FOUND unless the message
# exists, so a successful ack also shows that the send landed.
describe "Spanner Client Queues", :queues, :spanner do
  let(:db) { spanner_client }

  before do
    skip "Cloud Spanner Emulator does not yet support Queues (Send & Ack)" if emulator_enabled?

    # Created once per run, in the per-run test database.
    $spanner_test_queue_created ||= begin
      db_job = db.database.update statements: [queue_ddl_statement]
      db_job.wait_until_done!
      raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?
      true
    end
  end

  it "sends and acks messages with Client#commit" do
    commit_time = db.commit do |c|
      c.send_message "TestQueue", 1, StringIO.new("payload1")
      c.send_message "TestQueue", 2, StringIO.new("payload2")
    end
    _(commit_time).must_be_kind_of Time

    commit_time = db.commit do |c|
      c.ack_message "TestQueue", 1
      c.ack_message "TestQueue", 2
    end
    _(commit_time).must_be_kind_of Time
  end

  it "sends and acks a message with Client#transaction" do
    commit_time = db.transaction do |tx|
      tx.send_message "TestQueue", 3, StringIO.new("payload3")
    end
    _(commit_time).must_be_kind_of Time

    commit_time = db.transaction do |tx|
      tx.ack_message "TestQueue", 3
    end
    _(commit_time).must_be_kind_of Time
  end
end
