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

describe "Spanner Client Queues", :crud, :spanner do
  before do
    skip "Cloud Spanner Emulator does not yet support Queues (Send & Ack)" if ENV["SPANNER_EMULATOR_HOST"]
  end

  let :db do
    { gsql: spanner_client }
  end

  it "sends, reads back, and acknowledges queue messages transactionally" do
    db[:gsql].commit do |c|
      c.send_message "TestQueue", [1], "payload1"
      c.send_message "TestQueue", [2], "payload2"
      c.send_message "TestQueue", [3], "payload3", deliver_time: Time.now
    end

    # Read back from the queue table to verify messages were enqueued
    results1 = db[:gsql].read "TestQueue", [:Payload], keys: [1]
    rows1 = results1.rows.to_a
    _(rows1.count).must_equal 1
    payload1 = rows1.first[:Payload]
    _(payload1.respond_to?(:read) ? payload1.read : payload1.to_s).must_equal "payload1"

    results2 = db[:gsql].read "TestQueue", [:Payload], keys: [2]
    rows2 = results2.rows.to_a
    _(rows2.count).must_equal 1
    payload2 = rows2.first[:Payload]
    _(payload2.respond_to?(:read) ? payload2.read : payload2.to_s).must_equal "payload2"

    results3 = db[:gsql].read "TestQueue", [:Payload], keys: [3]
    rows3 = results3.rows.to_a
    _(rows3.count).must_equal 1
    payload3 = rows3.first[:Payload]
    _(payload3.respond_to?(:read) ? payload3.read : payload3.to_s).must_equal "payload3"

    # Acknowledge the first two messages
    db[:gsql].commit do |c|
      c.ack_message "TestQueue", [1]
      c.ack_message "TestQueue", [2]
    end

    # Verify the first two messages are removed from the queue and message 3 remains
    results1_after = db[:gsql].read "TestQueue", [:Payload], keys: [1]
    _(results1_after.rows.to_a.count).must_equal 0

    results2_after = db[:gsql].read "TestQueue", [:Payload], keys: [2]
    _(results2_after.rows.to_a.count).must_equal 0

    results3_after = db[:gsql].read "TestQueue", [:Payload], keys: [3]
    rows3_after = results3_after.rows.to_a
    _(rows3_after.count).must_equal 1
    payload3_after = rows3_after.first[:Payload]
    _(payload3_after.respond_to?(:read) ? payload3_after.read : payload3_after.to_s).must_equal "payload3"
  end

  it "acknowledges missing messages with ignore_not_found option" do
    db[:gsql].commit do |c|
      c.ack_message "TestQueue", [9999], ignore_not_found: true
    end

    # Acknowledging without ignore_not_found should raise an error when queue is present
    err = _(proc do
      db[:gsql].commit do |c|
        c.ack_message "TestQueue", [9999], ignore_not_found: false
      end
    end).must_raise Google::Cloud::NotFoundError, Google::Cloud::InvalidArgumentError, Google::Cloud::UnimplementedError
    _(err.message).must_match(/Queue|Table not found|not found|TestQueue|unimplemented|invalid/i)
  end
end
