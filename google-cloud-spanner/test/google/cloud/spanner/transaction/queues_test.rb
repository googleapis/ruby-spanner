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

require "helper"

# Transaction#send_message and #ack_message delegate to the transaction's
# Commit, so payload and key conversion is covered in commit_queues_test.rb.
# These tests check that every argument reaches the transaction's mutations.
describe Google::Cloud::Spanner::Transaction, :queues, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:session) { Google::Cloud::Spanner::Session.from_grpc session_grpc, spanner.service }
  let(:transaction_id) { "tx789" }
  let(:transaction_grpc) { Google::Cloud::Spanner::V1::Transaction.new id: transaction_id }
  let(:transaction) { Google::Cloud::Spanner::Transaction.from_grpc transaction_grpc, session }
  let(:deliver_time) { Time.now + 300 }

  it "adds a send mutation to the transaction" do
    key = transaction.send_message "TestQueue", ["tenant1", 2], "Hello, Queues!"

    assert_equal ["tenant1", 2], key
    assert_equal 1, transaction.mutations.count
    mutation = transaction.mutations.first

    assert_equal "TestQueue", mutation["send"].queue
    assert_equal Google::Cloud::Spanner::Convert.object_to_grpc_value(["tenant1", 2]).list_value, mutation["send"].key
    assert_equal Google::Cloud::Spanner::Convert.object_to_grpc_value("Hello, Queues!"), mutation["send"].payload
    assert_nil mutation["send"].deliver_time
  end

  it "passes deliver_time to the send mutation" do
    transaction.send_message "TestQueue", 1, "payload", deliver_time: deliver_time

    expected = Google::Cloud::Spanner::Convert.time_to_timestamp deliver_time
    assert_equal expected, transaction.mutations.first["send"].deliver_time
  end

  it "adds an ack mutation to the transaction" do
    key = transaction.ack_message "TestQueue", ["tenant1", 2]

    assert_equal ["tenant1", 2], key
    assert_equal 1, transaction.mutations.count
    mutation = transaction.mutations.first

    assert_equal "TestQueue", mutation.ack.queue
    assert_equal Google::Cloud::Spanner::Convert.object_to_grpc_value(["tenant1", 2]).list_value, mutation.ack.key
    assert_equal false, mutation.ack.ignore_not_found
  end

  it "passes ignore_not_found to the ack mutation" do
    transaction.ack_message "TestQueue", 1, ignore_not_found: true

    assert_equal true, transaction.mutations.first.ack.ignore_not_found
  end

  it "raises when session is missing" do
    transaction.session = nil

    assert_raises RuntimeError do
      transaction.send_message "TestQueue", 1, "test"
    end

    assert_raises RuntimeError do
      transaction.ack_message "TestQueue", 1
    end
  end
end
