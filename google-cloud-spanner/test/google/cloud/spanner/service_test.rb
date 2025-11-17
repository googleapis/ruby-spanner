# Copyright 2022 Google LLC
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

describe Google::Cloud::Spanner::Service, :mock_spanner  do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:default_options) { ::Gapic::CallOptions.new metadata: { "google-cloud-resource-prefix" => database_path(instance_id, database_id) } }
  let(:session_grpc) { Google::Cloud::Spanner::V1::Session.new name: session_path(instance_id, database_id, session_id) }
  let(:basic_service) { Google::Cloud::Spanner::Service.new "test_project", :this_channel_is_insecure }
  let(:expected_call_opts) {
    metadata = {
      "google-cloud-resource-prefix" => session_id
    }
    Gapic::CallOptions.new metadata: metadata
  }

  describe ".new" do
    it "sets quota_project with given value" do
      expected_quota_project = "test_quota_project"
      service = Google::Cloud::Spanner::Service.new(
        "test_project", :this_channel_is_insecure, quota_project: expected_quota_project
      )
      assert_equal expected_quota_project, service.quota_project
    end

    it "sets quota_project from credentials if not given from config" do 
      expected_quota_project = "test_quota_project"
      service = Google::Cloud::Spanner::Service.new(
        "test_project", OpenStruct.new(quota_project_id: expected_quota_project)
      )
      assert_equal expected_quota_project, service.quota_project
    end

    it "uses the default universe domain" do
      assert_equal "googleapis.com", basic_service.universe_domain
      assert_equal "spanner.googleapis.com", basic_service.host
    end

    it "sets a custom universe domain" do
      service = Google::Cloud::Spanner::Service.new "test_project", :this_channel_is_insecure, universe_domain: "myuniverse.com"
      assert_equal "myuniverse.com", service.universe_domain
      assert_equal "spanner.myuniverse.com", service.host
    end
  end

  describe ".create_session" do
    it "creates session with given database role" do
      mock = Minitest::Mock.new
      session = Google::Cloud::Spanner::V1::Session.new labels: nil, creator_role: "test_role"
      mock.expect :create_session, session_grpc, [{ database: database_path(instance_id, database_id), session: session }, default_options]
      service = Google::Cloud::Spanner::Service.new(
          "test_project", OpenStruct.new(client: OpenStruct.new(updater_proc: Proc.new{""}))
      )
      service.mocked_service = mock  
      service.create_session database_path(instance_id, database_id), database_role: "test_role"
      mock.verify
    end
  end

  describe ".batch_create_sessions" do
    it "batch creates session with given database role" do
      mock = Minitest::Mock.new
      session = Google::Cloud::Spanner::V1::Session.new labels: nil, creator_role: "test_role"
      mock.expect :batch_create_sessions, OpenStruct.new(session: Array.new(10) { session_grpc }), [{database: database_path(instance_id, database_id), session_count: 10, session_template: session }, default_options]
      service = Google::Cloud::Spanner::Service.new(
          "test_project", OpenStruct.new(client: OpenStruct.new(updater_proc: Proc.new{""}))
      )
      service.mocked_service = mock  
      service.batch_create_sessions database_path(instance_id, database_id), 10, database_role: "test_role"
      mock.verify
    end
  end

  describe "#begin_transaction" do
    it "sets the exclude_txn_from_change_streams field" do
      mocked_service = Minitest::Mock.new
      expected_request = {
        session: session_id,
        options: Google::Cloud::Spanner::V1::TransactionOptions.new(
          read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new,
          exclude_txn_from_change_streams: true
        ),
        request_options: nil,
        mutation_key: nil
      }
      expected_result = Object.new
      mocked_service.expect :begin_transaction, expected_result, [expected_request, expected_call_opts]
      basic_service.mocked_service = mocked_service
      result = basic_service.begin_transaction session_id, exclude_txn_from_change_streams: true
      mocked_service.verify
      assert_equal expected_result, result
    end
  end

  describe "#commit" do
    it "sets the exclude_txn_from_change_streams field" do
      mocked_service = Minitest::Mock.new
      expected_request = {
        session: session_id,
        transaction_id: nil,
        single_use_transaction: Google::Cloud::Spanner::V1::TransactionOptions.new(
          read_write: Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite.new,
          exclude_txn_from_change_streams: true
        ),
        mutations: [],
        request_options: nil,
        precommit_token: nil,
      }
      expected_result = Object.new
      mocked_service.expect :commit, expected_result, [expected_request, expected_call_opts]
      basic_service.mocked_service = mocked_service
      result = basic_service.commit session_id, [], exclude_txn_from_change_streams: true
      mocked_service.verify
      assert_equal expected_result, result
    end
  end

  describe "#create_pdml" do
    it "sets the exclude_txn_from_change_streams field" do
      mocked_service = Minitest::Mock.new
      expected_request = {
        session: session_id,
        options: Google::Cloud::Spanner::V1::TransactionOptions.new(
          partitioned_dml: Google::Cloud::Spanner::V1::TransactionOptions::PartitionedDml.new,
          exclude_txn_from_change_streams: true
        ),
        mutation_key: nil
      }
      expected_result = Object.new
      mocked_service.expect :begin_transaction, expected_result, [expected_request, expected_call_opts]
      basic_service.mocked_service = mocked_service
      result = basic_service.create_pdml session_id, exclude_txn_from_change_streams: true
      mocked_service.verify
      assert_equal expected_result, result
    end
  end
end
