# Copyright 2024 Google LLC
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
require "#{__dir__}/../data/protos/complex/user_pb" 

describe "Spanner Client", :spanner do
  let(:client) { spanner_client }
  let(:database) { spanner_client.database }
  let(:descriptor_path_simple) { "#{__dir__}/../data/protos/simple/user_descriptors.pb" }
  let(:descriptor_path_complex) { "#{__dir__}/../data/protos/complex/user_descriptors.pb" }
  let(:table_name) { "User" }
  let(:column_name) { "user" }
  let :create_proto do
    <<~CREATE_PROTO
      CREATE PROTO BUNDLE (
        testing.data.User,
        testing.data.User.Address
      )
    CREATE_PROTO
  end
  let :delete_proto do
    <<~DELETE_PROTO
      ALTER PROTO BUNDLE DELETE (
        testing.data.User,
        testing.data.User.Address
      )
    DELETE_PROTO
  end
  let :create_table do
    <<~CREATE_TABLE
      CREATE TABLE #{table_name} (
        userid INT64 NOT NULL,
        user testing.data.User NOT NULL
      ) PRIMARY KEY (userid)
    CREATE_TABLE
  end
  let(:drop_table) { "DROP TABLE #{table_name}" }

  after do
    db_job = database.update statements: [drop_table]
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?
  end

  it "creates a table using `CREATE PROTO BUNDLE` proto schema" do
    db_job = database.update statements: [create_proto, create_table], descriptor_set: descriptor_path_complex
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?

    db_job = database.update statements: [delete_proto], descriptor_set: descriptor_path_complex
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?
  end

  it "updates a table DDL using `ALTER PROTO BUNDLE UPDATE`" do
    db_job = database.update statements: [create_proto, create_table], descriptor_set: descriptor_path_complex
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?

    address = Testing::Data::User::Address.new city: "Seattle", state: "WA"
    user = Testing::Data::User.new id: 1, name: "Charlie", active: false, address: address
    client.upsert table_name, [{userid: 1, user: user}]

    update_proto =
      <<~UPDATE_PROTO
        ALTER PROTO BUNDLE UPDATE (
          testing.data.User
        )
      UPDATE_PROTO
    db_job = database.update statements: [update_proto], descriptor_set: descriptor_path_simple
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?

    results = client.read table_name, [column_name]

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    updated_user = results.rows.first[:user]
    _(updated_user.name).must_equal "Charlie"
    puts updated_user
    _(updated_user.address).must_be :nil?

    db_job = database.update statements: [delete_proto], descriptor_set: descriptor_path_simple
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?
  end
end
