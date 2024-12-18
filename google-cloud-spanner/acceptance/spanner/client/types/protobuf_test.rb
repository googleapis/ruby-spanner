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
require "data/protos/complex/user_pb"

describe "Spanner Client", :types, :protobuf, :spanner do
  let(:db) { spanner_client }
  let(:database) { spanner_client.database }
  let(:table_name) { "Users" }
  let(:column_name) { "User" }
  let(:admin) { $spanner_db_admin }
  let(:instance_id) { $spanner_instance_id }
  let(:database_id) { $spanner_database_id }
  let(:db_path) { admin.database_path project: spanner.project_id, instance: instance_id, database: database_id }
  let(:ddl_proto_statement) { <<~CREATE_PROTO
      CREATE PROTO BUNDLE (
        spanner.testing.data.User 
      )
    CREATE_PROTO
  }
  let (:ddl_table_statement) { <<~CREATE_TABLE
      CREATE TABLE Users (
        Id INT64 NOT NULL,
        #{column_name} `spanner.testing.data.User` NOT NULL, 
      )
    CREATE_TABLE
  }

  before do
    database.update statements: [ddl_proto_statement], 
      descriptor_set: "#{__dir__}/../../../data/protos/simple/user_descriptors.pb"
  end

  focus
  it "writes and reads custom PROTO types" do
    puts "HERE"
    address = Spanner::Testing::Data::User::Address.new(city: "Seattle", state: "WA")
    user = Spanner::Testing::Data::User.new(id: 1, name: "Charlie", active: false, address: address)
    db.upsert table_name, [user]
    results = db.read table_name, [column_name]

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ user: :PROTO })
    first_user = results.row.first
    _(first_user.name).must_equal "Charlie"
  end

  it "writes and queries custom PROTO types" do
    db.upsert table_name, [user]
    results = db.execute_sql "SELECT #{column_name} FROM #{table_name}"

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ user: :PROTO })
    first_user = results.rows.first
    _(first_user.name).must_equal "Charlie"
  end
end
