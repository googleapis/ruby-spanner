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
require "data/protos/user_pb"

describe "Spanner Client", :types, :protobuf, :spanner do

  let(:client) { spanner_client }
  let(:table_name) { "proto_users" }
  let(:column_name) { "user" }

  it "writes and reads custom PROTO types" do
    user = Testing::Data::User.new id: 1, name: "Charlie", active: false
    client.upsert table_name, [{ userid: 1, user: user }]
    results = client.read table_name, [column_name]

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ user: :PROTO })
    first_user = results.rows.first[:user]
    _(first_user.name).must_equal "Charlie"
  end

  it "writes and queries custom PROTO types" do
    user = Testing::Data::User.new id: 2, name: "Harvey", active: false
    client.upsert table_name, [{ userid: 2, user: user }]
    results = client.execute_sql "SELECT #{column_name} FROM #{table_name} WHERE userid = @id", params: { id: 2 }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ user: :PROTO })
    first_user = results.rows.first[:user]
    _(first_user.name).must_equal "Harvey"
  end
end
