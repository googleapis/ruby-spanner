# Copyright 2023 Google LLC
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

describe "Spanner Client", :types, :oid, :spanner do
  let(:db) { spanner_pg_client }
  let(:table_name) { "stuffs" }

  it "queries pg.oid" do
    skip if emulator_enabled?
    results = db.execute_sql "select 123::oid as value"

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ value: :INT64 })
    _(results.rows.first.to_h).must_equal({ value: 123 })
  end
end
