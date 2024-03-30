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

describe "Spanner Client", :params, :bool, :spanner do
  let(:db) { spanner_pg_client }

  it "queries and returns a PG.OID parameter" do
    skip if emulator_enabled?
    results = db.execute_query "SELECT $1 AS value", params: { p1: 123 }, types: { p1: :PG_OID }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :INT64
    _(results.rows.first[:value]).must_equal 123
  end
end
