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

require "spanner_helper"
require "google/cloud/spanner/interval"

describe "Spanner Client", :types, :interval, :spanner do
  let(:db) { spanner_client }
  let(:table_name) { "stuffs" }

  it "converts a valid ISO8601 string to an :INTERVAL" do
    skip if emulator_enabled?

    results = db.execute_query "SELECT @value AS value", params: { value: "P14M" }, types: { value: :INTERVAL }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :INTERVAL
    interval = results.rows.first[:value]
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.to_s).must_equal "P1Y2M"
  end
end
