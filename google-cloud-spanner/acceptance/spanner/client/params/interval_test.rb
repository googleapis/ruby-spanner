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

describe "Spanner Client", :params, :interval, :spanner do
  let(:db) { spanner_client }

  it "queries and returns an :INTERVAL parameter" do
    skip if emulator_enabled?

    interval = Interval.fromIso8601 "P1Y2M3D"
    results = db.execute_query "SELECT @value as value", params: { value: interval }, types: { value: :INTERVAL }

    _(results).must_be_kind_of Google::Cloud::Spanner::Interval
    _(results.fields[:value]).must_equal :INTERVAL
    _(results.rows.first[:value]).must_equal interval
  end
end