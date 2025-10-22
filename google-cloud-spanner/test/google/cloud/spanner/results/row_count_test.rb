# Copyright 2018 Google LLC
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

describe Google::Cloud::Spanner::Results, :row_count, :mock_spanner do
  it "knows exact row count" do
    results_enum = [
      Google::Cloud::Spanner::V1::PartialResultSet.new(
        metadata: Google::Cloud::Spanner::V1::ResultSetMetadata.new(
          row_type: Google::Cloud::Spanner::V1::StructType.new(
            fields: []
          )
        ),
        values: [],
        stats: Google::Cloud::Spanner::V1::ResultSetStats.new(
          row_count_exact: 1
        )
      )
    ].to_enum

    results = Google::Cloud::Spanner::Results.from_partial_result_sets results_enum, spanner.service, default_session_request&.name
    results.rows.to_a # force all results to be processed

    _(results).must_be :row_count_exact?
    _(results).wont_be :row_count_lower_bound?
    _(results.row_count).must_equal 1
  end

  it "knows lower bound row count" do
    results_enum = [
      Google::Cloud::Spanner::V1::PartialResultSet.new(
        metadata: Google::Cloud::Spanner::V1::ResultSetMetadata.new(
          row_type: Google::Cloud::Spanner::V1::StructType.new(
            fields: []
          )
        ),
        values: [],
        stats: Google::Cloud::Spanner::V1::ResultSetStats.new(
          row_count_lower_bound: 42
        )
      )
    ].to_enum

    results = Google::Cloud::Spanner::Results.from_partial_result_sets results_enum, spanner.service, default_session_request&.name
    results.rows.to_a # force all results to be processed

    _(results).must_be :row_count_lower_bound?
    _(results).wont_be :row_count_exact?
    _(results.row_count).must_equal 42
  end

  it "does not present row_count if none is provided" do
    results_enum = [
      Google::Cloud::Spanner::V1::PartialResultSet.new(
        metadata: Google::Cloud::Spanner::V1::ResultSetMetadata.new(
          row_type: Google::Cloud::Spanner::V1::StructType.new(
            fields: []
          )
        ),
        values: []
      )
    ].to_enum

    results = Google::Cloud::Spanner::Results.from_partial_result_sets results_enum, spanner.service, default_session_request&.name
    results.rows.to_a # force all results to be processed

    _(results).wont_be :row_count_exact?
    _(results).wont_be :row_count_lower_bound?
    _(results.row_count).must_be :nil?
  end
end
