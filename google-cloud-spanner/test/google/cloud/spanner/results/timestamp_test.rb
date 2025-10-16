# Copyright 2017 Google LLC
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

describe Google::Cloud::Spanner::Results, :timestamp, :mock_spanner do
  let(:time_obj) { Time.parse "2014-10-02T15:01:23.045123456Z" }
  let(:timestamp) { Google::Cloud::Spanner::Convert.time_to_timestamp time_obj }
  let :results_types do
    {
      metadata: {
        row_type: {
          fields: [
            { type: { code: :INT64 } },
            { type: { code: :INT64 } },
            { type: { code: :INT64 } },
            { type: { code: :INT64 } }
          ]
        },
        transaction: {
          read_timestamp: timestamp
        }
      }
    }
  end
  let(:results_enum) do
    [Google::Cloud::Spanner::V1::PartialResultSet.new(results_types)].to_enum
  end
  let(:results) { Google::Cloud::Spanner::Results.from_partial_result_sets results_enum, spanner.service, default_session_request&.name }

  it "knows it has a timestamp" do
    _(results).must_be_kind_of Google::Cloud::Spanner::Results

    _(results.timestamp).must_equal time_obj
  end
end
