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

describe Google::Cloud::Spanner::Results, :duplicate, :mock_spanner do
  let :results_types do
    {
      metadata: {
        row_type: {
          fields: [
            { name: "num", type: { code: :INT64 } },
            { name: "str", type: { code: :INT64 } },
            { name: "num", type: { code: :STRING } },
            { name: "str", type: { code: :STRING } }
          ]
        }
      }
    }
  end
  let :results_values do
    {
      values: [
        { string_value: "1" },
        { string_value: "2" },
        { string_value: "hello" },
        { string_value: "world" },
        { string_value: "3" },
        { string_value: "4" },
        { string_value: "hola" },
        { string_value: "mundo" }
      ]
    }
  end
  let(:results_enum) do
    [Google::Cloud::Spanner::V1::PartialResultSet.new(results_types),
     Google::Cloud::Spanner::V1::PartialResultSet.new(results_values)].to_enum
  end
  let(:results) { Google::Cloud::Spanner::Results.from_partial_result_sets results_enum, spanner.service, default_session_request&.name }

  it "handles duplicate names" do
    _(results).must_be_kind_of Google::Cloud::Spanner::Results

    fields = results.fields
    _(fields).wont_be :nil?
    _(fields).must_be_kind_of Google::Cloud::Spanner::Fields
    _(fields.types).must_equal [:INT64, :INT64, :STRING, :STRING]
    _(fields.keys).must_equal [:num, :str, :num, :str]
    _(fields.pairs).must_equal [[:num, :INT64], [:str, :INT64], [:num, :STRING], [:str, :STRING]]
    _(fields.to_a).must_equal [:INT64, :INT64, :STRING, :STRING]
    assert_raises Google::Cloud::Spanner::DuplicateNameError do
      fields.to_h
    end

    rows = results.rows.to_a # grab them all from the enumerator
    _(rows.count).must_equal 2
    _(rows.first.to_a).must_equal [1, 2, "hello", "world"]
    _(rows.last.to_a).must_equal [3, 4, "hola", "mundo"]
    assert_raises Google::Cloud::Spanner::DuplicateNameError do
      rows.first.to_h
    end
    rows.first.to_h skip_dup_check: true # does not raise
    assert_raises Google::Cloud::Spanner::DuplicateNameError do
      rows.last.to_h
    end
    rows.last.to_h skip_dup_check: true # does not raise
    _(rows.first.pairs).must_equal [[:num, 1], [:str, 2], [:num, "hello"], [:str, "world"]]
    _(rows.last.pairs).must_equal [[:num, 3], [:str, 4], [:num, "hola"], [:str, "mundo"]]
  end
end
