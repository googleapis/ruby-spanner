# Copyright 2016 Google LLC
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

describe Google::Cloud::Spanner::Results, :anonymous_struct, :mock_spanner do
  let :results_hash do
    {metadata:
      {row_type:
        {fields:
          [{type:
             {code: :ARRAY,
              array_element_type:
               {code: :STRUCT,
                struct_type:
                 {fields:
                   [{type:{code: :INT64}},
                    {type:{code: :INT64}}]}}}}]}},
     values:
      [{list_value:
         {values:
           [{list_value:
              {values:[{string_value: "1"}, {string_value: "2"}]}}]}}]}
  end
  let(:results_enum) do
    [Google::Cloud::Spanner::V1::PartialResultSet.new(results_hash)].to_enum
  end
  let(:results) { Google::Cloud::Spanner::Results.from_partial_result_sets results_enum, spanner.service, default_session_request&.name }

  it "handles anonymous structs" do
    _(results).must_be_kind_of Google::Cloud::Spanner::Results

    _(results.fields).wont_be :nil?
    _(results.fields).must_be_kind_of Google::Cloud::Spanner::Fields
    _(results.fields.keys).must_equal [0]
    _(results.fields.pairs).must_equal [[0, [Google::Cloud::Spanner::Fields.new([:INT64, :INT64])]]]
    _(results.fields.to_a).must_equal [[Google::Cloud::Spanner::Fields.new([:INT64, :INT64])]]
    _(results.fields.to_h).must_equal({ 0 => [Google::Cloud::Spanner::Fields.new([:INT64, :INT64])] })

    rows = results.rows.to_a # grab them all from the enumerator
    _(rows.count).must_equal 1
    row = rows.first
    _(row).must_be_kind_of Google::Cloud::Spanner::Data
    _(row.keys).must_equal [0]
    _(row.values).must_equal [[Google::Cloud::Spanner::Fields.new([:INT64, :INT64]).new([1, 2])]]
    _(row.pairs).must_equal [[0, [Google::Cloud::Spanner::Fields.new([:INT64, :INT64]).new([1, 2])]]]
    _(row.to_a).must_equal [[{ 0 => 1, 1 => 2 }]]
    _(row.to_h).must_equal({ 0 => [{ 0 => 1, 1 => 2 }] })
  end
end
