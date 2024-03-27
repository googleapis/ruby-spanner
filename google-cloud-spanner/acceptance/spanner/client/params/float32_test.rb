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

describe "Spanner Client", :params, :float32, :spanner do
  let(:db) { spanner_client }

  it "queries and returns a float32 parameter" do
    results = db.execute_query "SELECT @value AS value", params: { value: 12.0 }, types: { value: :FLOAT32 }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :FLOAT32
    _(results.rows.first[:value]).must_equal 12.0
  end

  it "queries and returns a float32 parameter (Infinity)" do
    results = db.execute_query "SELECT @value AS value", params: { value: Float::INFINITY }, types: { value: :FLOAT32 }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :FLOAT32
    _(results.rows.first[:value]).must_equal Float::INFINITY
  end

  it "queries and returns a float32 parameter (-Infinity)" do
    results = db.execute_query "SELECT @value AS value", params: { value: -Float::INFINITY }, types: { value: :FLOAT32 }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :FLOAT32
    _(results.rows.first[:value]).must_equal(-Float::INFINITY)
  end

  it "queries and returns a float32 parameter (-NaN)" do
    results = db.execute_query "SELECT @value AS value", params: { value: Float::NAN }, types: { value: :FLOAT32 }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :FLOAT32
    returned_value = results.rows.first[:value]
    _(returned_value).must_be_kind_of Float
    _(returned_value).must_be :nan?
  end

  it "queries and returns a NULL float32 parameter" do
    results = db.execute_query "SELECT @value AS value", params: { value: nil }, types: { value: :FLOAT32 }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :FLOAT32
    _(results.rows.first[:value]).must_be :nil?
  end

  it "queries and returns an array of float32 parameters" do
    results = db.execute_query "SELECT @value AS value", params: { value: [1.0, 2.2, 3.5] }, types: { value: [:FLOAT32] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:FLOAT32]
    result_value = results.rows.first[:value]
    _(result_value[0]).must_be_within_delta 1.0
    _(result_value[1]).must_be_within_delta 2.2
    _(result_value[2]).must_be_within_delta 3.5
  end

  it "queries and returns an array of special float32 parameters" do
    results = db.execute_query "SELECT @value AS value",
                               params: { value: [Float::INFINITY, -Float::INFINITY, -Float::NAN] }, types: { value: [:FLOAT32] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:FLOAT32]
    float_array = results.rows.first[:value]
    _(float_array.size).must_equal 3
    _(float_array[0]).must_equal Float::INFINITY
    _(float_array[1]).must_equal(-Float::INFINITY)
    _(float_array[2]).must_be :nan?
  end

  it "queries and returns an array of float32 parameters with a nil value" do
    results = db.execute_query "SELECT @value AS value", params: { value: [nil, 1.0, 2.2, 3.5] }, types: { value: [:FLOAT32] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:FLOAT32]
    result_value = results.rows.first[:value]
    _(result_value[0]).must_be :nil?
    _(result_value[1]).must_be_within_delta 1.0
    _(result_value[2]).must_be_within_delta 2.2
    _(result_value[3]).must_be_within_delta 3.5
  end

  it "queries and returns an empty array of float32 parameters" do
    results = db.execute_query "SELECT @value AS value", params: { value: [] }, types: { value: [:FLOAT32] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:FLOAT32]
    _(results.rows.first[:value]).must_equal []
  end

  it "queries and returns a NULL array of float32 parameters" do
    results = db.execute_query "SELECT @value AS value", params: { value: nil }, types: { value: [:FLOAT32] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:FLOAT32]
    _(results.rows.first[:value]).must_be :nil?
  end
end
