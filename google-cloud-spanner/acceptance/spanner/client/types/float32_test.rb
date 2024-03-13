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

describe "Spanner Client", :types, :float32, :spanner do
  let(:db) { spanner_client }
  let(:table_name) { "stuffs" }

  focus; it "writes and reads float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: 99.99 }
    results = db.read table_name, [:id, :float32], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    first_row = results.rows.first.to_h
    _(first_row[:id]).must_equal id
    _(first_row[:float32]).must_be_within_delta 99.99
  end

  focus; it "writes and queries float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: 99.99 }
    results = db.execute_sql "SELECT id, float32 FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    first_row = results.rows.first.to_h
    _(first_row[:id]).must_equal id
    _(first_row[:float32]).must_be_within_delta 99.99
  end

  focus; it "writes and reads Infinity float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: Float::INFINITY }
    results = db.read table_name, [:id, :float32], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    _(results.rows.first.to_h).must_equal({ id: id, float32: Float::INFINITY })
  end

  focus; it "writes and queries Infinity float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: Float::INFINITY }
    results = db.execute_sql "SELECT id, float32 FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    _(results.rows.first.to_h).must_equal({ id: id, float32: Float::INFINITY })
  end

  focus; it "writes and reads -Infinity float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: -Float::INFINITY }
    results = db.read table_name, [:id, :float32], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    _(results.rows.first.to_h).must_equal({ id: id, float32: -Float::INFINITY })
  end

  focus; it "writes and queries -Infinity float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: -Float::INFINITY }
    results = db.execute_sql "SELECT id, float32 FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    _(results.rows.first.to_h).must_equal({ id: id, float32: -Float::INFINITY })
  end

  focus; it "writes and reads NaN float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: Float::NAN }
    results = db.read table_name, [:id, :float32], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    returned_hash = results.rows.first.to_h
    returned_value = returned_hash[:float32]
    _(returned_value).must_be_kind_of Float
    _(returned_value).must_be :nan?
  end

  focus; it "writes and queries NaN float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: Float::NAN }
    results = db.execute_sql "SELECT id, float32 FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    returned_hash = results.rows.first.to_h
    returned_value = returned_hash[:float32]
    _(returned_value).must_be_kind_of Float
    _(returned_value).must_be :nan?
  end

  focus; it "writes and reads NULL float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: nil }
    results = db.read table_name, [:id, :float32], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    _(results.rows.first.to_h).must_equal({ id: id, float32: nil })
  end

  focus; it "writes and queries NULL float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32: nil }
    results = db.execute_sql "SELECT id, float32 FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32: :FLOAT32 })
    _(results.rows.first.to_h).must_equal({ id: id, float32: nil })
  end

  focus; it "writes and reads array of float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: [77.77, 88.88, 99.99] }
    results = db.read table_name, [:id, :float32s], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    first_row = results.rows.first.to_h
    _(first_row[:id]).must_equal id
    _(first_row[:float32s][0]).must_be_within_delta 77.77
    _(first_row[:float32s][1]).must_be_within_delta 88.88
    _(first_row[:float32s][2]).must_be_within_delta 99.99
  end

  focus; it "writes and queries array of float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: [77.77, 88.88, 99.99] }
    results = db.execute_sql "SELECT id, float32s FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    first_row = results.rows.first.to_h
    _(first_row[:id]).must_equal id
    _(first_row[:float32s][0]).must_be_within_delta 77.77
    _(first_row[:float32s][1]).must_be_within_delta 88.88
    _(first_row[:float32s][2]).must_be_within_delta 99.99
  end

  focus; it "writes and reads array of float32 with NULL" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: [nil, 77.77, 88.88, 99.99] }
    results = db.read table_name, [:id, :float32s], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    first_row = results.rows.first.to_h
    _(first_row[:id]).must_equal id
    _(first_row[:float32s][0]).must_be :nil?
    _(first_row[:float32s][1]).must_be_within_delta 77.77
    _(first_row[:float32s][2]).must_be_within_delta 88.88
    _(first_row[:float32s][3]).must_be_within_delta 99.99
  end

  focus; it "writes and queries array of float32 with NULL" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: [nil, 77.77, 88.88, 99.99] }
    results = db.execute_sql "SELECT id, float32s FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    first_row = results.rows.first.to_h
    _(first_row[:id]).must_equal id
    _(first_row[:float32s][0]).must_be :nil?
    _(first_row[:float32s][1]).must_be_within_delta 77.77
    _(first_row[:float32s][2]).must_be_within_delta 88.88
    _(first_row[:float32s][3]).must_be_within_delta 99.99
  end

  focus; it "writes and reads empty array of float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: [] }
    results = db.read table_name, [:id, :float32s], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    _(results.rows.first.to_h).must_equal({ id: id, float32s: [] })
  end

  focus; it "writes and queries empty array of float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: [] }
    results = db.execute_sql "SELECT id, float32s FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    _(results.rows.first.to_h).must_equal({ id: id, float32s: [] })
  end

  focus; it "writes and reads NULL array of float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: nil }
    results = db.read table_name, [:id, :float32s], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    _(results.rows.first.to_h).must_equal({ id: id, float32s: nil })
  end

  focus; it "writes and queries NULL array of float32" do
    id = SecureRandom.int64
    db.upsert table_name, { id: id, float32s: nil }
    results = db.execute_sql "SELECT id, float32s FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, float32s: [:FLOAT32] })
    _(results.rows.first.to_h).must_equal({ id: id, float32s: nil })
  end
end
