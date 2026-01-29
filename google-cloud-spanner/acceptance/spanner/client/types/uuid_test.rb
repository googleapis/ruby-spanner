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

describe "Spanner Client", :types, :uuid, :spanner do
  let(:db) { spanner_client }
  let(:table_name) { "stuffs" }

  it "writes and reads :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    uuid = SecureRandom.uuid
    db.upsert table_name, { id: id, uuid: uuid }
    results = db.read table_name, [:id, :uuid], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuid: :UUID })
    _(results.rows.first.to_h).must_equal({ id: id, uuid: uuid })
  end

  it "writes and queries :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    uuid = SecureRandom.uuid
    db.upsert table_name, { id: id, uuid: uuid }
    results = db.execute_query "SELECT id, uuid FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuid: :UUID })
    _(results.rows.first.to_h).must_equal({ id: id, uuid: uuid })
  end

  it "writes and reads NULL :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    db.upsert table_name, { id: id, uuid: nil }
    results = db.read table_name, [:id, :uuid], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuid: :UUID })
    _(results.rows.first.to_h).must_equal({ id: id, uuid: nil })
  end

  it "writes and queries NULL :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    db.upsert table_name, { id: id, uuid: nil }
    results = db.execute_query "SELECT id, uuid FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuid: :UUID })
    _(results.rows.first.to_h).must_equal({ id: id, uuid: nil })
  end

  it "writes and reads array of :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    uuids = [SecureRandom.uuid, SecureRandom.uuid, SecureRandom.uuid]
    db.upsert table_name, { id: id, uuids: uuids }
    results = db.read table_name, [:id, :uuids], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: [uuids[0], uuids[1], uuids[2]] })
  end

  it "writes and queries array of :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    uuids = [nil, SecureRandom.uuid, SecureRandom.uuid, SecureRandom.uuid]
    db.upsert table_name, { id: id, uuids: uuids }
    results = db.execute_query "SELECT id, uuids FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: [uuids[0], uuids[1], uuids[2], uuids[3]] })
  end

  it "writes and reads array of :UUID with NULL" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    uuids = [nil, SecureRandom.uuid, SecureRandom.uuid, SecureRandom.uuid]
    db.upsert table_name, { id: id, uuids: uuids }
    results = db.read table_name, [:id, :uuids], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: [uuids[0], uuids[1], uuids[2], uuids[3]] })
  end

  it "writes and queries array of :UUID with NULL" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    uuids = [nil, SecureRandom.uuid, SecureRandom.uuid, SecureRandom.uuid]
    db.upsert table_name, { id: id, uuids: uuids }
    results = db.execute_query "SELECT id, uuids FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: [uuids[0], uuids[1], uuids[2], uuids[3]] })
  end

  it "writes and reads empty array of :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    db.upsert table_name, { id: id, uuids: [] }
    results = db.read table_name, [:id, :uuids], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: [] })
  end

  it "writes and queries empty array of :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    db.upsert table_name, { id: id, uuids: [] }
    results = db.execute_query "SELECT id, uuids FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: [] })
  end

  it "writes and reads NULL array of :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    db.upsert table_name, { id: id, uuids: nil }
    results = db.read table_name, [:id, :uuids], keys: id

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: nil })
  end

  it "writes and queries NULL array of :UUID" do
    skip if emulator_enabled?

    id = SecureRandom.int64
    db.upsert table_name, { id: id, uuids: nil }
    results = db.execute_query "SELECT id, uuids FROM #{table_name} WHERE id = @id", params: { id: id }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields.to_h).must_equal({ id: :INT64, uuids: [:UUID] })
    _(results.rows.first.to_h).must_equal({ id: id, uuids: nil })
  end
end
