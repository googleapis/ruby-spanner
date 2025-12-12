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

describe "Spanner Client", :params, :uuid, :spanner do
  let(:db) { spanner_client }

  it "queries and returns an :UUID parameter" do
    skip if emulator_enabled?

    uuid = SecureRandom.uuid
    results = db.execute_query "SELECT @value AS value", params: { value: uuid }, types: { value: :UUID }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :UUID
    _(results.rows.first[:value]).must_equal uuid
  end

  it "queries and returns a NULL :UUID parameter" do
    skip if emulator_enabled?

    results = db.execute_query "SELECT @value AS value", params: { value: nil }, types: { value: :UUID }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal :UUID
    _(results.rows.first[:value]).must_be :nil?
  end

  it "queries and returns an array of :UUID parameters" do
    skip if emulator_enabled?

    uuids = [SecureRandom.uuid, SecureRandom.uuid, SecureRandom.uuid]
    results = db.execute_query "SELECT @value AS value", params: { value: uuids }, types: { value: [:UUID] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:UUID]
    _(results.rows.first[:value]).must_equal uuids
  end

  it "queries and returns an array of :UUID parameters with a nil value" do
    skip if emulator_enabled?

    uuids = [nil, SecureRandom.uuid, SecureRandom.uuid, SecureRandom.uuid]
    results = db.execute_query "SELECT @value AS value", params: { value: uuids }, types: { value: [:UUID] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:UUID]
    _(results.rows.first[:value]).must_equal uuids
  end

  it "queries and returns an empty array of :UUID parameters" do
    skip if emulator_enabled?

    results = db.execute_query "SELECT @value AS value", params: { value: [] }, types: { value: [:UUID] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:UUID]
    _(results.rows.first[:value]).must_equal []
  end

  it "queries and returns a NULL array of :UUID parameters" do
    skip if emulator_enabled?

    results = db.execute_query "SELECT @value AS value", params: { value: nil }, types: { value: [:UUID] }

    _(results).must_be_kind_of Google::Cloud::Spanner::Results
    _(results.fields[:value]).must_equal [:UUID]
    _(results.rows.first[:value]).must_be :nil?
  end
end
