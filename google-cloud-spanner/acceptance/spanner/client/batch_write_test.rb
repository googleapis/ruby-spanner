# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License 00:00:00Z");
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

describe "Spanner Client", :commit_timestamp, :spanner do
  let(:db) { spanner_client }
  let(:table_name) { "commit_timestamp_test" }
  let(:table_types) { [:committs] }

  before do
    db.commit do |c|
      c.delete "accounts"
      c.insert "accounts", default_account_rows
    end
    unless emulator_enabled?
      db.commit do |c|
        c.delete "accounts"
        c.insert "accounts", default_pg_account_rows
      end
    end
  end

focus
  it "performs batch operation" do
    new_rows = [
      { account_id: 1, username: "Charlie", active: false },
      { account_id: 4, username: "Harvey",  active: true },
      { account_id: 5, username: "Becky", active: false }
    ]
    results = db.batch_write do |b|
      b.mutation_group do |mg|
        mg.update "accounts", [new_rows[0]]
      end
      b.mutation_group do |mg|
        mg.insert "accounts", [new_rows[1]]
      end
      b.mutation_group do |mg|
        mg.insert "accounts", [new_rows[2]]
      end
    end

    # Ensure returned indexes cover what was requested with no overlaps
    _(results.ok_indexes.sort).must_equal [0, 1, 2]

    # Ensure that all ok results have a timestamp
    results.each do |result|
      _(result.commit_timestamp).wont_be(:nil?) if result.ok?
    end

    # Ensure that the rows were updated correctly
    read_result = db.read "accounts", [:account_id, :username, :active]
    read_rows = read_result.rows.to_h { |row| [row[:account_id], row.to_h] }
    results.each do |result|
      next unless result.ok?
      result.indexes.each do |index|
        expected_row = new_rows[index]
        actual_row = read_rows[expected_row[:account_id]]
        _(actual_row).must_equal expected_row
      end
    end
  end
end
