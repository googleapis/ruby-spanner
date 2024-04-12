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

  focus; it "performs batch operation" do
    results = db.batch_write do |b|
      b.mutation_group do |mg|
        mg.update "accounts", [{ account_id: 1, username: "Charlie", active: false }]
        mg.insert "accounts", [{ account_id: 4, username: "Harvey",  active: true }]
      end

    end
    pp results
    pp results.to_a
  end
end
