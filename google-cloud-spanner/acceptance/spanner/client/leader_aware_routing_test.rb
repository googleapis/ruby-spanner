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
require "concurrent"

describe "Leader Aware Routing", :transaction, :spanner do
  let(:db) { spanner_client }

  before do
    db.project.service.enable_leader_aware_routing = false
    db.commit do |c|
      c.delete "accounts"
      c.insert "accounts", default_account_rows
    end
  end

  after do
    db.project.service.enable_leader_aware_routing = true
    db.delete "accounts"
  end

  it "runs basic transaction without leader aware routing" do
    db.transaction do |tx|
      tx_results = tx.execute_query "SELECT * from accounts"
      _(tx_results.rows.count).must_equal default_account_rows.length
    end
  end
end
