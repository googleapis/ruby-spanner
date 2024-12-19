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

describe "Spanner Client", :spanner do
  let(:db) { spanner }
  let(:spanner_client) { spanner_client }
  let(:admin) { $spanner_db_admin }
  let(:instance_id) { $spanner_instance_id }
  let(:database_id) { $spanner_database_id }
  let(:db_path) { admin.database_path project: spanner.project_id, instance: instance_id, database: database_id }

  it "creates a table using `CREATE PROTO BUNDLE` via descriptor set path" do
    db_path = admin.database_path project: spanner.project_id,
                                  instance: instance_id,
                                  database: database_id
    descriptor_path = "#{__dir__}/../data/protos/simple/user_descriptors.pb"

    ddl_proto_statement =
      <<~CREATE_PROTO
        CREATE PROTO BUNDLE (
          spanner.testing.data.User#{' '}
        )
      CREATE_PROTO

    job = admin.update_database_ddl database: db_path, statements: [ddl_proto_statement],
                                    descriptor_set: descriptor_path
    _(job).wont_be :done? unless emulator_enabled?
    job.wait_until_done!

    _(job).must_be :done?
    raise Google::Cloud::Error.from_error(job.error) if job.error?

    ddl_table_statement =
      <<~CREATE_TABLE
        CREATE TABLE Users (
          Id INT64 NOT NULL,
          User `spanner.testing.data.User` NOT NULL,#{' '}
        )
      CREATE_TABLE

    job2 = admin.update_database_ddl database: db_path, statements: [ddl_table_statement]

    _(job2).must_be_kind_of Google::Cloud::Spanner::Database::Job
    _(job2).wont_be :done? unless emulator_enabled?
    job2.wait_until_done!

    _(job2).must_be :done?
    raise Google::Cloud::Error.from_error(job.error) if job.error?

    db_client.ddl
  end

  it "updates a table DDL using `ALTER PROTO BUNDLE UPDATE` via descriptor set path" do
    db_path = admin.database_path project: spanner.project_id,
                                  instance: instance_id,
                                  database: database_id
    descriptor_path = "#{__dir__}/../data/protos/complex/user_descriptors.pb"

    ddl_proto_statement =
      <<~UPDATE_PROTO
        ALTER PROTO BUNDLE UPDATE (
          spanner.testing.data.User#{' '}
        )
      UPDATE_PROTO

    job = admin.update_database_ddl database: db_path, statements: [ddl_proto_statement],
                                    descriptor_set: descriptor_path
    _(job).wont_be :done? unless emulator_enabled?
    job.wait_until_done!

    _(job).must_be :done?
    raise Google::Cloud::Error.from_error(job.error) if job.error?
    db_client.ddl
  end
end
