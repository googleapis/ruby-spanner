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
  let(:client) { spanner_client }
  let(:database) { spanner_client.database }
  let(:book_descriptor_path) { "#{__dir__}/../data/protos/book_descriptors.pb" }
  let(:user_descriptor_path) { "#{__dir__}/../data/protos/user_descriptors.pb" }
  let :create_book_proto do
    <<~CREATE_PROTO
      CREATE PROTO BUNDLE (
        testing.data.Book
      )
    CREATE_PROTO
  end
  let :create_book_table do
    <<~CREATE_TABLE
      CREATE TABLE Books (
        id INT64 NOT NULL,
        book testing.data.Book NOT NULL
      ) PRIMARY KEY (id)
    CREATE_TABLE
  end
  let :delete_proto_bundle do
    <<~DELETE_PROTO
      ALTER PROTO BUNDLE DELETE (
        testing.data.Book,
        testing.data.User
      )
    DELETE_PROTO
  end
  let :insert_user_proto do
    <<~INSERT_PROTO
      ALTER PROTO BUNDLE INSERT (
        testing.data.User
      )
    INSERT_PROTO
  end
  let(:drop_table) { "DROP TABLE Books" }
  let :book_descriptor_set do
    Google::Protobuf::FileDescriptorSet.decode File.binread(book_descriptor_path)
  end
  let :user_descriptor_set do
    Google::Protobuf::FileDescriptorSet.decode File.binread(user_descriptor_path)
  end

  it "performs proto bundle updates" do
    descriptor_set = Google::Protobuf::FileDescriptorSet.new
    book_descriptor_set.file.each do |file|
      descriptor_set.file << file
    end
    user_descriptor_set.file.each do |file|
      descriptor_set.file << file
    end

    # Create proto bundle with `User` and new table.
    db_job = database.update statements: [create_book_proto], descriptor_set: book_descriptor_set
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?

    db_job = database.update statements: [create_book_table], descriptor_set: book_descriptor_set
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?


    # Insert `Book` schema into the database's proto bundle.
    db_job = database.update statements: [insert_user_proto], descriptor_set: descriptor_set
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?

    # Deletes the entire proto bundle and drops the table.
    db_job = database.update statements: [drop_table], descriptor_set: descriptor_set
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?

    db_job = database.update statements: [delete_proto_bundle], descriptor_set: descriptor_set
    db_job.wait_until_done!
    raise GRPC::BadStatus.new(db_job.error.code, db_job.error.message) if db_job.error?
  end
end
