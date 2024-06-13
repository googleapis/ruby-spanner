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

module Google
  module Cloud
    module Spanner
      ##
      # Part of the BatchWrite DSL. This object defines a group of mutations
      # to be included in a BatchWrite operation. All writes within a mutation
      # group will execute atomically at a single logical point in time across
      # columns, rows, and tables in a database.
      #
      # All changes are accumulated in memory until the block passed to
      # {Client#batch_write} completes.
      #
      # @example
      #   require "google/cloud/spanner"
      #
      #   spanner = Google::Cloud::Spanner.new
      #
      #   db = spanner.client "my-instance", "my-database"
      #
      #   results = db.batch_write do |b|
      #     # First mutation group
      #     b.mutation_group do |mg|
      #       mg.upsert "Singers", [{ SingerId: 16, FirstName: "Charlie", LastName: "Terry" }]
      #     end
      #
      #     # Second mutation group
      #     b.mutation_group do |mg|
      #       mg.upsert "Singers", [{ SingerId: 17, FirstName: "Catalina", LastName: "Smith" }]
      #       mg.update "Albums", [{ SingerId: 17, AlbumId: 1, AlbumTitle: "Go Go Go" }]
      #     end
      #   end
      #
      class MutationGroup
        # @private
        def initialize
          @commit = Commit.new
        end

        ##
        # Inserts or updates rows in a table. If any of the rows already exist,
        # then its column values are overwritten with the ones provided. Any
        # column values not explicitly written are preserved.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#batch_write} completes.
        #
        # @param [String] table The name of the table in the database to be
        #   modified.
        # @param [Array<Hash>] rows One or more hash objects with the hash keys
        #   matching the table's columns, and the hash values matching the
        #   table's values.
        #
        #   Ruby types are mapped to Spanner types as follows:
        #
        #   | Spanner     | Ruby           | Notes  |
        #   |-------------|----------------|---|
        #   | `BOOL`      | `true`/`false` | |
        #   | `INT64`     | `Integer`      | |
        #   | `FLOAT64`   | `Float`        | |
        #   | `FLOAT32`   | `Float`        | |
        #   | `NUMERIC`   | `BigDecimal`   | |
        #   | `STRING`    | `String`       | |
        #   | `DATE`      | `Date`         | |
        #   | `TIMESTAMP` | `Time`, `DateTime` | |
        #   | `BYTES`     | `File`, `IO`, `StringIO`, or similar | |
        #   | `ARRAY`     | `Array` | Nested arrays are not supported. |
        #
        #   See [Data
        #   types](https://cloud.google.com/spanner/docs/data-definition-language#data_types).
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.batch_write do |b|
        #     b.mutation_group do |mg|
        #       mg.upsert "Singers", [{ SingerId: 16, FirstName: "Charlie", LastName: "Terry" }]
        #     end
        #   end
        #
        def upsert table, *rows
          @commit.upsert table, rows
        end
        alias save upsert

        ##
        # Inserts new rows in a table. If any of the rows already exist, the
        # write or request fails with error {Google::Cloud::AlreadyExistsError}.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#batch_write} completes.
        #
        # @param [String] table The name of the table in the database to be
        #   modified.
        # @param [Array<Hash>] rows One or more hash objects with the hash keys
        #   matching the table's columns, and the hash values matching the
        #   table's values.
        #
        #   Ruby types are mapped to Spanner types as follows:
        #
        #   | Spanner     | Ruby           | Notes  |
        #   |-------------|----------------|---|
        #   | `BOOL`      | `true`/`false` | |
        #   | `INT64`     | `Integer`      | |
        #   | `FLOAT64`   | `Float`        | |
        #   | `FLOAT32`   | `Float`        | |
        #   | `NUMERIC`   | `BigDecimal`   | |
        #   | `STRING`    | `String`       | |
        #   | `DATE`      | `Date`         | |
        #   | `TIMESTAMP` | `Time`, `DateTime` | |
        #   | `BYTES`     | `File`, `IO`, `StringIO`, or similar | |
        #   | `ARRAY`     | `Array` | Nested arrays are not supported. |
        #
        #   See [Data
        #   types](https://cloud.google.com/spanner/docs/data-definition-language#data_types).
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.batch_write do |b|
        #     b.mutation_group do |mg|
        #       mg.insert "Singers", [{ SingerId: 16, FirstName: "Charlie", LastName: "Terry" }]
        #     end
        #   end
        #
        def insert table, *rows
          @commit.insert table, rows
        end

        ##
        # Updates existing rows in a table. If any of the rows does not already
        # exist, the request fails with error {Google::Cloud::NotFoundError}.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#batch_write} completes.
        #
        # @param [String] table The name of the table in the database to be
        #   modified.
        # @param [Array<Hash>] rows One or more hash objects with the hash keys
        #   matching the table's columns, and the hash values matching the
        #   table's values.
        #
        #   Ruby types are mapped to Spanner types as follows:
        #
        #   | Spanner     | Ruby           | Notes  |
        #   |-------------|----------------|---|
        #   | `BOOL`      | `true`/`false` | |
        #   | `INT64`     | `Integer`      | |
        #   | `FLOAT64`   | `Float`        | |
        #   | `FLOAT32`   | `Float`        | |
        #   | `NUMERIC`   | `BigDecimal`   | |
        #   | `STRING`    | `String`       | |
        #   | `DATE`      | `Date`         | |
        #   | `TIMESTAMP` | `Time`, `DateTime` | |
        #   | `BYTES`     | `File`, `IO`, `StringIO`, or similar | |
        #   | `ARRAY`     | `Array` | Nested arrays are not supported. |
        #
        #   See [Data
        #   types](https://cloud.google.com/spanner/docs/data-definition-language#data_types).
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.batch_write do |b|
        #     b.mutation_group do |mg|
        #       mg.update "Singers", [{ SingerId: 16, FirstName: "Charlie", LastName: "Terry" }]
        #     end
        #   end
        #
        def update table, *rows
          @commit.update table, rows
        end

        ##
        # Inserts or replaces rows in a table. If any of the rows already exist,
        # it is deleted, and the column values provided are inserted instead.
        # Unlike #upsert, this means any values not explicitly written become
        # `NULL`.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#batch_write} completes.
        #
        # @param [String] table The name of the table in the database to be
        #   modified.
        # @param [Array<Hash>] rows One or more hash objects with the hash keys
        #   matching the table's columns, and the hash values matching the
        #   table's values.
        #
        #   Ruby types are mapped to Spanner types as follows:
        #
        #   | Spanner     | Ruby           | Notes  |
        #   |-------------|----------------|---|
        #   | `BOOL`      | `true`/`false` | |
        #   | `INT64`     | `Integer`      | |
        #   | `FLOAT64`   | `Float`        | |
        #   | `FLOAT32`   | `Float`        | |
        #   | `NUMERIC`   | `BigDecimal`   | |
        #   | `STRING`    | `String`       | |
        #   | `DATE`      | `Date`         | |
        #   | `TIMESTAMP` | `Time`, `DateTime` | |
        #   | `BYTES`     | `File`, `IO`, `StringIO`, or similar | |
        #   | `ARRAY`     | `Array` | Nested arrays are not supported. |
        #
        #   See [Data
        #   types](https://cloud.google.com/spanner/docs/data-definition-language#data_types).
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.batch_write do |b|
        #     b.mutation_group do |mg|
        #       mg.replace "Singers", [{ SingerId: 16, FirstName: "Charlie", LastName: "Terry" }]
        #     end
        #   end
        #
        def replace table, *rows
          @commit.replace table, rows
        end

        ##
        # Deletes rows from a table. Succeeds whether or not the specified rows
        # were present.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#batch_write} completes.
        #
        # @param [String] table The name of the table in the database to be
        #   modified.
        # @param [Object, Array<Object>] keys A single, or list of keys or key
        #   ranges to match returned data to. Values should have exactly as many
        #   elements as there are columns in the primary key.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.batch_write do |b|
        #     b.mutation_group do |mg|
        #       mg.delete "Singers", [1, 2, 3]
        #     end
        #   end
        #
        def delete table, keys = []
          @commit.delete table, keys
        end

        ##
        # @private
        # All of the mutations created in the transaction block.
        def mutations
          @commit.mutations
        end
      end
    end
  end
end
