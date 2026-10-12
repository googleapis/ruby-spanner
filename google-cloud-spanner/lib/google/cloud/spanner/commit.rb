# Copyright 2017 Google LLC
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


require "google/cloud/spanner/data"
require "google/cloud/spanner/convert"
require "google/cloud/spanner/send_options"
require "google/cloud/spanner/ack_options"

module Google
  module Cloud
    module Spanner
      ##
      # # Commit
      #
      # Accepts mutations for execution within a transaction. All writes will
      # execute atomically at a single logical point in time across columns,
      # rows, and tables in a database.
      #
      # All changes are accumulated in memory until the block passed to
      # {Client#commit} completes.
      #
      # @example
      #   require "google/cloud/spanner"
      #
      #   spanner = Google::Cloud::Spanner.new
      #
      #   db = spanner.client "my-instance", "my-database"
      #
      #   db.commit do |c|
      #     c.update "users", [{ id: 1, name: "Charlie", active: false }]
      #     c.insert "users", [{ id: 2, name: "Harvey",  active: true }]
      #   end
      #
      class Commit
        ##
        # @private
        def initialize
          # @type [Array<Google::Cloud::Spanner::V1::Mutation>]
          @mutations = []
        end

        ##
        # Inserts or updates rows in a table. If any of the rows already exist,
        # then its column values are overwritten with the ones provided. Any
        # column values not explicitly written are preserved.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#commit} completes.
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
        #   | `PROTO`     | Determined by proto_fqn | |
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
        #   db.commit do |c|
        #     c.upsert "users", [{ id: 1, name: "Charlie", active: false },
        #                        { id: 2, name: "Harvey",  active: true }]
        #   end
        #
        def upsert table, *rows
          mutations_from_rows table, rows, "insert_or_update"
        end
        alias save upsert

        ##
        # Inserts new rows in a table. If any of the rows already exist, the
        # write or request fails with error {Google::Cloud::AlreadyExistsError}.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#commit} completes.
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
        #   | `PROTO`     | Determined by proto_fqn | |
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
        #   db.commit do |c|
        #     c.insert "users", [{ id: 1, name: "Charlie", active: false },
        #                        { id: 2, name: "Harvey",  active: true }]
        #   end
        #
        def insert table, *rows
          mutations_from_rows table, rows, "insert"
        end

        ##
        # Updates existing rows in a table. If any of the rows does not already
        # exist, the request fails with error {Google::Cloud::NotFoundError}.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#commit} completes.
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
        #   | `PROTO`     | Determined by proto_fqn | |
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
        #   db.commit do |c|
        #     c.update "users", [{ id: 1, name: "Charlie", active: false },
        #                        { id: 2, name: "Harvey",  active: true }]
        #   end
        #
        def update table, *rows
          mutations_from_rows table, rows, "update"
        end

        ##
        # Inserts or replaces rows in a table. If any of the rows already exist,
        # it is deleted, and the column values provided are inserted instead.
        # Unlike #upsert, this means any values not explicitly written become
        # `NULL`.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#commit} completes.
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
        #   | `PROTO`     | Determined by proto_fqn | |
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
        #   db.commit do |c|
        #     c.replace "users", [{ id: 1, name: "Charlie", active: false },
        #                        { id: 2, name: "Harvey",  active: true }]
        #   end
        #
        def replace table, *rows
          mutations_from_rows table, rows, "replace"
        end

        ##
        # Deletes rows from a table. Succeeds whether or not the specified rows
        # were present.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#commit} completes.
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
        #   db.commit do |c|
        #     c.delete "users", [1, 2, 3]
        #   end
        #
        def delete table, keys = []
          @mutations += [
            V1::Mutation.new(
              delete: V1::Mutation::Delete.new(
                table: table, key_set: key_set(keys)
              )
            )
          ]
          keys
        end

        ##
        # Sends a message to a queue.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#commit} completes.
        #
        # @param [String] queue The name of the queue.
        # @param [Object, Array<Object>] key A single key or array of composite key parts for the message.
        # @param [String, IO, StringIO, Hash, Google::Protobuf::MessageExts] payload
        #   The message payload. It is converted the same way as column values
        #   (see {#upsert}):
        #   * `String` - Sent as text.
        #   * `IO`, `StringIO`, `File` - Sent as bytes (Base64-encoded). Ruby has
        #     no bytes type, so wrap binary data in a `StringIO`.
        #   * `Hash` - Serialized to a JSON string.
        #   * `Google::Protobuf::MessageExts` - Serialized and Base64-encoded.
        # @param [SendOptions, Hash] send_options A {SendOptions} or a hash of
        #   send options. Send options are optional. The following options can
        #   be provided:
        #
        #   * `:deliver_time` (Time) The time at which Spanner begins trying to
        #     deliver the message. If not set, the message is delivered
        #     immediately. A time in the past is replaced with a time closer to
        #     the current time.
        #
        # @return [Object, Array<Object>] The key of the message.
        #
        # @raise [ArgumentError] If the payload type is unsupported, or if
        #   `send_options` is not a {SendOptions}, a hash of its options, or
        #   `nil`.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   db.commit do |c|
        #     c.send_message "OrderQueue", 101, StringIO.new("Order payload")
        #   end
        #
        # @example Deliver the message in five minutes
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   send_options = { deliver_time: Time.now + 300 }
        #
        #   db.commit do |c|
        #     c.send_message "OrderQueue", 101, StringIO.new("Order payload"),
        #                    send_options: send_options
        #   end
        #
        def send_message queue, key, payload, send_options: nil
          send_options = build_options send_options, SendOptions
          send_mutation = V1::Mutation::Send.new(
            queue: queue,
            key: key_list_value(key),
            payload: Convert.object_to_grpc_value(payload),
            deliver_time: Convert.time_to_timestamp(send_options.deliver_time)
          )
          @mutations += [V1::Mutation.new(send: send_mutation)]
          key
        end

        ##
        # Acknowledges a message in a queue, which removes it from the queue.
        #
        # All changes are accumulated in memory until the block passed to
        # {Client#commit} completes.
        #
        # @param [String] queue The name of the queue.
        # @param [Object, Array<Object>] key A single key or array of composite key parts to match.
        # @param [AckOptions, Hash] ack_options An {AckOptions} or a hash of ack
        #   options. Ack options are optional. The following options can be
        #   provided:
        #
        #   * `:ignore_not_found` (Boolean) If `true`, the ack succeeds even if
        #     the message doesn't exist. Default value is `false`, which fails
        #     the commit with `NOT_FOUND` when the message doesn't exist.
        #
        # @return [Object, Array<Object>] The key of the message.
        #
        # @raise [ArgumentError] If `ack_options` is not an {AckOptions}, a
        #   hash of its options, or `nil`.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   db.commit do |c|
        #     c.ack_message "OrderQueue", 101
        #   end
        #
        # @example Ack a message whether or not it exists
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   ack_options = { ignore_not_found: true }
        #
        #   db.commit do |c|
        #     c.ack_message "OrderQueue", 101, ack_options: ack_options
        #   end
        #
        def ack_message queue, key, ack_options: nil
          ack_options = build_options ack_options, AckOptions
          ack_mutation = V1::Mutation::Ack.new(
            queue: queue,
            key: key_list_value(key),
            ignore_not_found: ack_options.ignore_not_found
          )
          @mutations += [V1::Mutation.new(ack: ack_mutation)]
          key
        end

        # Return the current mutations added to this `Spanner::Commit` object.
        # @private
        # @return [Array<Google::Cloud::Spanner::V1::Mutation>]
        def mutations
          @mutations
        end

        protected

        ##
        # @private
        # Generates mutations from `rows` to be performed on a given table, converting
        # given rows to their corresponding column and gRPC values.
        #
        # @param [String] table The name of the table in the database to be
        #   modified.
        # @param [Array<Hash>] rows One or more hash objects with the hash keys
        #   matching the table's columns, and the hash values matching the
        #   table's values.
        # @param [String] type The type of mutation to be performed.
        #
        def mutations_from_rows table, rows, type
          rows = Array(rows).flatten
          return rows if rows.empty?
          rows.compact
          rows.delete_if { |row| row.respond_to?(:empty?) && row.empty? }
          @mutations += rows.map do |row|
            # This case applies whenever a Protobuf object is the row itself, and not part of individual column fields.
            if row.class.respond_to? :descriptor
              columns = row.class.descriptor.map(&:name)
              values = [Google::Protobuf::ListValue.new(values: [Convert.object_to_grpc_value(row, :PROTO)])]
            else
              columns = row.keys.map(&:to_s)
              values = [Convert.object_to_grpc_value(row.values).list_value]
            end
            V1::Mutation.new(
              "#{type}": V1::Mutation::Write.new(
                table: table, columns: columns,
                values: values
              )
            )
          end
          rows
        end

        ##
        # @private
        # Generates a `ListValue` protobuf containing gRPC values for the given primary key.
        #
        # @param [Object, Array<Object>] key A single, or array of keys to convert.
        # @return [Google::Protobuf::ListValue]
        #
        def key_list_value key
          key = [key] unless key.is_a? Array
          key_list = key.map do |k|
            Convert.object_to_grpc_value k
          end
          Google::Protobuf::ListValue.new values: key_list
        end

        ##
        # @private
        # Returns `options` as an instance of `options_class`, so callers can
        # pass the options object, a hash of its keyword arguments, or `nil`.
        #
        # @param [Object, Hash, nil] options The options given by the caller.
        # @param [Class] options_class The options class, e.g. {SendOptions}.
        # @return [Object] An instance of `options_class`.
        # @raise [ArgumentError] If `options` is any other type, or the hash
        #   has a key that `options_class` doesn't accept.
        #
        def build_options options, options_class
          case options
          when options_class then options
          when nil then options_class.new
          when Hash then options_class.new(**options)
          else
            raise ArgumentError, "expected #{options_class}, Hash, or nil, got #{options.class}"
          end
        end

        def key_set keys
          return V1::KeySet.new all: true if keys.nil?
          keys = [keys] unless keys.is_a? Array
          return V1::KeySet.new all: true if keys.empty?
          if keys_are_ranges? keys
            key_ranges = keys.map do |r|
              Convert.to_key_range r
            end
            return V1::KeySet.new ranges: key_ranges
          end
          key_list = keys.map do |key|
            key = [key] unless key.is_a? Array
            Convert.object_to_grpc_value(key).list_value
          end
          V1::KeySet.new keys: key_list
        end

        def keys_are_ranges? keys
          keys.each do |key|
            return true if key.is_a? ::Range
            return true if key.is_a? Google::Cloud::Spanner::Range
          end
          false
        end
      end
    end
  end
end
