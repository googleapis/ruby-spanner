# Copyright 2016 Google LLC
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
require "google/cloud/spanner/results"
require "google/cloud/spanner/commit"
require "google/cloud/spanner/commit_response"
require "google/cloud/spanner/batch_update"
require "google/cloud/spanner/batch_write"

module Google
  module Cloud
    module Spanner
      ##
      # @private
      #
      # # Session
      #
      # A session can be used to perform transactions that read and/or modify
      # data in a Cloud Spanner database. Sessions are meant to be reused for
      # many consecutive transactions.
      #
      # Sessions can only execute one transaction at a time. To execute multiple
      # concurrent read-write/write-only transactions, create multiple sessions.
      # Note that standalone reads and queries use a transaction internally, and
      # count toward the one transaction limit.
      #
      # Cloud Spanner limits the number of sessions that can exist at any given
      # time; thus, it is a good idea to delete idle and/or unneeded sessions.
      # Aside from explicit deletes, Cloud Spanner can delete sessions for which
      # no operations are sent for more than an hour.
      #
      class Session
        # The wrapped `V1::Session` protobuf session object.
        # @private
        # @return [::Google::Cloud::Spanner::V1::Session]
        attr_accessor :grpc

        # The `Spanner::Service` object.
        # @private
        # @return [::Google::Cloud::Spanner::Service]
        attr_accessor :service

        # A hash of values to specify the custom query options for executing SQL query.
        # Example option: `:optimizer_version`.
        # @private
        # @return [::Hash, nil]
        attr_accessor :query_options

        # Creates a new `Spanner::Session` instance.
        # @param grpc [::Google::Cloud::Spanner::V1::Session] Underlying `V1::Session` object.
        # @param service [::Google::Cloud::Spanner::Service] A `Spanner::Service` object.
        # @param query_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   query options for executing SQL query. Example option: `:optimizer_version`.
        # @private
        def initialize grpc, service, query_options: nil
          @grpc = grpc
          @service = service
          @query_options = query_options
          @created_time = Process.clock_gettime Process::CLOCK_MONOTONIC
        end

        # The unique identifier for the project.
        # @private
        # @return [::String]
        def project_id
          @grpc.name.split("/")[1]
        end

        # The unique identifier for the instance.
        # @private
        # @return [::String]
        def instance_id
          @grpc.name.split("/")[3]
        end

        # The unique identifier for the database.
        # @private
        # @return [::String]
        def database_id
          @grpc.name.split("/")[5]
        end

        # The unique identifier for the session.
        # @private
        # @return [::String]
        def session_id
          @grpc.name.split("/")[7]
        end

        # Full session name.
        # Values are of the form:
        # `projects/<project_id>/instances/<instance_id>/databases/<database_id>/sessions/<session_id>`.
        # @private
        # @return [::String]
        def path
          @grpc.name
        end

        ##
        # Executes a SQL query.
        #
        # @param [String] sql The SQL query string. See [Query
        #   syntax](https://cloud.google.com/spanner/docs/query-syntax).
        #
        #   The SQL query string can contain parameter placeholders. A parameter
        #   placeholder consists of "@" followed by the parameter name.
        #   Parameter names consist of any combination of letters, numbers, and
        #   underscores.
        # @param [Hash] params SQL parameters for the query string. The
        #   parameter placeholders, minus the "@", are the the hash keys, and
        #   the literal values are the hash values. If the query string contains
        #   something like "WHERE id > @msg_id", then the params must contain
        #   something like `:msg_id => 1`.
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
        #   | `STRUCT`    | `Hash`, {Data} | |
        #   | `PROTO`     | Determined by proto_fqn | |
        #
        #   See [Data
        #   types](https://cloud.google.com/spanner/docs/data-definition-language#data_types).
        #
        #   See [Data Types - Constructing a
        #   STRUCT](https://cloud.google.com/spanner/docs/data-types#constructing-a-struct).
        # @param [Hash] types Types of the SQL parameters in `params`. It is not
        #   always possible for Cloud Spanner to infer the right SQL type from a
        #   value in `params`. In these cases, the `types` hash must be used to
        #   specify the SQL type for these values.
        #
        #   The keys of the hash should be query string parameter placeholders,
        #   minus the "@". The values of the hash should be Cloud Spanner type
        #   codes from the following list:
        #
        #   * `:BOOL`
        #   * `:BYTES`
        #   * `:DATE`
        #   * `:FLOAT64`
        #   * `:FLOAT32`
        #   * `:NUMERIC`
        #   * `:INT64`
        #   * `:STRING`
        #   * `:TIMESTAMP`
        #   * `Array` - Lists are specified by providing the type code in an
        #     array. For example, an array of integers are specified as
        #     `[:INT64]`.
        #   * {Fields} - Types for STRUCT values (`Hash`/{Data} objects) are
        #     specified using a {Fields} object.
        #
        #   Types are optional.
        # @param [Google::Cloud::Spanner::V1::TransactionSelector] transaction The
        #   transaction selector value to send. Only used for single-use
        #   transactions.
        # @param [Integer] seqno A per-transaction sequence number used to
        #   identify this request.
        # @param [Hash] query_options A hash of values to specify the custom
        #   query options for executing SQL query. Query options are optional.
        #   The following settings can be provided:
        #
        #   * `:optimizer_version` (String) The version of optimizer to use.
        #     Empty to use database default. "latest" to use the latest
        #     available optimizer version.
        #   * `:optimizer_statistics_package` (String) Statistics package to
        #     use. Empty to use the database default.
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @param precommit_token_notify [::Proc, nil] Optional.
        #   The notification function for the precommit token.
        #
        # @return [Google::Cloud::Spanner::Results] The results of the query
        #   execution.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.execute_query "SELECT * FROM users"
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        # @example Query using query parameters:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.execute_query(
        #     "SELECT * FROM users WHERE active = @active",
        #     params: { active: true }
        #   )
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        # @example Query with a SQL STRUCT query parameter as a Hash:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   user_hash = { id: 1, name: "Charlie", active: false }
        #
        #   results = db.execute_query(
        #     "SELECT * FROM users WHERE " \
        #     "ID = @user_struct.id " \
        #     "AND name = @user_struct.name " \
        #     "AND active = @user_struct.active",
        #     params: { user_struct: user_hash }
        #   )
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        # @example Specify the SQL STRUCT type using Fields object:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   user_type = db.fields id: :INT64, name: :STRING, active: :BOOL
        #   user_hash = { id: 1, name: nil, active: false }
        #
        #   results = db.execute_query(
        #     "SELECT * FROM users WHERE " \
        #     "ID = @user_struct.id " \
        #     "AND name = @user_struct.name " \
        #     "AND active = @user_struct.active",
        #     params: { user_struct: user_hash },
        #     types: { user_struct: user_type }
        #   )
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        # @example Or, query with a SQL STRUCT as a typed Data object:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   user_type = db.fields id: :INT64, name: :STRING, active: :BOOL
        #   user_data = user_type.struct id: 1, name: nil, active: false
        #
        #   results = db.execute_query(
        #     "SELECT * FROM users WHERE " \
        #     "ID = @user_struct.id " \
        #     "AND name = @user_struct.name " \
        #     "AND active = @user_struct.active",
        #     params: { user_struct: user_struct }
        #   )
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        # @example Query using query options:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.execute_query \
        #     "SELECT * FROM users", query_options: {
        #       optimizer_version: "1",
        #       optimizer_statistics_package: "auto_20191128_14_47_22UTC"
        #     }
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        # @example Query using custom timeout and retry policy:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   timeout = 30.0
        #   retry_policy = {
        #     initial_delay: 0.25,
        #     max_delay:     32.0,
        #     multiplier:    1.3,
        #     retry_codes:   ["UNAVAILABLE"]
        #   }
        #   call_options = { timeout: timeout, retry_policy: retry_policy }
        #
        #   results = db.execute_query \
        #     "SELECT * FROM users", call_options: call_options
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        def execute_query sql, params: nil, types: nil, transaction: nil,
                          partition_token: nil, seqno: nil, query_options: nil,
                          request_options: nil, call_options: nil, data_boost_enabled: nil,
                          directed_read_options: nil, route_to_leader: nil,
                          precommit_token_notify: nil
          ensure_service!
          query_options = merge_if_present query_options, @query_options

          execute_query_options = {
            transaction: transaction, params: params, types: types,
            partition_token: partition_token, seqno: seqno,
            query_options: query_options, request_options: request_options,
            call_options: call_options,
            route_to_leader: route_to_leader
          }
          execute_query_options[:data_boost_enabled] = data_boost_enabled unless data_boost_enabled.nil?
          execute_query_options[:directed_read_options] = directed_read_options unless directed_read_options.nil?

          response = service.execute_streaming_sql path, sql, **execute_query_options

          results = Results.from_execute_query_response response, service, path, sql, execute_query_options,
                                                        precommit_token_notify: precommit_token_notify
          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC
          results
        end

        ##
        # Executes DML statements in a batch.
        #
        # @param [Google::Cloud::Spanner::V1::TransactionSelector] transaction The
        #   transaction selector value to send. Only used for single-use
        #   transactions.
        # @param [Integer] seqno A per-transaction sequence number used to
        #   identify this request.
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @yield [batch_update] a batch update object
        # @yieldparam [Google::Cloud::Spanner::BatchUpdate] batch_update a batch
        #   update object accepting DML statements and optional parameters and
        #   types of the parameters.
        #
        # @raise [Google::Cloud::Spanner::BatchUpdateError] If an error occurred
        #   while executing a statement. The error object contains a cause error
        #   with the service error type and message, and a list with the exact
        #   number of rows that were modified for each successful statement
        #   before the error.
        #
        # @return [::Google::Cloud::Spanner::V1::ExecuteBatchDmlResponse]
        #   An unwrapped result of the service call -- a `V1::ExecuteBatchDmlResponse` object.
        #
        def batch_update transaction, seqno, request_options: nil,
                         call_options: nil
          ensure_service!

          raise ArgumentError, "block is required" unless block_given?
          batch = BatchUpdate.new

          yield batch

          results = service.execute_batch_dml path, transaction,
                                              batch.statements, seqno,
                                              request_options: request_options,
                                              call_options: call_options
          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC
          results
        end

        ##
        # Read rows from a database table, as a simple alternative to
        # {#execute_query}.
        #
        # @param [String] table The name of the table in the database to be
        #   read.
        # @param [Array<String, Symbol>] columns The columns of table to be
        #   returned for each row matching this request.
        # @param [Object, Array<Object>] keys A single, or list of keys or key
        #   ranges to match returned data to. Values should have exactly as many
        #   elements as there are columns in the primary key.
        # @param [String] index The name of an index to use instead of the
        #   table's primary key when interpreting `id` and sorting result rows.
        #   Optional.
        # @param [Integer] limit If greater than zero, no more than this number
        #   of rows will be returned. The default is no limit.
        # @param [Google::Cloud::Spanner::V1::TransactionSelector] transaction The
        #   transaction selector value to send. Only used for single-use
        #   transactions.
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        # @param [::Google::Cloud::Spanner::V1::ReadRequest::OrderBy] order_by An option to control the order in which
        #   rows are returned from a read.
        #   To see the available options refer to
        #   ['Google::Cloud::Spanner::V1::ReadRequest::OrderBy'](https://cloud.google.com/ruby/docs/reference/google-cloud-spanner-v1/latest/Google-Cloud-Spanner-V1-ReadRequest-OrderBy)
        # @param [::Google::Cloud::Spanner::V1::ReadRequest::LockHint] lock_hint A lock hint mechanism for reads done
        #   within a transaction.
        #   To see the available options refer to
        #   ['Google::Cloud::Spanner::V1::ReadRequest::LockHint'](https://cloud.google.com/ruby/docs/reference/google-cloud-spanner-v1/latest/Google-Cloud-Spanner-V1-ReadRequest-LockHint)
        #
        # @param precommit_token_notify [::Proc, nil] Optional.
        #   The notification function for the precommit token.
        #
        # @return [Google::Cloud::Spanner::Results] The results of the read
        #   operation.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.read "users", [:id, :name]
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        def read table, columns, keys: nil, index: nil, limit: nil,
                 transaction: nil, partition_token: nil, request_options: nil,
                 call_options: nil, data_boost_enabled: nil, directed_read_options: nil,
                 route_to_leader: nil, order_by: nil, lock_hint: nil, precommit_token_notify: nil
          ensure_service!

          read_options = {
            keys: keys, index: index, limit: limit,
            transaction: transaction,
            partition_token: partition_token,
            request_options: request_options,
            call_options: call_options,
            route_to_leader: route_to_leader,
            order_by: order_by,
            lock_hint: lock_hint
          }
          read_options[:data_boost_enabled] = data_boost_enabled unless data_boost_enabled.nil?
          read_options[:directed_read_options] = directed_read_options unless directed_read_options.nil?

          response = service.streaming_read_table \
            path, table, columns, **read_options

          results = Results.from_read_response response, service, path, table, columns, read_options,
                                               precommit_token_notify: precommit_token_notify

          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC

          results
        end

        def partition_query sql, transaction, params: nil, types: nil,
                            partition_size_bytes: nil, max_partitions: nil,
                            call_options: nil
          ensure_service!

          results = service.partition_query \
            path, sql, transaction, params: params, types: types,
                                    partition_size_bytes: partition_size_bytes,
                                    max_partitions: max_partitions,
                                    call_options: call_options

          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC

          results
        end

        def partition_read table, columns, transaction, keys: nil,
                           index: nil, partition_size_bytes: nil,
                           max_partitions: nil, call_options: nil
          ensure_service!

          results = service.partition_read \
            path, table, columns, transaction,
            keys: keys, index: index,
            partition_size_bytes: partition_size_bytes,
            max_partitions: max_partitions,
            call_options: call_options

          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC

          results
        end

        ##
        # Creates changes to be applied to rows in the database.
        #
        # @param [String] transaction_id The identifier of previously-started
        #   transaction to be used instead of starting a new transaction.
        #   Optional.
        # @param [Boolean] exclude_txn_from_change_streams If set to true,
        #   mutations will not be recorded in change streams with DDL option
        #   `allow_txn_exclusion=true`. Used if starting a new transaction.
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::IsolationLevel] isolation_level Optional. The
        #   isolation level for the transaction.
        # @param [Hash] commit_options A hash of commit options.
        #   e.g., return_commit_stats. Commit options are optional.
        #   The following options can be provided:
        #
        #   * `:return_commit_stats` (Boolean) A boolean value. If `true`,
        #     then statistics related to the transaction will be included in
        #     {CommitResponse}. Default value is `false`
        #   *  `:maxCommitDelay` (Numeric) The amount of latency in millisecond in this request
        #         is willing to incur in order to improve throughput.
        #         The commit delay must be at least 0ms and at most 500ms.
        #         Default value is nil.
        #
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite::ReadLockMode] read_lock_mode
        #   The read lock mode for the transaction.
        #   Can be one of the following:
        #   * `:READ_LOCK_MODE_UNSPECIFIED` (0): The default unspecified read lock mode.
        #   * `:PESSIMISTIC` (1): The pessimistic lock mode, where read locks are acquired immediately on read.
        #   * `:OPTIMISTIC` (2): The optimistic lock mode, where locks for reads are not acquired on read
        #       but instead on a commit to validate that the data has not changed since the transaction started.
        #
        #
        # @yield [commit] The block for mutating the data.
        # @yieldparam [Google::Cloud::Spanner::Commit] commit The Commit object.
        #
        # @return [Time, CommitResponse] The timestamp at which the operation
        #   committed. If commit options are set it returns {CommitResponse}.
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
        # @example Get commit stats
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   commit_options = { return_commit_stats: true }
        #   commit_resp = db.commit commit_options: commit_options do |c|
        #     c.update "users", [{ id: 1, name: "Charlie", active: false }]
        #     c.insert "users", [{ id: 2, name: "Harvey",  active: true }]
        #   end
        #
        #   puts commit_resp.timestamp
        #   puts commit_resp.stats.mutation_count
        #
        def commit transaction_id: nil, exclude_txn_from_change_streams: false,
                   isolation_level: nil, commit_options: nil, request_options: nil,
                   call_options: nil, read_lock_mode: nil
          ensure_service!
          commit = Commit.new
          yield commit

          should_retry = true
          # @type [Google::Cloud::Spanner::V1::MultiplexedSessionPrecommitToken]
          precommit_token = nil
          while should_retry
            commit_resp = service.commit(path,
                                         commit.mutations,
                                         transaction_id: transaction_id,
                                         exclude_txn_from_change_streams: exclude_txn_from_change_streams,
                                         isolation_level: isolation_level,
                                         read_lock_mode: read_lock_mode,
                                         commit_options: commit_options,
                                         request_options: request_options,
                                         call_options: call_options,
                                         precommit_token: precommit_token)

            precommit_token = commit_resp.precommit_token
            should_retry = !precommit_token.nil?
          end

          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC
          resp = CommitResponse.from_grpc commit_resp
          commit_options ? resp : resp.timestamp
        end

        ##
        # Batches the supplied mutation groups in a collection of efficient
        # transactions.
        #
        # All mutations in a group are committed atomically. However, mutations
        # across groups can be committed non-atomically in an unspecified order
        # and thus they must be independent of each other. Partial failure is
        # possible, i.e., some groups may have been committed successfully,
        # while others may have failed. The results of individual batches are
        # streamed into the response as the batches are applied.
        #
        # BatchWrite requests are not replay protected, meaning that each mutation
        # group may be applied more than once. Replays of non-idempotent mutations
        # may have undesirable effects. For example, replays of an insert mutation
        # may produce an already exists error or if you use generated or commit
        # timestamp-based keys, it may result in additional rows being added to the
        # mutation's table. We recommend structuring your mutation groups to be
        # idempotent to avoid this issue.
        #
        # @param [Boolean] exclude_txn_from_change_streams If set to true,
        #   mutations will not be recorded in change streams with DDL option
        #   `allow_txn_exclusion=true`.
        # @param [Hash] request_options Common request options.
        #
        #   * `:priority` (String) The relative priority for requests.
        #     The priority acts as a hint to the Cloud Spanner scheduler
        #     and does not guarantee priority or order of execution.
        #     Valid values are `:PRIORITY_LOW`, `:PRIORITY_MEDIUM`,
        #     `:PRIORITY_HIGH`. If priority not set then default is
        #     `PRIORITY_UNSPECIFIED` is equivalent to `:PRIORITY_HIGH`.
        #   * `:tag` (String) A per-request tag which can be applied to
        #     queries or reads, used for statistics collection. Tag must be a
        #     valid identifier of the form: `[a-zA-Z][a-zA-Z0-9_\-]` between 2
        #     and 64 characters in length.
        #
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @yield [batch_write] a batch write object
        # @yieldparam [Google::Cloud::Spanner::BatchWrite] batch_write a batch
        #   write object used to add mutaion groups through {MutationGroup}.
        #
        # @return [Google::Cloud::Spanner::BatchWriteResults] The results of
        #   the batch write operation. This is a stream of responses, each
        #   covering a set of the mutation groups that were either applied or
        #   failed together.
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
        #   results.each do |response|
        #     puts "groups applied: #{response.indexes}" if response.ok?
        #   end
        #
        def batch_write exclude_txn_from_change_streams: false,
                        request_options: nil,
                        call_options: nil
          ensure_service!
          b = BatchWrite.new
          yield b
          response = service.batch_write path, b.mutation_groups_grpc,
                                         exclude_txn_from_change_streams: exclude_txn_from_change_streams,
                                         request_options: request_options,
                                         call_options: call_options
          results = BatchWriteResults.new response
          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC
          results
        end

        ##
        # Inserts or updates rows in a table. If any of the rows already exist,
        # then its column values are overwritten with the ones provided. Any
        # column values not explicitly written are preserved.
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
        # @param [String] transaction_id The identifier of previously-started
        #   transaction to be used instead of starting a new transaction.
        #   Optional.
        # @param [Boolean] exclude_txn_from_change_streams If set to true,
        #   mutations will not be recorded in change streams with DDL option
        #   `allow_txn_exclusion=true`. Used if starting a new transaction.
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::IsolationLevel] isolation_level Optional. The
        #   isolation level for the transaction.
        # @param [Hash] commit_options A hash of commit options.
        #   e.g., return_commit_stats. Commit options are optional.
        #   The following options can be provided:
        #
        #   * `:return_commit_stats` (Boolean) A boolean value. If `true`,
        #     then statistics related to the transaction will be included in
        #     {CommitResponse}. Default value is `false`
        #   *  `:maxCommitDelay` (Numeric) The amount of latency in millisecond in this request
        #         is willing to incur in order to improve throughput.
        #         The commit delay must be at least 0ms and at most 500ms.
        #         Default value is nil.
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite::ReadLockMode] read_lock_mode
        #   The read lock mode for the transaction.
        #   Can be one of the following:
        #   * `:READ_LOCK_MODE_UNSPECIFIED` (0): The default unspecified read lock mode.
        #   * `:PESSIMISTIC` (1): The pessimistic lock mode, where read locks are acquired immediately on read.
        #   * `:OPTIMISTIC` (2): The optimistic lock mode, where locks for reads are not acquired on read
        #       but instead on a commit to validate that the data has not changed since the transaction started.
        #
        #
        # @return [Time, CommitResponse] The timestamp at which the operation
        #   committed. If commit options are set it returns {CommitResponse}.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   db.upsert "users", [{ id: 1, name: "Charlie", active: false },
        #                       { id: 2, name: "Harvey",  active: true }]
        #
        # @example Get commit stats
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   records = [{ id: 1, name: "Charlie", active: false },
        #             { id: 2, name: "Harvey",  active: true }]
        #   commit_options = { return_commit_stats: true }
        #   commit_resp = db.upsert "users", records, commit_options: commit_options
        #
        #   puts commit_resp.timestamp
        #   puts commit_resp.stats.mutation_count
        #
        def upsert table, *rows,
                   transaction_id: nil, exclude_txn_from_change_streams: false,
                   isolation_level: nil, commit_options: nil, request_options: nil,
                   call_options: nil, read_lock_mode: nil
          opts = {
            transaction_id: transaction_id,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams,
            isolation_level: isolation_level,
            commit_options: commit_options,
            request_options: request_options,
            call_options: call_options,
            read_lock_mode: read_lock_mode
          }
          commit(**opts) do |c|
            c.upsert table, rows
          end
        end
        alias save upsert

        ##
        # Inserts new rows in a table. If any of the rows already exist, the
        # write or request fails with error `ALREADY_EXISTS`.
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
        # @param [String] transaction_id The identifier of previously-started
        #   transaction to be used instead of starting a new transaction.
        #   Optional.
        # @param [Boolean] exclude_txn_from_change_streams If set to true,
        #   mutations will not be recorded in change streams with DDL option
        #   `allow_txn_exclusion=true`. Used if starting a new transaction.
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::IsolationLevel] isolation_level Optional. The
        #   isolation level for the transaction.
        # @param [Hash] commit_options A hash of commit options.
        #   e.g., return_commit_stats. Commit options are optional.
        #   The following options can be provided:
        #
        #   * `:return_commit_stats` (Boolean) A boolean value. If `true`,
        #     then statistics related to the transaction will be included in
        #     {CommitResponse}. Default value is `false`
        #   *  `:maxCommitDelay` (Numeric) The amount of latency in millisecond in this request
        #         is willing to incur in order to improve throughput.
        #         The commit delay must be at least 0ms and at most 500ms.
        #         Default value is nil.
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite::ReadLockMode] read_lock_mode
        #   The read lock mode for the transaction.
        #   Can be one of the following:
        #   * `:READ_LOCK_MODE_UNSPECIFIED` (0): The default unspecified read lock mode.
        #   * `:PESSIMISTIC` (1): The pessimistic lock mode, where read locks are acquired immediately on read.
        #   * `:OPTIMISTIC` (2): The optimistic lock mode, where locks for reads are not acquired on read
        #       but instead on a commit to validate that the data has not changed since the transaction started.
        #
        #
        # @return [Time, CommitResponse] The timestamp at which the operation
        #   committed. If commit options are set it returns {CommitResponse}.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   db.insert "users", [{ id: 1, name: "Charlie", active: false },
        #                       { id: 2, name: "Harvey",  active: true }]
        #
        # @example Get commit stats
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   records = [{ id: 1, name: "Charlie", active: false },
        #              { id: 2, name: "Harvey",  active: true }]
        #   commit_options = { return_commit_stats: true }
        #   commit_resp = db.insert "users", records, commit_options: commit_options
        #
        #   puts commit_resp.timestamp
        #   puts commit_resp.stats.mutation_count
        #
        def insert table, *rows,
                   transaction_id: nil, exclude_txn_from_change_streams: false,
                   isolation_level: nil, commit_options: nil, request_options: nil,
                   call_options: nil, read_lock_mode: nil
          opts = {
            transaction_id: transaction_id,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams,
            isolation_level: isolation_level,
            commit_options: commit_options,
            request_options: request_options,
            call_options: call_options,
            read_lock_mode: read_lock_mode
          }
          commit(**opts) do |c|
            c.insert table, rows
          end
        end

        ##
        # Updates existing rows in a table. If any of the rows does not already
        # exist, the request fails with error `NOT_FOUND`.
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
        # @param [String] transaction_id The identifier of previously-started
        #   transaction to be used instead of starting a new transaction.
        #   Optional.
        # @param [Boolean] exclude_txn_from_change_streams If set to true,
        #   mutations will not be recorded in change streams with DDL option
        #   `allow_txn_exclusion=true`. Used if starting a new transaction.
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::IsolationLevel] isolation_level Optional. The
        #   isolation level for the transaction.
        # @param [Hash] commit_options A hash of commit options.
        #   e.g., return_commit_stats. Commit options are optional.
        #   The following options can be provided:
        #
        #   * `:return_commit_stats` (Boolean) A boolean value. If `true`,
        #     then statistics related to the transaction will be included in
        #     {CommitResponse}. Default value is `false`
        #   *  `:maxCommitDelay` (Numeric) The amount of latency in millisecond in this request
        #         is willing to incur in order to improve throughput.
        #         The commit delay must be at least 0ms and at most 500ms.
        #         Default value is nil.
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite::ReadLockMode] read_lock_mode
        #   The read lock mode for the transaction.
        #   Can be one of the following:
        #   * `:READ_LOCK_MODE_UNSPECIFIED` (0): The default unspecified read lock mode.
        #   * `:PESSIMISTIC` (1): The pessimistic lock mode, where read locks are acquired immediately on read.
        #   * `:OPTIMISTIC` (2): The optimistic lock mode, where locks for reads are not acquired on read
        #       but instead on a commit to validate that the data has not changed since the transaction started.
        #
        #
        # @return [Time, CommitResponse] The timestamp at which the operation
        #   committed. If commit options are set it returns {CommitResponse}.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   db.update "users", [{ id: 1, name: "Charlie", active: false },
        #                       { id: 2, name: "Harvey",  active: true }]
        #
        # @example Get commit stats
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   records = [{ id: 1, name: "Charlie", active: false },
        #              { id: 2, name: "Harvey",  active: true }]
        #   commit_options = { return_commit_stats: true  }
        #   commit_resp = db.update "users", records, commit_options: commit_options
        #
        #   puts commit_resp.timestamp
        #   puts commit_resp.stats.mutation_count
        #
        def update table, *rows,
                   transaction_id: nil, exclude_txn_from_change_streams: false,
                   isolation_level: nil, commit_options: nil, request_options: nil,
                   call_options: nil, read_lock_mode: nil
          opts = {
            transaction_id: transaction_id,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams,
            isolation_level: isolation_level,
            commit_options: commit_options,
            request_options: request_options,
            call_options: call_options,
            read_lock_mode: read_lock_mode
          }
          commit(**opts) do |c|
            c.update table, rows
          end
        end

        ##
        # Inserts or replaces rows in a table. If any of the rows already exist,
        # it is deleted, and the column values provided are inserted instead.
        # Unlike #upsert, this means any values not explicitly written become
        # `NULL`.
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
        # @param [String] transaction_id The identifier of previously-started
        #   transaction to be used instead of starting a new transaction.
        #   Optional.
        # @param [Boolean] exclude_txn_from_change_streams If set to true,
        #   mutations will not be recorded in change streams with DDL option
        #   `allow_txn_exclusion=true`. Used if starting a new transaction.
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::IsolationLevel] isolation_level Optional. The
        #   isolation level for the transaction.
        # @param [Hash] commit_options A hash of commit options.
        #   e.g., return_commit_stats. Commit options are optional.
        #   The following options can be provided:
        #
        #   * `:return_commit_stats` (Boolean) A boolean value. If `true`,
        #     then statistics related to the transaction will be included in
        #     {CommitResponse}. Default value is `false`
        #   *  `:maxCommitDelay` (Numeric) The amount of latency in millisecond in this request
        #         is willing to incur in order to improve throughput.
        #         The commit delay must be at least 0ms and at most 500ms.
        #         Default value is nil.
        #
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite::ReadLockMode] read_lock_mode
        #   The read lock mode for the transaction.
        #   Can be one of the following:
        #   * `:READ_LOCK_MODE_UNSPECIFIED` (0): The default unspecified read lock mode.
        #   * `:PESSIMISTIC` (1): The pessimistic lock mode, where read locks are acquired immediately on read.
        #   * `:OPTIMISTIC` (2): The optimistic lock mode, where locks for reads are not acquired on read
        #       but instead on a commit to validate that the data has not changed since the transaction started.
        #
        #
        # @return [Time, CommitResponse] The timestamp at which the operation
        #   committed. If commit options are set it returns {CommitResponse}.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   db.replace "users", [{ id: 1, name: "Charlie", active: false },
        #                        { id: 2, name: "Harvey",  active: true }]
        #
        # @example Get commit stats
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   records = [{ id: 1, name: "Charlie", active: false },
        #              { id: 2, name: "Harvey",  active: true }]
        #   commit_options = { return_commit_stats: true  }
        #   commit_resp = db.replace "users", records, commit_options: commit_options
        #
        #   puts commit_resp.timestamp
        #   puts commit_resp.stats.mutation_count
        #
        def replace table, *rows,
                    transaction_id: nil, exclude_txn_from_change_streams: false,
                    isolation_level: nil, commit_options: nil, request_options: nil,
                    call_options: nil, read_lock_mode: nil
          opts = {
            transaction_id: transaction_id,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams,
            isolation_level: isolation_level,
            commit_options: commit_options,
            request_options: request_options,
            call_options: call_options,
            read_lock_mode: read_lock_mode
          }
          commit(**opts) do |c|
            c.replace table, rows
          end
        end

        ##
        # Deletes rows from a table. Succeeds whether or not the specified rows
        # were present.
        #
        # @param [String] table The name of the table in the database to be
        #   modified.
        # @param [Object, Array<Object>] keys A single, or list of keys or key
        #   ranges to match returned data to. Values should have exactly as many
        #   elements as there are columns in the primary key.
        # @param [String] transaction_id The identifier of previously-started
        #   transaction to be used instead of starting a new transaction.
        #   Optional.
        # @param [Boolean] exclude_txn_from_change_streams If set to true,
        #   mutations will not be recorded in change streams with DDL option
        #   `allow_txn_exclusion=true`. Used if starting a new transaction.
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::IsolationLevel] isolation_level Optional. The
        #   isolation level for the transaction.
        # @param [Hash] commit_options A hash of commit options.
        #   e.g., return_commit_stats. Commit options are optional.
        #   The following options can be provided:
        #
        #   * `:return_commit_stats` (Boolean) A boolean value. If `true`,
        #     then statistics related to the transaction will be included in
        #     {CommitResponse}. Default value is `false`
        #   *  `:maxCommitDelay` (Numeric) The amount of latency in millisecond in this request
        #         is willing to incur in order to improve throughput.
        #         The commit delay must be at least 0ms and at most 500ms.
        #         Default value is nil.
        # @param [Hash] request_options Common request options.
        #
        #   * `:request_tag` (String) A per-request tag which can be applied
        #     to queries or reads, used for statistics collection. Both
        #     request_tag and transaction_tag can be specified for a read or
        #     query that belongs to a transaction. This field is ignored for
        #     requests where it's not applicable (e.g. CommitRequest).
        #     `request_tag` must be a valid identifier of the form:
        #     `[a-zA-Z][a-zA-Z0-9_\-]` between 2 and 64 characters in length.
        #   * `:transaction_tag` (String) A tag used for statistics collection
        #     about this transaction. Both request_tag and transaction_tag can
        #     be specified for a read or query that belongs to a transaction.
        #     The value of transaction_tag should be the same for all requests
        #     belonging to the same transaction. If this request doesn't belong
        #     to any transaction, transaction_tag will be ignored.
        #     `transaction_tag` must be a valid identifier of the format:
        #     [a-zA-Z][a-zA-Z0-9_\-]{0,49}
        # @param [Hash] call_options A hash of values to specify the custom
        #   call options, e.g., timeout, retries, etc. Call options are
        #   optional. The following settings can be provided:
        #
        #   * `:timeout` (Numeric) A numeric value of custom timeout in seconds
        #     that overrides the default setting.
        #   * `:retry_policy` (Hash) A hash of values that overrides the default
        #     setting of retry policy with the following keys:
        #     * `:initial_delay` (`Numeric`) - The initial delay in seconds.
        #     * `:max_delay` (`Numeric`) - The max delay in seconds.
        #     * `:multiplier` (`Numeric`) - The incremental backoff multiplier.
        #     * `:retry_codes` (`Array<String>`) - The error codes that should
        #       trigger a retry.
        #
        # @param [Google::Cloud::Spanner::V1::TransactionOptions::ReadWrite::ReadLockMode] read_lock_mode
        #   The read lock mode for the transaction.
        #   Can be one of the following:
        #   * `:READ_LOCK_MODE_UNSPECIFIED` (0): The default unspecified read lock mode.
        #   * `:PESSIMISTIC` (1): The pessimistic lock mode, where read locks are acquired immediately on read.
        #   * `:OPTIMISTIC` (2): The optimistic lock mode, where locks for reads are not acquired on read
        #       but instead on a commit to validate that the data has not changed since the transaction started.
        #
        #
        # @return [Time, CommitResponse] The timestamp at which the operation
        #   committed. If commit options are set it returns {CommitResponse}.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   db.delete "users", [1, 2, 3]
        #
        # @example Get commit stats
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   commit_options = { return_commit_stats: true }
        #   commit_resp = db.delete "users", [1,2,3], commit_options: commit_options
        #
        #   puts commit_resp.timestamp
        #   puts commit_resp.stats.mutation_count
        #
        def delete table, keys = [],
                   transaction_id: nil, exclude_txn_from_change_streams: false,
                   isolation_level: nil, commit_options: nil, request_options: nil,
                   call_options: nil, read_lock_mode: nil
          opts = {
            transaction_id: transaction_id,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams,
            isolation_level: isolation_level,
            commit_options: commit_options,
            request_options: request_options,
            call_options: call_options,
            read_lock_mode: read_lock_mode
          }
          commit(**opts) do |c|
            c.delete table, keys
          end
        end

        ##
        # Rolls back the transaction, releasing any locks it holds.
        def rollback transaction_id
          service.rollback path, transaction_id
          @last_updated_at = Process.clock_gettime Process::CLOCK_MONOTONIC
          true
        end

        # Explicitly begins a new transaction and creates a server-side transaction object.
        # Unlike {#create_empty_transaction}, this method makes an immediate
        # `BeginTransaction` RPC call.
        #
        # @param exclude_txn_from_change_streams [::Boolean] Optional. Defaults to `false`.
        #   When `exclude_txn_from_change_streams` is set to `true`, it prevents read
        #   or write transactions from being tracked in change streams.
        # @private
        # @return [::Google::Cloud::Spanner::Transaction]
        def create_transaction exclude_txn_from_change_streams: false, read_lock_mode: nil
          route_to_leader = LARHeaders.begin_transaction true
          tx_grpc = service.begin_transaction path,
                                              route_to_leader: route_to_leader,
                                              exclude_txn_from_change_streams: exclude_txn_from_change_streams,
                                              read_lock_mode: read_lock_mode
          Transaction.from_grpc \
            tx_grpc, self,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams, read_lock_mode: read_lock_mode
        end

        # Creates a new empty transaction wrapper without a server-side object.
        # This is used for inline-begin transactions and does not make an RPC call.
        # See {#create_transaction} for the RPC-based method.
        #
        # @param exclude_txn_from_change_streams [::Boolean] Optional. Defaults to `false`.
        #   When `exclude_txn_from_change_streams` is set to `true`, it prevents read
        #   or write transactions from being tracked in change streams.
        # @param previous_transaction_id [::String, nil] Optional.
        #   An id of the previous transaction, if this new transaction wrapper is being created
        #   as a part of a retry. Previous transaction id should be added to TransactionOptions
        #   of a new ReadWrite transaction when retry is attempted.
        # @private
        # @return [::Google::Cloud::Spanner::Transaction] The new *empty-wrapper* transaction object.
        def create_empty_transaction exclude_txn_from_change_streams: false, previous_transaction_id: nil, read_lock_mode: nil
          Transaction.from_grpc nil, self, exclude_txn_from_change_streams: exclude_txn_from_change_streams,
previous_transaction_id: previous_transaction_id, read_lock_mode: read_lock_mode
        end

        # If the session is non-multiplexed, keeps the session alive by executing `"SELECT 1"`.
        # This method will re-create the session if necessary.
        # For multiplexed session the keepalive is not required and this method immediately returns `true`.
        # @private
        # @return [::Boolean]
        #   `true` if the session is multiplexed or if the keepalive was successful for non-multiplexed session,
        #   `false` if the non-multiplexed session was not found and the had to be recreated.
        def keepalive!
          return true if multiplexed?

          ensure_service!
          route_to_leader = LARHeaders.execute_query false
          execute_query "SELECT 1", route_to_leader: route_to_leader
          true
        rescue Google::Cloud::NotFoundError
          labels = @grpc.labels.to_h unless @grpc.labels.to_h.empty?
          @grpc = service.create_session \
            V1::Spanner::Paths.database_path(
              project: project_id, instance: instance_id, database: database_id
            ),
            labels: labels
          false
        end

        # Permanently deletes the session unless this session is multiplexed.
        # Multiplexed sessions can not be deleted, and this method immediately returns.
        # @private
        # @return [void]
        def release!
          return if multiplexed?
          ensure_service!
          service.delete_session path
        end

        # Determines if the session has been idle longer than the given
        # duration in seconds.
        #
        # @param duration_sec [::Numeric] interval in seconds
        # @private
        # @return [::Boolean]
        def idle_since? duration_sec
          return true if @last_updated_at.nil?
          Process.clock_gettime(Process::CLOCK_MONOTONIC) > @last_updated_at + duration_sec
        end

        # Determines if the session did exist for at least the given
        # duration in seconds.
        #
        # @param duration_sec [::Numeric] interval in seconds
        # @private
        # @return [::Boolean]
        def existed_since? duration_sec
          Process.clock_gettime(Process::CLOCK_MONOTONIC) > @created_time + duration_sec
        end

        # Creates a new `Spanner::Session` instance from a `V1::Session` object.
        # @param grpc [::Google::Cloud::Spanner::V1::Session] Underlying `V1::Session` object.
        # @param service [::Google::Cloud::Spanner::Service] A `Spanner::Service` ref.
        # @param query_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   query options for executing SQL query. Example option: `:optimizer_version`.
        # @private
        # @return [::Google::Cloud::Spanner::Session]
        def self.from_grpc grpc, service, query_options: nil
          new grpc, service, query_options: query_options
        end

        ##
        # @private
        def session
          self
        end

        protected

        # Whether this session is multiplexed.
        # @private
        # @return [::Boolean]
        def multiplexed?
          @grpc.multiplexed
        end

        ##
        # @private Raise an error unless an active connection to the service is
        # available.
        def ensure_service!
          raise "Must have active connection to service" unless service
        end

        # Merge two hashes
        # @param hash [::Hash, nil]
        # @param hash_to_merge [::Hash, nil]
        # @private
        # @return [::Hash, nil]
        def merge_if_present hash, hash_to_merge
          if hash.nil?
            hash_to_merge
          else
            hash_to_merge.nil? ? hash : hash_to_merge.merge(hash)
          end
        end
      end
    end
  end
end
