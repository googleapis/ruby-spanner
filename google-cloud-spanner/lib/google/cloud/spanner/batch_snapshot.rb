# Copyright 2018 Google LLC
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


require "google/cloud/spanner/convert"
require "google/cloud/spanner/session"
require "google/cloud/spanner/partition"
require "google/cloud/spanner/results"
require "json"
require "base64"

module Google
  module Cloud
    module Spanner
      ##
      # # BatchSnapshot
      #
      # Represents a read-only transaction that can be configured to read at
      # timestamps in the past and allows for exporting arbitrarily large
      # amounts of data from Cloud Spanner databases. This is a snapshot which
      # additionally allows to partition a read or query request. The read/query
      # request can then be executed independently over each partition while
      # observing the same snapshot of the database. A BatchSnapshot can also be
      # shared across multiple processes/machines by passing around its
      # serialized value and then recreating the transaction using {#dump}.
      #
      # Unlike locking read-write transactions, BatchSnapshot will never abort.
      # They can fail if the chosen read timestamp is garbage collected; however
      # any read or query activity within an hour on the transaction avoids
      # garbage collection and most applications do not need to worry about this
      # in practice.
      #
      # See {BatchClient#batch_snapshot} and {BatchClient#load_batch_snapshot}.
      #
      # @example
      #   require "google/cloud/spanner"
      #
      #   spanner = Google::Cloud::Spanner.new
      #
      #   batch_client = spanner.batch_client "my-instance", "my-database"
      #   batch_snapshot = batch_client.batch_snapshot
      #
      #   partitions = batch_snapshot.partition_read "users", [:id, :name]
      #
      #   partition = partitions.first
      #   results = batch_snapshot.execute_partition partition
      #
      #   batch_snapshot.close
      #
      class BatchSnapshot
        # @private The transaction grpc object.
        attr_reader :grpc

        # @private The Session object.
        attr_reader :session

        # @private Directed Read Options
        attr_reader :directed_read_options

        ##
        # @private Creates a BatchSnapshot object.
        def initialize grpc, session, directed_read_options: nil
          @grpc = grpc
          @session = session
          @directed_read_options = directed_read_options
        end

        ##
        # Identifier of the batch snapshot transaction.
        # @return [String] The transaction id.
        def transaction_id
          return nil if grpc.nil?
          grpc.id
        end

        ##
        # The read timestamp chosen for batch snapshot.
        # @return [Time] The chosen timestamp.
        def timestamp
          return nil if grpc.nil?
          Convert.timestamp_to_time grpc.read_timestamp
        end

        ##
        # Returns a list of {Partition} objects to execute a batch query against
        # a database.
        #
        # These partitions can be executed across multiple processes, even
        # across different machines. The partition size and count can be
        # configured, although the values given may not necessarily be honored
        # depending on the query and options in the request.
        #
        # The query must have a single [distributed
        # union](https://cloud.google.com/spanner/docs/query-execution-operators#distributed_union)
        # operator at the root of the query plan. Such queries are
        # root-partitionable. If a query cannot be partitioned at the root,
        # Cloud Spanner cannot achieve the parallelism and in this case
        # partition generation will fail.
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
        #   | `STRING`    | `String`       | |
        #   | `DATE`      | `Date`         | |
        #   | `TIMESTAMP` | `Time`, `DateTime` | |
        #   | `BYTES`     | `File`, `IO`, `StringIO`, or similar | |
        #   | `ARRAY`     | `Array` | Nested arrays are not supported. |
        #   | `STRUCT`    | `Hash`, {Data} | |
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
        # @param [Integer] partition_size_bytes The desired data size for each
        #   partition generated. This is only a hint. The actual size of each
        #   partition may be smaller or larger than this size request.
        # @param [Integer] max_partitions The desired maximum number of
        #   partitions to return. For example, this may be set to the number of
        #   workers available. This is only a hint and may provide different
        #   results based on the request.
        # @param [Hash] query_options A hash of values to specify the custom
        #   query options for executing SQL query. Query options are optional.
        #   The following settings can be provided:
        #
        #   * `:optimizer_version` (String) The version of optimizer to use.
        #     Empty to use database default. "latest" to use the latest
        #     available optimizer version.
        #   * `:optimizer_statistics_package` (String) Statistics package to
        #     use. Empty to use the database default.
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
        # @param [Boolean] data_boost_enabled  If this field is
        #   set `true`, the request will be executed via offline access.
        #   Defaults to `false`.
        # @param [Hash]  directed_read_options Client options used to set the directed_read_options
        #    for all ReadRequests and ExecuteSqlRequests that indicates which replicas
        #    or regions should be used for non-transactional reads or queries.
        #    Will represent [`Google::Cloud::Spanner::V1::DirectedReadOptions`](https://cloud.google.com/ruby/docs/reference/google-cloud-spanner-v1/latest/Google-Cloud-Spanner-V1-DirectedReadOptions)
        #   The following settings can be provided:
        #
        #   * `:exclude_replicas` (Hash)
        #      Exclude_replicas indicates what replicas should be excluded from serving requests.
        #      Spanner will not route requests to the replicas in this list.
        #   * `:include_replicas` (Hash) Include_replicas indicates the order of replicas to process the request.
        #      If auto_failover_disabled is set to true and
        #      all replicas are exhausted without finding a healthy replica,
        #      Spanner will wait for a replica in the list to become available,
        #      requests may fail due to DEADLINE_EXCEEDED errors.
        #
        # @return [Array<Google::Cloud::Spanner::Partition>] The partitions
        #   created by the query partition.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   sql = "SELECT u.id, u.active FROM users AS u \
        #          WHERE u.id < 2000 AND u.active = false"
        #   partitions = batch_snapshot.partition_query sql
        #
        #   partition = partitions.first
        #   results = batch_snapshot.execute_partition partition
        #
        #   batch_snapshot.close
        #
        def partition_query sql, params: nil, types: nil,
                            partition_size_bytes: nil, max_partitions: nil,
                            query_options: nil, call_options: nil, data_boost_enabled: false,
                            directed_read_options: nil
          ensure_session!

          params, types = Convert.to_input_params_and_types params, types

          results = session.partition_query \
            sql, tx_selector, params: params, types: types,
                              partition_size_bytes: partition_size_bytes,
                              max_partitions: max_partitions,
                              call_options: call_options
          results.partitions.map do |grpc|
            # Convert partition protos to execute sql request protos
            execute_sql_grpc = V1::ExecuteSqlRequest.new(
              {
                session: session.path,
                sql: sql,
                params: params,
                param_types: types,
                transaction: tx_selector,
                partition_token: grpc.partition_token,
                query_options: query_options,
                data_boost_enabled: data_boost_enabled,
                directed_read_options: directed_read_options || @directed_read_options
              }.compact
            )
            Partition.from_execute_sql_grpc execute_sql_grpc
          end
        end

        ##
        # Returns a list of {Partition} objects to read zero or more rows from a
        # database.
        #
        # These partitions can be executed across multiple processes, even
        # across different machines. The partition size and count can be
        # configured, although the values given may not necessarily be honored
        # depending on the query and options in the request.
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
        # @param [Integer] partition_size_bytes The desired data size for each
        #   partition generated. This is only a hint. The actual size of each
        #   partition may be smaller or larger than this size request.
        # @param [Integer] max_partitions The desired maximum number of
        #   partitions to return. For example, this may be set to the number of
        #   workers available. This is only a hint and may provide different
        #   results based on the request.
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
        # @param [Boolean] data_boost_enabled  If this field is
        #   set `true`, the request will be executed via offline access.
        #   Defaults to `false`.
        # @param [Hash]  directed_read_options Client options used to set the directed_read_options
        #    for all ReadRequests and ExecuteSqlRequests that indicates which replicas
        #    or regions should be used for non-transactional reads or queries.
        #    Will represent [`Google::Cloud::Spanner::V1::DirectedReadOptions`](https://cloud.google.com/ruby/docs/reference/google-cloud-spanner-v1/latest/Google-Cloud-Spanner-V1-DirectedReadOptions)
        #   The following settings can be provided:
        #
        #   * `:exclude_replicas` (Hash)
        #      Exclude_replicas indicates what replicas should be excluded from serving requests.
        #      Spanner will not route requests to the replicas in this list.
        #   * `:include_replicas` (Hash) Include_replicas indicates the order of replicas to process the request.
        #      If auto_failover_disabled is set to true and
        #      all replicas are exhausted without finding a healthy replica,
        #      Spanner will wait for a replica in the list to become available,
        #      requests may fail due to DEADLINE_EXCEEDED errors.
        #
        # @return [Array<Google::Cloud::Spanner::Partition>] The partitions
        #   created by the read partition.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   partitions = batch_snapshot.partition_read "users", [:id, :name]
        #
        #   partition = partitions.first
        #   results = batch_snapshot.execute_partition partition
        #
        #   batch_snapshot.close
        #
        def partition_read table, columns, keys: nil, index: nil,
                           partition_size_bytes: nil, max_partitions: nil,
                           call_options: nil, data_boost_enabled: false,
                           directed_read_options: nil
          ensure_session!

          columns = Array(columns).map(&:to_s)
          keys = Convert.to_key_set keys

          results = session.partition_read \
            table, columns, tx_selector,
            keys: keys, index: index,
            partition_size_bytes: partition_size_bytes,
            max_partitions: max_partitions,
            call_options: call_options

          results.partitions.map do |grpc|
            # Convert partition protos to read request protos
            read_grpc = V1::ReadRequest.new(
              {
                session: session.path,
                table: table,
                columns: columns,
                key_set: keys,
                index: index,
                transaction: tx_selector,
                partition_token: grpc.partition_token,
                data_boost_enabled: data_boost_enabled,
                directed_read_options: directed_read_options || @directed_read_options
              }.compact
            )
            Partition.from_read_grpc read_grpc
          end
        end

        ##
        # Execute the partition to return a {Results}. The result returned
        # could be zero or more rows. The row metadata may be absent if no rows
        # are returned.
        #
        # @param [Google::Cloud::Spanner::Partition] partition The partition to
        #   be executed.
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
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   partitions = batch_snapshot.partition_read "users", [:id, :name]
        #
        #   partition = partitions.first
        #   results = batch_snapshot.execute_partition partition
        #
        #   batch_snapshot.close
        #
        def execute_partition partition, call_options: nil
          ensure_session!

          partition = Partition.load partition unless partition.is_a? Partition
          # TODO: raise if partition.empty?

          # TODO: raise if session.path != partition.session
          # TODO: raise if grpc.transaction != partition.transaction

          opts = { call_options: call_options }
          if partition.execute?
            execute_partition_query partition, **opts
          elsif partition.read?
            execute_partition_read partition, **opts
          end
        end

        ##
        # Closes the batch snapshot and releases the underlying resources.
        #
        # This should only be called once the batch snapshot is no longer needed
        # anywhere. In particular if this batch snapshot is being used across
        # multiple machines, calling this method on any of the machines will
        # render the batch snapshot invalid everywhere.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   partitions = batch_snapshot.partition_read "users", [:id, :name]
        #
        #   partition = partitions.first
        #   results = batch_snapshot.execute_partition partition
        #
        #   batch_snapshot.close
        #
        def close
          ensure_session!

          session.release!
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
        #   | `STRING`    | `String`       | |
        #   | `DATE`      | `Date`         | |
        #   | `TIMESTAMP` | `Time`, `DateTime` | |
        #   | `BYTES`     | `File`, `IO`, `StringIO`, or similar | |
        #   | `ARRAY`     | `Array` | Nested arrays are not supported. |
        #   | `STRUCT`    | `Hash`, {Data} | |
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
        # @param [Hash] query_options A hash of values to specify the custom
        #   query options for executing SQL query. Query options are optional.
        #   The following settings can be provided:
        #
        #   * `:optimizer_version` (String) The version of optimizer to use.
        #     Empty to use database default. "latest" to use the latest
        #     available optimizer version.
        #   * `:optimizer_statistics_package` (String) Statistics package to
        #     use. Empty to use the database default.
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
        # @param [Hash]  directed_read_options Client options used to set the directed_read_options
        #    for all ReadRequests and ExecuteSqlRequests that indicates which replicas
        #    or regions should be used for non-transactional reads or queries.
        #    Will represent [`Google::Cloud::Spanner::V1::DirectedReadOptions`](https://cloud.google.com/ruby/docs/reference/google-cloud-spanner-v1/latest/Google-Cloud-Spanner-V1-DirectedReadOptions)
        #   The following settings can be provided:
        #
        #   * `:exclude_replicas` (Hash)
        #      Exclude_replicas indicates what replicas should be excluded from serving requests.
        #      Spanner will not route requests to the replicas in this list.
        #   * `:include_replicas` (Hash) Include_replicas indicates the order of replicas to process the request.
        #      If auto_failover_disabled is set to true and
        #      all replicas are exhausted without finding a healthy replica,
        #      Spanner will wait for a replica in the list to become available,
        #      requests may fail due to DEADLINE_EXCEEDED errors.
        #
        # @return [Google::Cloud::Spanner::Results] The results of the query
        #   execution.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   results = batch_snapshot.execute_query "SELECT * FROM users"
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        # @example Query using query parameters:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   results = batch_snapshot.execute_query(
        #      "SELECT * FROM users " \
        #      "WHERE active = @active",
        #      params: { active: true }
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
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   user_hash = { id: 1, name: "Charlie", active: false }
        #
        #   results = batch_snapshot.execute_query(
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
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   user_type = batch_client.fields(
        #     { id: :INT64, name: :STRING, active: :BOOL }
        #   )
        #   user_hash = { id: 1, name: nil, active: false }
        #
        #   results = batch_snapshot.execute_query(
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
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   user_type = batch_client.fields(
        #     { id: :INT64, name: :STRING, active: :BOOL }
        #   )
        #   user_data = user_type.struct id: 1, name: nil, active: false
        #
        #   results = batch_snapshot.execute_query(
        #     "SELECT * FROM users WHERE " \
        #     "ID = @user_struct.id " \
        #     "AND name = @user_struct.name " \
        #     "AND active = @user_struct.active",
        #     params: { user_struct: user_data }
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
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   results = batch_snapshot.execute_query \
        #     "SELECT * FROM users",
        #     query_options: {
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
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
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
        #   results = batch_snapshot.execute_query \
        #      "SELECT * FROM users",
        #      call_options: call_options
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        def execute_query sql, params: nil, types: nil, query_options: nil,
                          call_options: nil, directed_read_options: nil
          ensure_session!

          params, types = Convert.to_input_params_and_types params, types

          session.execute_query sql, params: params, types: types,
                                transaction: tx_selector,
                                query_options: query_options,
                                call_options: call_options,
                                directed_read_options: directed_read_options || @directed_read_options
        end
        alias execute execute_query
        alias query execute_query
        alias execute_sql execute_query

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
        # @param [Hash]  directed_read_options Client options used to set the directed_read_options
        #    for all ReadRequests and ExecuteSqlRequests that indicates which replicas
        #    or regions should be used for non-transactional reads or queries.
        #    Will represent [`Google::Cloud::Spanner::V1::DirectedReadOptions`](https://cloud.google.com/ruby/docs/reference/google-cloud-spanner-v1/latest/Google-Cloud-Spanner-V1-DirectedReadOptions)
        #   The following settings can be provided:
        #
        #   * `:exclude_replicas` (Hash)
        #      Exclude_replicas indicates what replicas should be excluded from serving requests.
        #      Spanner will not route requests to the replicas in this list.
        #   * `:include_replicas` (Hash) Include_replicas indicates the order of replicas to process the request.
        #      If auto_failover_disabled is set to true and
        #      all replicas are exhausted without finding a healthy replica,
        #      Spanner will wait for a replica in the list to become available,
        #      requests may fail due to DEADLINE_EXCEEDED errors.
        #
        # @return [Google::Cloud::Spanner::Results] The results of the read
        #   operation.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   results = batch_snapshot.read "users", [:id, :name]
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        def read table, columns, keys: nil, index: nil, limit: nil,
                 call_options: nil, directed_read_options: nil
          ensure_session!

          columns = Array(columns).map(&:to_s)
          keys = Convert.to_key_set keys

          session.read table, columns, keys: keys, index: index, limit: limit,
                                       transaction: tx_selector,
                                       call_options: call_options,
                                       directed_read_options: directed_read_options || @directed_read_options
        end

        ##
        # @private
        # Converts the the batch snapshot object to a Hash ready for
        # serialization.
        #
        # @return [Hash] A hash containing a representation of the batch
        #   snapshot object.
        #
        def to_h
          {
            session: Base64.strict_encode64(@session.grpc.to_proto),
            transaction: Base64.strict_encode64(@grpc.to_proto)
          }
        end

        ##
        # Serializes the batch snapshot object so it can be recreated on another
        # process. See {BatchClient#load_batch_snapshot}.
        #
        # @return [String] The serialized representation of the batch snapshot.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   partitions = batch_snapshot.partition_read "users", [:id, :name]
        #
        #   partition = partitions.first
        #
        #   serialized_snapshot = batch_snapshot.dump
        #   serialized_partition = partition.dump
        #
        #   # In a separate process
        #   new_batch_snapshot = batch_client.load_batch_snapshot \
        #     serialized_snapshot
        #
        #   new_partition = batch_client.load_partition \
        #     serialized_partition
        #
        #   results = new_batch_snapshot.execute_partition \
        #     new_partition
        #
        def dump
          JSON.dump to_h
        end
        alias serialize dump

        ##
        # @private Loads the serialized batch snapshot. See
        # {BatchClient#load_batch_snapshot}.
        def self.load data, service: nil, query_options: nil
          data = JSON.parse data, symbolize_names: true unless data.is_a? Hash

          session_grpc = V1::Session.decode Base64.decode64(data[:session])
          transaction_grpc = V1::Transaction.decode Base64.decode64(data[:transaction])

          from_grpc transaction_grpc, Session.from_grpc(session_grpc, service, query_options: query_options)
        end

        ##
        # @private Creates a new BatchSnapshot instance from a
        # `Google::Cloud::Spanner::V1::Transaction`.
        def self.from_grpc grpc, session, directed_read_options: nil
          new grpc, session, directed_read_options: directed_read_options
        end

        protected

        # The TransactionSelector to be used for queries
        def tx_selector
          V1::TransactionSelector.new id: transaction_id
        end

        ##
        # @private Raise an error unless an active connection to the service is
        # available.
        def ensure_session!
          raise "Must have active connection to service" unless session
        end

        def execute_partition_query partition, call_options: nil
          query_options = partition.execute.query_options
          query_options = query_options.to_h unless query_options.nil?
          session.execute_query \
            partition.execute.sql,
            params: partition.execute.params,
            types: partition.execute.param_types.to_h,
            transaction: partition.execute.transaction,
            partition_token: partition.execute.partition_token,
            query_options: query_options,
            call_options: call_options,
            data_boost_enabled: partition.execute.data_boost_enabled,
            directed_read_options: partition.execute.directed_read_options
        end

        def execute_partition_read partition, call_options: nil
          session.read partition.read.table,
                       partition.read.columns.to_a,
                       keys: partition.read.key_set,
                       index: partition.read.index,
                       transaction: partition.read.transaction,
                       partition_token: partition.read.partition_token,
                       call_options: call_options,
                       data_boost_enabled: partition.read.data_boost_enabled,
                       directed_read_options: partition.read.directed_read_options
        end
      end
    end
  end
end
