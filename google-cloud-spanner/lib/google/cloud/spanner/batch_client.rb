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


require "google/cloud/spanner/errors"
require "google/cloud/spanner/project"
require "google/cloud/spanner/session"
require "google/cloud/spanner/batch_snapshot"

module Google
  module Cloud
    module Spanner
      ##
      # # BatchClient
      #
      # Provides a batch client that can be used to read data from a Cloud
      # Spanner database. An instance of this class is tied to a specific
      # database.
      #
      # BatchClient is useful when one wants to read or query a large amount of
      # data from Cloud Spanner across multiple processes, even across different
      # machines. It allows to create partitions of Cloud Spanner database and
      # then read or query over each partition independently yet at the same
      # snapshot.
      #
      # See {Google::Cloud::Spanner::Project#batch_client}.
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
      class BatchClient
        ##
        # @private Creates a new Spanner BatchClient instance.
        def initialize project, instance_id, database_id, session_labels: nil,
                       query_options: nil, directed_read_options: nil
          @project = project
          @instance_id = instance_id
          @database_id = database_id
          @session_labels = session_labels
          @query_options = query_options
          @directed_read_options = directed_read_options
        end

        # The unique identifier for the project.
        # @return [String]
        def project_id
          @project.service.project
        end

        # The unique identifier for the instance.
        # @return [String]
        def instance_id
          @instance_id
        end

        # The unique identifier for the database.
        # @return [String]
        def database_id
          @database_id
        end

        # The Spanner project connected to.
        # @return [Project]
        def project
          @project
        end

        # The Spanner instance connected to.
        # @return [Instance]
        def instance
          @project.instance instance_id
        end

        # The Spanner database connected to.
        # @return [Database]
        def database
          @project.database instance_id, database_id
        end

        # A hash of values to specify the custom directed read options for executing
        # SQL query.
        # @return [Hash]
        def directed_read_options
          @directed_read_options
        end

        ##
        # Returns a {BatchSnapshot} context in which multiple reads and/or
        # queries can be performed. All reads/queries will use the same
        # timestamp, and the timestamp can be inspected after this transaction
        # is created successfully. This is a blocking method since it waits to
        # finish the RPCs.
        #
        # @param [true, false] strong Read at a timestamp where all previously
        #   committed transactions are visible.
        # @param [Time, DateTime] timestamp Executes all reads at the given
        #   timestamp. Unlike other modes, reads at a specific timestamp are
        #   repeatable; the same read at the same timestamp always returns the
        #   same data. If the timestamp is in the future, the read will block
        #   until the specified timestamp, modulo the read's deadline.
        #
        #   Useful for large scale consistent reads such as mapreduces, or for
        #   coordinating many reads against a consistent snapshot of the data.
        #   (See
        #   [TransactionOptions](https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#transactionoptions).)
        # @param [Time, DateTime] read_timestamp Same as `timestamp`.
        # @param [Numeric] staleness Executes all reads at a timestamp that is
        #   `staleness` seconds old. For example, the number 10.1 is translated
        #   to 10 seconds and 100 milliseconds.
        #
        #   Guarantees that all writes that have committed more than the
        #   specified number of seconds ago are visible. Because Cloud Spanner
        #   chooses the exact timestamp, this mode works even if the client's
        #   local clock is substantially skewed from Cloud Spanner commit
        #   timestamps.
        #
        #   Useful for reading at nearby replicas without the distributed
        #   timestamp negotiation overhead of single-use `staleness`. (See
        #   [TransactionOptions](https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#transactionoptions).)
        # @param [Numeric] exact_staleness Same as `staleness`.
        #
        # @yield [snapshot] The block for reading and writing data.
        # @yieldparam [Google::Cloud::Spanner::Snapshot] snapshot The Snapshot
        #   object.
        #
        # @return [Google::Cloud::Spanner::BatchSnapshot]
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
        def batch_snapshot strong: nil, timestamp: nil, read_timestamp: nil,
                           staleness: nil, exact_staleness: nil
          validate_snapshot_args! strong: strong, timestamp: timestamp,
                                  read_timestamp: read_timestamp,
                                  staleness: staleness,
                                  exact_staleness: exact_staleness

          ensure_service!
          snp_session = session
          snp_grpc = @project.service.create_snapshot \
            snp_session.path, strong: strong,
                              timestamp: timestamp || read_timestamp,
                              staleness: staleness || exact_staleness
          BatchSnapshot.from_grpc snp_grpc, snp_session, directed_read_options: @directed_read_options
        end

        ##
        # Returns a {BatchSnapshot} context in which multiple reads and/or
        # queries can be performed. All reads/queries will use the same
        # timestamp, and the timestamp can be inspected after this transaction
        # is created successfully. This method does not perform an RPC.
        #
        # @param [String] serialized_snapshot The serialized representation of
        #   an existing batch snapshot. See {BatchSnapshot#dump}.
        #
        # @return [Google::Cloud::Spanner::BatchSnapshot]
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
        def load_batch_snapshot serialized_snapshot
          ensure_service!

          BatchSnapshot.load serialized_snapshot, service: @project.service, query_options: @query_options
        end

        ##
        # Returns a {Partition} from a serialized representation. See
        # {Partition.load}.
        #
        # @param [String] serialized_partition The serialized representation of
        #   an existing batch partition. See {Partition#dump}.
        #
        # @return [Google::Cloud::Spanner::Partition]
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
        def load_partition serialized_partition
          Partition.load serialized_partition
        end

        ##
        # Creates a configuration object ({Fields}) that may be provided to
        # queries or used to create STRUCT objects. (The STRUCT will be
        # represented by the {Data} class.) See {Client#execute} and/or
        # {Fields#struct}.
        #
        # For more information, see [Data Types - Constructing a
        # STRUCT](https://cloud.google.com/spanner/docs/data-types#constructing-a-struct).
        #
        # @param [Array, Hash] types Accepts an array or hash types.
        #
        #   Arrays can contain just the type value, or a sub-array of the
        #   field's name and type value. Hash keys must contain the field name
        #   as a `Symbol` or `String`, or the field position as an `Integer`.
        #   Hash values must contain the type value. If a Hash is used the
        #   fields will be created using the same order as the Hash keys.
        #
        #   Supported type values incude:
        #
        #   * `:BOOL`
        #   * `:BYTES`
        #   * `:DATE`
        #   * `:FLOAT64`
        #   * `:FLOAT32`
        #   * `:INT64`
        #   * `:STRING`
        #   * `:TIMESTAMP`
        #   * `Array` - Lists are specified by providing the type code in an
        #     array. For example, an array of integers are specified as
        #     `[:INT64]`.
        #   * {Fields} - Nested Structs are specified by providing a Fields
        #     object.
        #
        # @return [Fields] The fields of the given types.
        #
        # @example Create a STRUCT value with named fields using Fields object:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #
        #   named_type = batch_client.fields(
        #     { id: :INT64, name: :STRING, active: :BOOL }
        #   )
        #   named_data = named_type.struct(
        #     { id: 42, name: nil, active: false }
        #   )
        #
        # @example Create a STRUCT value with anonymous field names:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #
        #   anon_type = batch_client.fields [:INT64, :STRING, :BOOL]
        #   anon_data = anon_type.struct [42, nil, false]
        #
        # @example Create a STRUCT value with duplicate field names:
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #
        #   dup_type = batch_client.fields(
        #     [[:x, :INT64], [:x, :STRING], [:x, :BOOL]]
        #   )
        #   dup_data = dup_type.struct [42, nil, false]
        #
        def fields types
          Fields.new types
        end

        ##
        # Creates a Spanner Range. This can be used in place of a Ruby Range
        # when needing to exclude the beginning value.
        #
        # @param [Object] beginning The object that defines the beginning of the
        #   range.
        # @param [Object] ending The object that defines the end of the range.
        # @param [Boolean] exclude_begin Determines if the range excludes its
        #   beginning value. Default is `false`.
        # @param [Boolean] exclude_end Determines if the range excludes its
        #   ending value. Default is `false`.
        #
        # @return [Google::Cloud::Spanner::Range] The new Range instance.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   batch_client = spanner.batch_client "my-instance", "my-database"
        #   batch_snapshot = batch_client.batch_snapshot
        #
        #   key_range = batch_client.range 1, 100
        #
        #   partitions = batch_snapshot.partition_read "users", [:id, :name],
        #                                           keys: key_range
        #
        def range beginning, ending, exclude_begin: false, exclude_end: false
          Range.new beginning, ending,
                    exclude_begin: exclude_begin,
                    exclude_end: exclude_end
        end

        # @private
        def to_s
          "(project_id: #{project_id}, instance_id: #{instance_id}, " \
            "database_id: #{database_id})"
        end

        # @private
        def inspect
          "#<#{self.class.name} #{self}>"
        end

        protected

        ##
        # @private Raise an error unless an active connection to the service is
        # available.
        def ensure_service!
          raise "Must have active connection to service" unless @project.service
        end

        ##
        # New session for each use.
        def session
          ensure_service!
          grpc = @project.service.create_session \
            V1::Spanner::Paths.database_path(
              project: project_id, instance: instance_id, database: database_id
            ),
            labels: @session_labels
          Session.from_grpc grpc, @project.service, query_options: @query_options
        end

        ##
        # Check for valid snapshot arguments
        def validate_snapshot_args! strong: nil,
                                    timestamp: nil, read_timestamp: nil,
                                    staleness: nil, exact_staleness: nil
          valid_args_count = [strong, timestamp, read_timestamp, staleness,
                              exact_staleness].compact.count
          return true if valid_args_count <= 1
          raise ArgumentError,
                "Can only provide one of the following arguments: " \
                "(strong, timestamp, read_timestamp, staleness, " \
                "exact_staleness)"
        end
      end
    end
  end
end
