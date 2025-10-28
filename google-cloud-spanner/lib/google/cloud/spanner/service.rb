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


require "google/cloud/spanner/errors"
require "google/cloud/spanner/credentials"
require "google/cloud/spanner/version"
require "google/cloud/spanner/v1"
require "google/cloud/spanner/admin/instance/v1"
require "google/cloud/spanner/admin/database/v1"
require "google/cloud/spanner/convert"
require "google/cloud/spanner/lar_headers"

module Google
  module Cloud
    module Spanner
      ##
      # @private Represents the gRPC Spanner service, including all the API
      # methods.
      class Service
        attr_accessor :project
        attr_accessor :credentials
        attr_accessor :timeout
        attr_accessor :host
        attr_accessor :lib_name
        attr_accessor :lib_version
        attr_accessor :quota_project
        attr_accessor :enable_leader_aware_routing

        attr_reader :universe_domain

        RST_STREAM_INTERNAL_ERROR = "Received RST_STREAM".freeze
        EOS_INTERNAL_ERROR = "Received unexpected EOS on DATA frame from server".freeze

        # Creates a new `Spanner::Service` instance.
        # @param project [::String] The project id to use
        # @param credentials [::Symbol, ::Google::Auth::Credentials] Credentials
        # @param quota_project [::String, nil] Optional. The quota project id to use
        # @param host [::String, nil] Optional. The endpoint override.
        # @param timeout [::Numeric, nil] Optional. Timeout for Gapic client.
        # @param lib_name [::String, nil] Optional. Library name for headers.
        # @param lib_version [::String, nil] Optional. Library version for headers.
        # @param enable_leader_aware_routing [::Boolean, nil] Optional. Whether Leader
        #   Aware Routing should be enabled.
        # @param universe_domain [::String, nil] Optional. The domain of the universe to connect to.
        # @private
        def initialize project, credentials, quota_project: nil,
                       host: nil, timeout: nil, lib_name: nil, lib_version: nil,
                       enable_leader_aware_routing: nil, universe_domain: nil
          @project = project
          @credentials = credentials
          @quota_project = quota_project || (credentials.quota_project_id if credentials.respond_to? :quota_project_id)
          # TODO: This logic is part of UniverseDomainConcerns in gapic-common
          # but is being copied here because we need to determine the host up
          # front in order to build a gRPC channel. We should refactor this
          # somehow to allow this logic to live where it is supposed to.
          @universe_domain = universe_domain || ENV["GOOGLE_CLOUD_UNIVERSE_DOMAIN"] || "googleapis.com"
          @host = host ||
                  Google::Cloud::Spanner::V1::Spanner::Client::DEFAULT_ENDPOINT_TEMPLATE.sub(
                    Gapic::UniverseDomainConcerns::ENDPOINT_SUBSTITUTION, @universe_domain
                  )
          @timeout = timeout
          @lib_name = lib_name
          @lib_version = lib_version
          @enable_leader_aware_routing = enable_leader_aware_routing
        end

        def channel
          require "grpc"
          GRPC::Core::Channel.new host, chan_args, chan_creds
        end

        def chan_args
          { "grpc.service_config_disable_resolution" => 1 }
        end

        def chan_creds
          return credentials if insecure?
          require "grpc"
          GRPC::Core::ChannelCredentials.new.compose \
            GRPC::Core::CallCredentials.new credentials.client.updater_proc
        end

        # `V1::Spanner::Client` or a mock.
        # @return [::Google::Cloud::Spanner::V1::Spanner::Client]
        def service
          return mocked_service if mocked_service
          @service ||=
            V1::Spanner::Client.new do |config|
              config.credentials = channel
              config.quota_project = @quota_project
              config.timeout = timeout if timeout
              config.endpoint = host if host
              config.universe_domain = @universe_domain
              config.lib_name = lib_name_with_prefix
              config.lib_version = Google::Cloud::Spanner::VERSION
              config.metadata = { "google-cloud-resource-prefix" => "projects/#{@project}" }
            end
        end
        attr_accessor :mocked_service

        def instances
          return mocked_instances if mocked_instances
          @instances ||=
            Admin::Instance::V1::InstanceAdmin::Client.new do |config|
              config.credentials = channel
              config.quota_project = @quota_project
              config.timeout = timeout if timeout
              config.endpoint = host if host
              config.universe_domain = @universe_domain
              config.lib_name = lib_name_with_prefix
              config.lib_version = Google::Cloud::Spanner::VERSION
              config.metadata = { "google-cloud-resource-prefix" => "projects/#{@project}" }
            end
        end
        attr_accessor :mocked_instances

        def databases
          return mocked_databases if mocked_databases
          @databases ||=
            Admin::Database::V1::DatabaseAdmin::Client.new do |config|
              config.credentials = channel
              config.quota_project = @quota_project
              config.timeout = timeout if timeout
              config.endpoint = host if host
              config.universe_domain = @universe_domain
              config.lib_name = lib_name_with_prefix
              config.lib_version = Google::Cloud::Spanner::VERSION
              config.metadata = { "google-cloud-resource-prefix" => "projects/#{@project}" }
            end
        end
        attr_accessor :mocked_databases

        def insecure?
          credentials == :this_channel_is_insecure
        end

        def list_instances token: nil, max: nil, call_options: nil
          opts = default_options call_options: call_options
          request = {
            parent:     project_path,
            page_size:  max,
            page_token: token
          }
          paged_enum = instances.list_instances request, opts
          paged_enum.response
        end

        # Gets information about a particular instance
        # @param name [::String] The name of the Spanner instance, e.g. 'myinstance'
        #   or path to the Spanner instance, e.g. `projects/myproject/instances/myinstance`.
        # @private
        # @return [::Google::Cloud::Spanner::Admin::Instance::V1::Instance]
        def get_instance name, call_options: nil
          opts = default_options call_options: call_options
          request = { name: instance_path(name) }
          instances.get_instance request, opts
        end

        def create_instance instance_id, name: nil, config: nil, nodes: nil,
                            processing_units: nil, labels: nil,
                            call_options: nil
          opts = default_options call_options: call_options
          labels = labels.to_h { |k, v| [String(k), String(v)] } if labels

          create_obj = Admin::Instance::V1::Instance.new({
            display_name: name, config: instance_config_path(config),
            node_count: nodes, processing_units: processing_units,
            labels: labels
          }.compact)

          request = {
            parent:      project_path,
            instance_id: instance_id,
            instance:    create_obj
          }
          instances.create_instance request, opts
        end

        def update_instance instance, field_mask: nil, call_options: nil
          opts = default_options call_options: call_options

          if field_mask.nil? || field_mask.empty?
            field_mask = %w[display_name node_count labels]
          end

          request = {
            instance: instance,
            field_mask: Google::Protobuf::FieldMask.new(paths: field_mask)
          }
          instances.update_instance request, opts
        end

        def delete_instance name, call_options: nil
          opts = default_options call_options: call_options
          request = { name: instance_path(name) }
          instances.delete_instance request, opts
        end

        def get_instance_policy name, call_options: nil
          opts = default_options call_options: call_options
          request = { resource: instance_path(name) }
          instances.get_iam_policy request, opts
        end

        def set_instance_policy name, new_policy, call_options: nil
          opts = default_options call_options: call_options
          request = {
            resource: instance_path(name),
            policy:  new_policy
          }
          instances.set_iam_policy request, opts
        end

        def test_instance_permissions name, permissions, call_options: nil
          opts = default_options call_options: call_options
          request = {
            resource:    instance_path(name),
            permissions: permissions
          }
          instances.test_iam_permissions request, opts
        end

        def list_instance_configs token: nil, max: nil, call_options: nil
          opts = default_options call_options: call_options
          request = { parent: project_path, page_size: max, page_token: token }
          paged_enum = instances.list_instance_configs request, opts
          paged_enum.response
        end

        def get_instance_config name, call_options: nil
          opts = default_options call_options: call_options
          request = { name: instance_config_path(name) }
          instances.get_instance_config request, opts
        end

        def list_databases instance_id, token: nil, max: nil, call_options: nil
          opts = default_options call_options: call_options
          request = {
            parent:     instance_path(instance_id),
            page_size:  max,
            page_token: token
          }
          paged_enum = databases.list_databases request, opts
          paged_enum.response
        end

        def get_database instance_id, database_id, call_options: nil
          opts = default_options call_options: call_options
          request = { name: database_path(instance_id, database_id) }
          databases.get_database request, opts
        end

        def create_database instance_id, database_id, statements: [],
                            call_options: nil, encryption_config: nil
          opts = default_options call_options: call_options
          request = {
            parent: instance_path(instance_id),
            create_statement: "CREATE DATABASE `#{database_id}`",
            extra_statements: Array(statements),
            encryption_config: encryption_config
          }
          databases.create_database request, opts
        end

        def drop_database instance_id, database_id, call_options: nil
          opts = default_options call_options: call_options
          request = { database: database_path(instance_id, database_id) }
          databases.drop_database request, opts
        end

        def get_database_ddl instance_id, database_id, call_options: nil
          opts = default_options call_options: call_options
          request = { database: database_path(instance_id, database_id) }
          databases.get_database_ddl request, opts
        end

        def update_database_ddl instance_id, database_id, statements: [],
                                operation_id: nil, call_options: nil, descriptor_set: nil
          bin_data =
            case descriptor_set
            when Google::Protobuf::FileDescriptorSet
              Google::Protobuf::FileDescriptorSet.encode descriptor_set
            when String
              File.binread descriptor_set
            when NilClass
              nil
            else
              raise ArgumentError,
                    "A value of type #{descriptor_set.class} is not supported."
            end

          proto_descriptors = bin_data unless bin_data.nil?
          opts = default_options call_options: call_options
          request = {
            database: database_path(instance_id, database_id),
            statements: Array(statements),
            operation_id: operation_id,
            proto_descriptors: proto_descriptors
          }
          databases.update_database_ddl request, opts
        end

        def get_database_policy instance_id, database_id, call_options: nil
          opts = default_options call_options: call_options
          request = { resource: database_path(instance_id, database_id) }
          databases.get_iam_policy request, opts
        end

        def set_database_policy instance_id, database_id, new_policy,
                                call_options: nil
          opts = default_options call_options: call_options
          request = {
            resource: database_path(instance_id, database_id),
            policy:   new_policy
          }
          databases.set_iam_policy request, opts
        end

        def test_database_permissions instance_id, database_id, permissions,
                                      call_options: nil
          opts = default_options call_options: call_options
          request = {
            resource:    database_path(instance_id, database_id),
            permissions: permissions
          }
          databases.test_iam_permissions request, opts
        end

        def get_session session_name, call_options: nil
          route_to_leader = LARHeaders.get_session
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          service.get_session({ name: session_name }, opts)
        end

        # Creates a new Spanner session.
        # This creates a `V1::Session` protobuf object not wrapped in `Spanner::Session`.
        #
        # @param database_name [::String] The full name of the database.
        # @param labels [::Hash, nil] Optional. The labels to be applied to all sessions
        #   created by the client. Example: `"team" => "billing-service"`.
        # @param database_role [::String, nil] Optional. The Spanner session creator role.
        #   Example: `analyst`.
        # @param multiplexed [::Boolean] Optional. Default to `false`.
        #   If `true`, specifies a multiplexed session.
        # @param call_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   call options. Example option `:timeout`.
        # @return [::Google::Cloud::Spanner::V1::Session]
        # @private
        def create_session database_name, labels: nil,
                           database_role: nil, multiplexed: false,
                           call_options: nil
          route_to_leader = LARHeaders.create_session
          opts = default_options(
            session_name: database_name,
            call_options: call_options,
            route_to_leader: route_to_leader
          )

          # check if we need a session object in request or server defaults would work.
          params_diff_from_default = !(labels.nil? && database_role.nil? && !multiplexed)

          if params_diff_from_default
            session = V1::Session.new(
              labels: labels,
              creator_role: database_role,
              multiplexed: multiplexed
            )
          end

          service.create_session(
            { database: database_name, session: session }, opts
          )
        end

        def batch_create_sessions database_name, session_count, labels: nil,
                                  call_options: nil, database_role: nil
          route_to_leader = LARHeaders.batch_create_sessions
          opts = default_options session_name: database_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          session = V1::Session.new labels: labels, creator_role: database_role if labels || database_role
          # The response may have fewer sessions than requested in the RPC.
          request = {
            database: database_name,
            session_count: session_count,
            session_template: session
          }
          service.batch_create_sessions request, opts
        end

        def delete_session session_name, call_options: nil
          route_to_leader = LARHeaders.delete_session
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          service.delete_session({ name: session_name }, opts)
        end

        def execute_streaming_sql session_name, sql, transaction: nil,
                                  params: nil, types: nil, resume_token: nil,
                                  partition_token: nil, seqno: nil,
                                  query_options: nil, request_options: nil,
                                  call_options: nil, data_boost_enabled: nil,
                                  directed_read_options: nil,
                                  route_to_leader: nil
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request =  {
            session: session_name,
            sql: sql,
            transaction: transaction,
            params: params,
            param_types: types,
            resume_token: resume_token,
            partition_token: partition_token,
            seqno: seqno,
            query_options: query_options,
            request_options: request_options,
            directed_read_options: directed_read_options
          }
          request[:data_boost_enabled] = data_boost_enabled unless data_boost_enabled.nil?
          service.execute_streaming_sql request, opts
        end

        def execute_batch_dml session_name, transaction, statements, seqno,
                              request_options: nil, call_options: nil
          route_to_leader = LARHeaders.execute_batch_dml
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          statements = statements.map(&:to_grpc)
          request = {
            session: session_name,
            transaction: transaction,
            statements: statements,
            seqno: seqno,
            request_options: request_options
          }
          service.execute_batch_dml request, opts
        end

        def streaming_read_table session_name, table_name, columns, keys: nil,
                                 index: nil, transaction: nil, limit: nil,
                                 resume_token: nil, partition_token: nil,
                                 request_options: nil, call_options: nil,
                                 data_boost_enabled: nil, directed_read_options: nil,
                                 route_to_leader: nil, order_by: nil, lock_hint: nil
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = {
            session: session_name, table: table_name, columns: columns,
            key_set: keys, transaction: transaction, index: index,
            limit: limit, resume_token: resume_token,
            partition_token: partition_token, request_options: request_options,
            order_by: order_by, lock_hint: lock_hint
          }
          request[:data_boost_enabled] = data_boost_enabled unless data_boost_enabled.nil?
          request[:directed_read_options] = directed_read_options unless directed_read_options.nil?
          service.streaming_read request, opts
        end

        def partition_read session_name, table_name, columns, transaction,
                           keys: nil, index: nil, partition_size_bytes: nil,
                           max_partitions: nil, call_options: nil
          partition_opts = partition_options partition_size_bytes,
                                             max_partitions
          route_to_leader = LARHeaders.partition_read
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = {
            session: session_name, table: table_name, key_set: keys,
            transaction: transaction, index: index, columns: columns,
            partition_options: partition_opts
          }
          service.partition_read request, opts
        end

        def partition_query session_name, sql, transaction, params: nil,
                            types: nil, partition_size_bytes: nil,
                            max_partitions: nil, call_options: nil
          partition_opts = partition_options partition_size_bytes,
                                             max_partitions
          route_to_leader = LARHeaders.partition_query
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = {
            session: session_name, sql: sql, transaction: transaction,
            params: params, param_types: types,
            partition_options: partition_opts
          }
          service.partition_query request, opts
        end

        # Commits a transaction. Can be a predefined (`transaction_id`) transaction
        # or a single-use created for this request. The request includes the mutations to be
        # applied to rows in the database.
        #
        # @param session_name [::String]
        #   Required. The session in which the transaction to be committed is running.
        # @param mutations [::Array<::Google::Cloud::Spanner::V1::Mutation>] Optional.
        #   The mutations to be executed when this transaction commits. All
        #   mutations are applied atomically, in the order they appear in
        #   this list. Defaults to an empty array.
        # @param transaction_id [::String, nil] Optional.
        #   Commit a previously-started transaction. If nil, a new single-use transation will be used.
        # @param exclude_txn_from_change_streams [::Boolean] Optional. Defaults to `false`.
        #   When `exclude_txn_from_change_streams` is set to `true`, it prevents read
        #   or write transactions from being tracked in change streams.
        # @param commit_options [::Hash, nil]  Optional. A hash of commit options.
        #   Example option: `:return_commit_stats`.
        # @param request_options [::Hash, nil] Optional. Common request options.
        #   Example option: `:priority`.
        # @param call_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   call options. Example option `:timeout`.
        # @param precommit_token [::Google::Cloud::Spanner::V1::MultiplexedSessionPrecommitToken, nil] Optional.
        #    If the read-write transaction was executed on a multiplexed session, then a precommit token
        #    with the highest sequence number received in this transaction attempt must be included.
        # @private
        # @return [::Google::Cloud::Spanner::V1::CommitResponse]
        def commit session_name, mutations = [],
                   transaction_id: nil, exclude_txn_from_change_streams: false,
                   commit_options: nil, request_options: nil, call_options: nil,
                   precommit_token: nil
          route_to_leader = LARHeaders.commit
          tx_opts = nil
          if transaction_id.nil?
            tx_opts = V1::TransactionOptions.new(
              read_write: V1::TransactionOptions::ReadWrite.new,
              exclude_txn_from_change_streams: exclude_txn_from_change_streams
            )
          end
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = {
            session: session_name, transaction_id: transaction_id,
            single_use_transaction: tx_opts, mutations: mutations,
            request_options: request_options, precommit_token: precommit_token
          }

          request = add_commit_options request, commit_options
          # request is a hash equivalent of `::Google::Cloud::Spanner::V1::CommitRequest`
          service.commit request, opts
        end

        # Merges commit options hash to a hash representing a `V1::CommitRequest`.
        # @param request [::Hash] A `::Google::Cloud::Spanner::V1::CommitRequest` in a hash form.
        # @param commit_options [::Hash, nil]  Optional. A hash of commit options.
        #   Example option: `:return_commit_stats`.
        # @return [::Hash] An enriched `::Google::Cloud::Spanner::V1::CommitRequest` in a hash form.
        # @private
        def add_commit_options request, commit_options
          if commit_options
            if commit_options.key? :return_commit_stats
              request[:return_commit_stats] =
                commit_options[:return_commit_stats]
            end
            if commit_options.key? :max_commit_delay
              request[:max_commit_delay] =
                Convert.number_to_duration(commit_options[:max_commit_delay],
                                           millisecond: true)
            end
          end
          request
        end

        def rollback session_name, transaction_id, call_options: nil
          route_to_leader = LARHeaders.rollback
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = { session: session_name, transaction_id: transaction_id }
          service.rollback request, opts
        end

        # Explicitly begins a new transaction, making a `BeginTransaction` rpc call,
        # and creating and returning a `V1::Transaction` object.
        #
        # Explicit transaction creation can often be skipped:
        # {::Google::Cloud::Spanner::V1::Spanner::Client#read Read},
        # {::Google::Cloud::Spanner::V1::Spanner::Client#execute_sql ExecuteSql} and
        # {::Google::Cloud::Spanner::V1::Spanner::Client#execute_batch_dml ExecuteBatchDml}
        # can begin a new transaction as part of the request (so-called inline-begin).
        # The inline-begin functionality is used in methods on `Spanner::Transaction` class,
        # e.g. `Spanner::Transaction#read`, accessible to the end-users via the `Spanner::Client#transaction` method.
        #
        # All the above methods, and  {::Google::Cloud::Spanner::V1::Spanner::Client#commit Commit}
        # can utilize single-use transactions that do not require an explicit BeginTransaction call.
        # Single-use transactions are used by the methods on `Spanner::Client` class,
        # e.g. `Spanner::Client#read`, with the exception of `Spanner::Client#transaction`.
        #
        # @param session_name [::String]
        #   Required. The session in which the transaction is to be created.
        #   Values are of the form:
        #   `projects/<project_id>/instances/<instance_id>/databases/<database_id>/sessions/<session_id>`.
        # @param exclude_txn_from_change_streams [::Boolean] Optional. Defaults to `false`.
        #   When `exclude_txn_from_change_streams` is set to `true`, it prevents read
        #   or write transactions from being tracked in change streams.
        # @param request_options [::Hash, nil] Optional. Common request options.
        #   Example option: `:priority`.
        # @param call_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   call options. Example option `:timeout`.
        # @param route_to_leader [::String, nil] Optional. The value to be sent
        #   as `x-goog-spanner-route-to-leader` header for leader aware routing.
        #   Expected values: `"true"` or `"false"`.
        # @param mutation_key [::Google::Cloud::Spanner::V1::Mutation, nil] Optional.
        #   If a read-write transaction on a multiplexed session commit mutations
        #   without performing any reads or queries, one of the mutations from the mutation set
        #   must be sent as a mutation key for `BeginTransaction`.
        # @private
        # @return [::Google::Cloud::Spanner::V1::Transaction]
        def begin_transaction session_name,
                              exclude_txn_from_change_streams: false,
                              request_options: nil,
                              call_options: nil,
                              route_to_leader: nil,
                              mutation_key: nil
          tx_opts = V1::TransactionOptions.new(
            read_write: V1::TransactionOptions::ReadWrite.new,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams
          )
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = {
            session: session_name,
            options: tx_opts,
            request_options: request_options,
            mutation_key: mutation_key
          }
          service.begin_transaction request, opts
        end

        def batch_write session_name,
                        mutation_groups,
                        exclude_txn_from_change_streams: false,
                        request_options: nil,
                        call_options: nil
          route_to_leader = LARHeaders.batch_write
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = {
            session: session_name,
            request_options: request_options,
            mutation_groups: mutation_groups,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams
          }
          service.batch_write request, opts
        end

        # Creates a specialized `V1::Transaction` object. Reads on that object will have
        # at most one of following consistency properties:
        # * reading all previously commited transactions
        # * reading all data from a given timestamp
        # * reading all data from a timestamp that is exactly a given value old
        # (the last one sidesteps worries of client-server time skew).
        #
        # Having at _least_ one of those is not enforced so this can create normal transactions
        # as well.
        # Created transactions will include the  the read timestamp chosen for the transaction.
        # @param session_name [::String] Required.
        #   Required. The session in which the snapshot transaction is to be created..
        #   Values are of the form:
        #   `projects/<project_id>/instances/<instance_id>/databases/<database_id>/sessions/<session_id>`.
        # @param strong [::Boolean, nil] Optional.
        #   Whether this transaction should have strong consistency.
        # @param timestamp [::String, ::Date ::Time, nil] Optional.
        #   Timestamp that the reads should be executed at. Reads are repeatable with this option.
        # @param staleness [::Numeric, nil] Optional.
        #   The offset of staleness that the reads should be executed at.
        # @param call_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   call options. Example option `:timeout`.
        def create_snapshot session_name, strong: nil, timestamp: nil,
                            staleness: nil, call_options: nil
          tx_opts = V1::TransactionOptions.new(
            read_only: V1::TransactionOptions::ReadOnly.new(
              {
                strong: strong,
                read_timestamp: Convert.time_to_timestamp(timestamp),
                exact_staleness: Convert.number_to_duration(staleness),
                return_read_timestamp: true
              }.compact
            )
          )
          opts = default_options session_name: session_name,
                                 call_options: call_options
          request = { session: session_name, options: tx_opts, mutation_key: nil }
          service.begin_transaction request, opts
        end

        def create_pdml session_name,
                        exclude_txn_from_change_streams: false,
                        call_options: nil
          tx_opts = V1::TransactionOptions.new(
            partitioned_dml: V1::TransactionOptions::PartitionedDml.new,
            exclude_txn_from_change_streams: exclude_txn_from_change_streams
          )
          route_to_leader = LARHeaders.begin_transaction true
          opts = default_options session_name: session_name,
                                 call_options: call_options,
                                 route_to_leader: route_to_leader
          request = { session: session_name, options: tx_opts, mutation_key: nil }
          service.begin_transaction request, opts
        end

        def create_backup instance_id, database_id, backup_id, expire_time,
                          version_time, call_options: nil,
                          encryption_config: nil
          opts = default_options call_options: call_options
          backup = {
            database: database_path(instance_id, database_id),
            expire_time: expire_time,
            version_time: version_time
          }
          request = {
            parent:    instance_path(instance_id),
            backup_id: backup_id,
            backup:    backup,
            encryption_config: encryption_config
          }
          databases.create_backup request, opts
        end

        def get_backup instance_id, backup_id, call_options: nil
          opts = default_options call_options: call_options
          request = { name: backup_path(instance_id, backup_id) }
          databases.get_backup request, opts
        end

        def update_backup backup, update_mask, call_options: nil
          opts = default_options call_options: call_options
          request = { backup: backup, update_mask: update_mask }
          databases.update_backup request, opts
        end

        def delete_backup instance_id, backup_id, call_options: nil
          opts = default_options call_options: call_options
          request = { name: backup_path(instance_id, backup_id) }
          databases.delete_backup request, opts
        end

        def list_backups instance_id,
                         filter: nil, page_size: nil, page_token: nil,
                         call_options: nil
          opts = default_options call_options: call_options
          request = {
            parent:    instance_path(instance_id),
            filter:    filter,
            page_size: page_size,
            page_token: page_token
          }
          databases.list_backups request, opts
        end

        def list_database_operations instance_id,
                                     filter: nil,
                                     page_size: nil,
                                     page_token: nil,
                                     call_options: nil
          opts = default_options call_options: call_options
          request = {
            parent:     instance_path(instance_id),
            filter:     filter,
            page_size:  page_size,
            page_token: page_token
          }
          databases.list_database_operations request, opts
        end

        # Lists the backup `::Google::Longrunning::Operation` long-running operations in
        # the given instance. A backup operation has a name of the form
        # projects/<project>/instances/<instance>/backups/<backup>/operations/<operation>.
        # @param instance_id [::String] The name of the Spanner instance, e.g. 'myinstance'
        #   or path to the Spanner instance, e.g. `projects/myproject/instances/myinstance`.
        # @param filter [::String, nil] Optional.
        #   An expression that filters the list of returned backup operations.
        #   Example filter: `done:true`.
        # @param page_size [::Integer, nil] Optional.
        #   Number of operations to be returned in the response. If 0 or
        #   less, defaults to the server's maximum allowed page size.
        # @param page_token [::String, nil] Optional.
        #   If set, `page_token` should contain a value received as a `next_page_token`
        #   from a previous `ListBackupOperationsResponse` to the same `parent`
        #   and with the same `filter`.
        # @param call_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   call options. Example option `:timeout`.
        # @private
        # @return [::Gapic::PagedEnumerable<::Gapic::Operation>]
        def list_backup_operations instance_id,
                                   filter: nil, page_size: nil,
                                   page_token: nil,
                                   call_options: nil
          opts = default_options call_options: call_options
          request = {
            parent:     instance_path(instance_id),
            filter:     filter,
            page_size:  page_size,
            page_token: page_token
          }
          databases.list_backup_operations request, opts
        end

        def restore_database backup_instance_id, backup_id,
                             database_instance_id, database_id,
                             call_options: nil, encryption_config: nil
          opts = default_options call_options: call_options
          request = {
            parent:      instance_path(database_instance_id),
            database_id: database_id,
            backup:      backup_path(backup_instance_id, backup_id),
            encryption_config: encryption_config
          }
          databases.restore_database request, opts
        end

        ##
        # Checks if a request can be retried. This is based on the error returned.
        # Retryable errors are:
        #   - Unavailable error
        #   - Internal EOS error
        #   - Internal RST_STREAM error
        def retryable? err
          err.instance_of?(Google::Cloud::UnavailableError) ||
            err.instance_of?(GRPC::Unavailable) ||
            (err.instance_of?(Google::Cloud::InternalError) && err.message.include?(EOS_INTERNAL_ERROR)) ||
            (err.instance_of?(GRPC::Internal) && err.details.include?(EOS_INTERNAL_ERROR)) ||
            (err.instance_of?(Google::Cloud::InternalError) && err.message.include?(RST_STREAM_INTERNAL_ERROR)) ||
            (err.instance_of?(GRPC::Internal) && err.details.include?(RST_STREAM_INTERNAL_ERROR))
        end

        def inspect
          "#{self.class}(#{@project})"
        end

        protected

        def lib_name_with_prefix
          return "gccl" if [nil, "gccl"].include? lib_name

          value = lib_name.dup
          value << "/#{lib_version}" if lib_version
          value << " gccl"
        end

        # Creates new `Gapic::CallOptions` from typical parameters for Spanner RPC calls.
        #
        # @param session_name [::String, nil] Optional.
        #   The session name. Used to extract the routing header. The value will be
        #   used to send the old `google-cloud-resource-prefix` routing header.
        #   Expected values are of the form:
        #   `projects/<project_id>/instances/<instance_id>/databases/<database_id>/sessions/<session_id>`.
        #   If nil is specified nothing will be sent.
        # @param call_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   call options. Example option `:timeout`.
        # @param route_to_leader [::String, nil] Optional. The value to be sent
        #   as `x-goog-spanner-route-to-leader` header for leader aware routing.
        #   Expected values: `"true"` or `"false"`. If nil is specified nothing will be sent.
        # @private
        # @return [::Gapic::CallOptions]
        def default_options session_name: nil, call_options: nil, route_to_leader: nil
          opts = {}
          metadata = {}
          if session_name
            default_prefix = session_name.split("/sessions/").first
            metadata["google-cloud-resource-prefix"] = default_prefix
          end
          if @enable_leader_aware_routing && !route_to_leader.nil?
            metadata["x-goog-spanner-route-to-leader"] = route_to_leader
          end
          opts[:metadata] = metadata
          if call_options
            opts[:timeout] = call_options[:timeout] if call_options[:timeout]
            opts[:retry_policy] = call_options[:retry_policy] if call_options[:retry_policy]
          end
          ::Gapic::CallOptions.new(**opts)
        end

        def partition_options partition_size_bytes, max_partitions
          return nil unless partition_size_bytes || max_partitions
          partition_opts = V1::PartitionOptions.new
          if partition_size_bytes
            partition_opts.partition_size_bytes = partition_size_bytes
          end
          partition_opts.max_partitions = max_partitions if max_partitions
          partition_opts
        end

        def project_path
          Admin::Instance::V1::InstanceAdmin::Paths.project_path \
            project: project
        end

        # Converts an instance name to instance path.
        # If an instance path is given, returns it unchanged
        # @param name [::String] name of the Spanner instance, e.g. 'myinstance'
        #   or path to the Spanner instance, e.g. `projects/myproject/instances/myinstance`.
        # @private
        # @return [::String]
        def instance_path name
          return name if name.to_s.include? "/"

          Admin::Instance::V1::InstanceAdmin::Paths.instance_path \
            project: project, instance: name
        end

        def instance_config_path name
          return name if name.to_s.include? "/"

          Admin::Instance::V1::InstanceAdmin::Paths.instance_config_path \
            project: project, instance_config: name
        end

        def database_path instance_id, database_id
          Admin::Database::V1::DatabaseAdmin::Paths.database_path \
            project: project, instance: instance_id, database: database_id
        end

        def session_path instance_id, database_id, session_id
          V1::Spanner::Paths.session_path \
            project: project, instance: instance_id, database: database_id,
            session: session_id
        end

        def backup_path instance_id, backup_id
          Admin::Database::V1::DatabaseAdmin::Paths.backup_path \
            project: project, instance: instance_id, backup: backup_id
        end
      end
    end
  end
end
