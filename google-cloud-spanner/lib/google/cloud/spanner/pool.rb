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


require "concurrent"
require "google/cloud/spanner/errors"
require "google/cloud/spanner/session"
require "google/cloud/spanner/session_creation_options"

module Google
  module Cloud
    module Spanner
      ##
      # @private
      #
      # # Pool
      #
      # Implements a pool for managing and reusing
      # {Google::Cloud::Spanner::Session} instances.
      #
      class Pool
        # @return [Array<Session>] A stack of `Session` objects.
        attr_accessor :sessions_available

        # @return [Hash{String => Session}] A hash with session_id as keys,
        # and `Session` objects as values.
        attr_accessor :sessions_in_use

        # Creates a new Session pool that manages non-multiplexed sessions.
        # @param service [::Google::Cloud::Spanner::Service] A `Spanner::Service` reference.
        # @param session_creation_options [::Google::Cloud::Spanner::SessionCreationOptions] Required.
        #   Options used for session creation. E.g. session labels.
        # @param min [::Integer] Min number of sessions to keep
        # @param max [::Integer] Max number of sessions to keep
        # @param keepalive [::Numeric] How long after their last usage the sessions can be reclaimed
        # @param fail [::Boolean] If `true` the pool will raise `SessionLimitError` if number of new sessions
        #   needed is more that can be created due to the `max` parameter. If `false` it will wait instead.
        # @param threads [::Integer, nil] Number of threads in the thread pool that is used for keepalive and
        #   release session actions. If `nil` the Pool will choose a reasonable default.
        # @private
        def initialize service, session_creation_options, min: 10, max: 100, keepalive: 1800,
                       fail: true, threads: nil
          @service = service
          @session_creation_options = session_creation_options
          @min = min
          @max = max
          @keepalive = keepalive
          @fail = fail
          @threads = threads || [2, Concurrent.processor_count * 2].max

          @mutex = Mutex.new
          @resource = ConditionVariable.new

          # initialize pool and availability stack
          init
        end

        # Provides a session for running an operation
        # @yield session Session a client can use to run an operation
        # @yieldparam [::Google::Cloud::Spanner::Session] `Spanner::Session` to run an operation
        # @private
        # @return [nil]
        def with_session
          session = checkout_session
          begin
            yield session
          ensure
            checkin_session session
          end
        end

        def checkout_session
          action = nil
          @mutex.synchronize do
            loop do
              raise ClientClosedError if @closed

              # Use LIFO to ensure sessions are used from backend caches, which
              # will reduce the read / write latencies on user requests.
              session = sessions_available.pop # LIFO
              if session
                sessions_in_use[session.session_id] = session
                return session
              end

              if can_allocate_more_sessions?
                action = :new
                break
              end

              raise SessionLimitError if @fail

              @resource.wait @mutex
            end
          end

          if action == :new
            session = new_session!
            @mutex.synchronize do
              sessions_in_use[session.session_id] = session
            end
            return session
          end

          nil
        end

        def checkin_session session
          @mutex.synchronize do
            unless sessions_in_use.key? session.session_id
              raise ArgumentError, "Cannot checkin session"
            end

            sessions_available.push session
            sessions_in_use.delete session.session_id

            @resource.signal
          end

          nil
        end

        def reset!
          close
          init

          @mutex.synchronize do
            @closed = false
          end

          true
        end
        alias reset reset!

        def close
          shutdown
          @thread_pool.wait_for_termination

          true
        end

        def keepalive_or_release!
          to_keepalive = []
          to_release = []

          @mutex.synchronize do
            available_count = sessions_available.count
            release_count = available_count - @min
            release_count = 0 if release_count.negative?

            to_keepalive = sessions_available.select do |x|
              x.idle_since? @keepalive
            end

            # Remove a random portion of the sessions
            to_release = to_keepalive.sample release_count
            to_keepalive -= to_release

            # Remove those to be released from circulation
            @sessions_available -= to_release
          end

          to_release.each { |x| future { x.release! } }
          to_keepalive.each { |x| future { x.keepalive! } }
        end

        private

        def init
          # init the thread pool
          @thread_pool = Concurrent::ThreadPoolExecutor.new \
            max_threads: @threads
          # init the stacks
          @new_sessions_in_process = 0
          # init the keepalive task
          create_keepalive_task!
          # init session stack
          @sessions_available = batch_create_new_sessions @min
          @sessions_in_use = {}
        end

        def shutdown
          @mutex.synchronize do
            @closed = true
          end
          @keepalive_task.shutdown
          # Unblock all waiting threads
          @resource.broadcast
          # Delete all sessions
          @mutex.synchronize do
            sessions_available.each { |s| future { s.release! } }
            sessions_in_use.each_value { |s| future { s.release! } }
            @sessions_available = []
            @sessions_in_use = {}
          end
          # shutdown existing thread pool
          @thread_pool.shutdown
        end

        def new_session!
          @mutex.synchronize do
            @new_sessions_in_process += 1
          end

          begin
            session = create_new_session
          rescue StandardError => e
            @mutex.synchronize do
              @new_sessions_in_process -= 1
            end
            raise e
          end

          @mutex.synchronize do
            @new_sessions_in_process -= 1
          end

          session
        end

        def can_allocate_more_sessions?
          # This is expected to be called from within a synchronize block
          sessions_available.size + sessions_in_use.size + @new_sessions_in_process < @max
        end

        def create_keepalive_task!
          @keepalive_task = Concurrent::TimerTask.new execution_interval: 300 do
            keepalive_or_release!
          end
          @keepalive_task.execute
        end

        def future(&)
          Concurrent::Future.new(executor: @thread_pool, &).execute
        end

        # Creates a new `Spanner::Session`.
        # @private
        # @return [::Google::Cloud::Spanner::Session]
        def create_new_session
          ensure_service!
          grpc = @service.create_session(
            @session_creation_options.database_path,
            labels:  @session_creation_options.session_labels,
            database_role: @session_creation_options.session_creator_role
          )

          Session.from_grpc grpc, @service, query_options: @query_options
        end

        # Creates a batch of new session objects of size `total`.
        # Makes multiple RPCs if necessary. Returns empty array if total is 0.
        # @private
        # @return [::Array<::Google::Cloud::Spanner::Session>]
        def batch_create_new_sessions total
          sessions = []
          remaining = total
          while remaining.positive?
            sessions += batch_create_sessions remaining
            remaining = total - sessions.count
          end
          sessions
        end

        # Tries to creates a batch of new session objects of size `session_count`.
        # The response may have fewer sessions than requested in the RPC.
        # @private
        # @return [::Array<::Google::Cloud::Spanner::Session>]
        def batch_create_sessions session_count
          ensure_service!
          resp = @service.batch_create_sessions(
            @session_creation_options.database_path,
            session_count,
            labels:  @session_creation_options.session_labels,
            database_role: @session_creation_options.session_creator_role
          )

          resp.session.map do |grpc|
            Session.from_grpc grpc, @service, query_options: @query_options
          end
        end

        # Raise an error unless an active connection to the service is available.
        # @private
        # @raise [::StandardError]
        # @return [void]
        def ensure_service!
          raise "Must have active connection to service" unless @service
        end
      end
    end
  end
end
