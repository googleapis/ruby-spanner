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

        def initialize client, min: 10, max: 100, keepalive: 1800,
                       fail: true, threads: nil
          @client = client
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

        def reset
          close
          init

          @mutex.synchronize do
            @closed = false
          end

          true
        end

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
          @sessions_available = @client.batch_create_new_sessions @min
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
            session = @client.create_new_session
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

        def future &block
          Concurrent::Future.new(executor: @thread_pool, &block).execute
        end
      end
    end
  end
end
