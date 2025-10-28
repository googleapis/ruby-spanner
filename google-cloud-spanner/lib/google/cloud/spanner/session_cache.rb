# Copyright 2025 Google LLC
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
require "google/cloud/spanner/session"
require "google/cloud/spanner/session_creation_options"

module Google
  module Cloud
    module Spanner
      # Cache for the multiplex `{Google::Cloud::Spanner::Session}` instance.
      # @private
      class SessionCache
        # Time in seconds before this SessionCache will refresh the session.
        # Counted from the session creation time (not from last usage).
        # This is specific to multiplex sessions.
        # The backend can keep sessions alive for quite a bit longer (28 days) but
        # we perform refresh after 7 days.
        # @private
        SESSION_REFRESH_SEC = 7 * 24 * 3600

        # Create a single-session "cache" for multiplex sessions.
        # @param service [::Google::Cloud::Spanner::Service] A `Spanner::Service` reference.
        # @param session_creation_options [::Google::Cloud::Spanner::SessionCreationOptions] Required.
        # @private
        def initialize service, session_creation_options
          @service = service
          @session_creation_options = session_creation_options
          @mutex = Mutex.new
          @session = nil
        end

        # Yields the current session to run an operation (or a series of operations) on.
        # @yield session A session to run requests on
        # @yieldparam session [::Google::Cloud::Spanner::Session]
        # @private
        # @yieldreturn [::Object] The result of the operation that ran on a session.
        # @return [::Object] The value returned by the yielded block.
        def with_session
          ensure_session!
          yield @session
        end

        # Re-initializes the session in the session cache.
        # @private
        # @return [::Boolean]
        def reset!
          @mutex.synchronize do
            @session = create_new_session
          end

          true
        end

        # Closes the pool. This is a NOP for Multiplex Session Cache since
        # multiplex sessions don't require cleanup.
        # @private
        # @return [::Boolean]
        def close
          true
        end

        # Returns the current session. For use in the `{Spanner::BatchClient}`
        # where usage pattern is incompatible with `with_session`.
        # For other uses please use `with_session` instead.
        # @private
        # @return [::Google::Cloud::Spanner::Session]
        def session
          ensure_session!
          @session
        end

        private

        # Ensures that a single session exists and is current.
        # @private
        # @return [nil]
        def ensure_session!
          return unless @session.nil? || @session.existed_since?(SESSION_REFRESH_SEC)

          @mutex.synchronize do
            return unless @session.nil? || @session.existed_since?(SESSION_REFRESH_SEC)
            @session = create_new_session
          end

          nil
        end

        # Creates a new multiplexed `Spanner::Session`.
        # @private
        # @return [::Google::Cloud::Spanner::Session]
        def create_new_session
          ensure_service!
          grpc = @service.create_session(
            @session_creation_options.database_path,
            labels:  @session_creation_options.session_labels,
            database_role: @session_creation_options.session_creator_role,
            multiplexed: true
          )

          Session.from_grpc grpc, @service, query_options: @session_creation_options.query_options
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
