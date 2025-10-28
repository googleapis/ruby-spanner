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

module Google
  module Cloud
    module Spanner
      # Options for creating new sessions that clients use
      # to parametrize Pool and SessionCache.
      # Example: session labels.
      # @private
      class SessionCreationOptions
        # The full path to the Spanner database. Values are of the form:
        # `projects/<project_id>/instances/<instance_id>/databases/<database_id>.
        # @private
        # @return [::String]
        attr_reader :database_path

        # The labels to be applied to all sessions created by the client.
        # Optional. Example: `"team" => "billing-service"`.
        # @private
        # @return [::Hash, nil]
        attr_reader :session_labels

        # The Spanner session creator role.
        # Optional. Example: `analyst`.
        # @return [::String, nil]
        attr_reader :session_creator_role

        # A hash of values to specify the custom query options for executing SQL query.
        # Optional. Example option: `:optimizer_version`.
        # @private
        # @return [::Hash, nil]
        attr_reader :query_options

        # Creates a new SessionCreationOptions object.
        # @param database_path [::String]
        #   The full path to the Spanner database. Values are of the form:
        #   `projects/<project_id>/instances/<instance_id>/databases/<database_id>.
        # @param session_labels [::Hash, nil] Optional. The labels to be applied to all sessions
        #   created by the client. Example: `"team" => "billing-service"`.
        # @param session_creator_role [::String, nil] Optional. The Spanner session creator role.
        #   Example: `analyst`.
        # @param query_options [::Hash, nil] Optional. A hash of values to specify the custom
        #   query options for executing SQL query. Example option: `:optimizer_version`.
        # @private
        def initialize database_path:, session_labels: nil, session_creator_role: nil, query_options: nil
          if database_path.nil? || database_path.empty?
            raise ArgumentError, "database_path is required for session creation options"
          end

          @database_path = database_path
          @session_labels = session_labels
          @session_creator_role = session_creator_role
          @query_options = query_options
        end
      end
    end
  end
end
