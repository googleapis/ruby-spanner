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
      # @private Helper module to access header values
      # for Leader Aware Routing
      module LARHeaders
        module ClassMethods
          def begin_transaction is_read_write
            is_read_write ? "true" : "false"
          end

          def read is_read_write
            is_read_write ? "true" : "false"
          end

          def execute_query is_read_write
            is_read_write ? "true" : "false"
          end

          def commit
            "true"
          end

          def execute_batch_dml
            "true"
          end

          def create_session
            "true"
          end

          def batch_create_sessions
            "true"
          end

          def delete_session
            "false"
          end

          # rubocop:disable Naming/AccessorMethodName
          def get_session
            "true"
          end
          # rubocop:enable Naming/AccessorMethodName

          def partition_read
            "true"
          end

          def partition_query
            "true"
          end

          def rollback
            "true"
          end
        end

        extend ClassMethods
      end
    end
  end
end
