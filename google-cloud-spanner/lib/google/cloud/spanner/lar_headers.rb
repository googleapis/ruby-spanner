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
      # for Leader Aware Routing (LAR).
      #
      # For a multi-region Cloud Spanner instance, the majority of
      # the write latency is attributed to the network round trip
      # caused by several RPCs between SpanFE and SpanServer. This
      # latency is amplified when the client application is deployed
      # away from the leader region.
      #
      # To decrease the latency, we use LAR to help Google Front End (GFE)
      # or Cloudpath Front End (CFE) to route requests directly to the
      # leader region or the nearest non-leader region.
      #
      # For every RPC, we pass the header `x-goog-spanner-route-to-leader`,
      # whose value is either 'true' or 'false'. Some RPCs (such as admin APIs)
      # don't pass the header since they're deprecated in the handwritten
      # client. Also, users of the client can choose to opt out of the
      # LAR feature, in which case, we don't pass the header.
      #
      # This module lists the header values for different RPCs to be used
      # by the client. These values are expected to be constant. Some RPCs
      # (such as `execute_streaming_sql`) can have different values
      # depending on whether they're run within read-only transactions
      # or read-write transactions.
      #
      module LARHeaders
        module ClassMethods
          def begin_transaction is_read_write_tx
            is_read_write_tx ? "true" : "false"
          end

          def read is_read_write_tx
            is_read_write_tx ? "true" : "false"
          end

          def execute_query is_read_write_tx
            is_read_write_tx ? "true" : "false"
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
