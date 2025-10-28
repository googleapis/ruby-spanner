# Copyright 2023 Google LLC
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
      # @private Helper class to process BatchDML response
      class BatchUpdateResults
        # The `V1::ExecuteBatchDmlResponse` object to process
        # @private
        # @return [::Google::Cloud::Spanner::V1::ExecuteBatchDmlResponse]
        attr_reader :grpc

        # Initializes the `BatchUpdateResults` helper
        # @private
        # @param grpc [::Google::Cloud::Spanner::V1::ExecuteBatchDmlResponse]
        #   The response from `ExecuteBatchDml` rpc to process in this helper.
        def initialize grpc
          @grpc = grpc
        end

        def row_counts
          if @grpc.status.code.zero?
            @grpc.result_sets.map { |rs| rs.stats.row_count_exact }
          else
            begin
              raise Google::Cloud::Error.from_error @grpc.status
            rescue Google::Cloud::Error
              raise Google::Cloud::Spanner::BatchUpdateError.from_grpc @grpc
            end
          end
        end

        ##
        # Returns transaction if available. Otherwise returns nil
        def transaction
          @grpc&.result_sets&.first&.metadata&.transaction
        end
      end
    end
  end
end
