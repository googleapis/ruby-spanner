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
        ## Object of type
        # Google::Cloud::Spanner::V1::ExecuteBatchDmlResponse
        attr_reader :grpc

        def row_counts
          unless @grpc.status.code.zero?
            raise Google::Cloud::Spanner::BatchUpdateError.from_grpc @grpc
          end
          @grpc.result_sets.map { |rs| rs.stats.row_count_exact }
        end

        def transaction
          @grpc.result_sets.first.metadata.transaction
        end

        def self.from_grpc grpc
          new.tap do |b|
            b.instance_variable_set :@grpc, grpc
          end
        end
      end
    end
  end
end
