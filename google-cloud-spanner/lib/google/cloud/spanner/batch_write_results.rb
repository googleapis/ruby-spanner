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
      # @private Helper class to process BatchWrite response
      class BatchWriteResults
        ## Object of type
        # Google::Cloud::Spanner::V1::BatchWriteResponse
        attr_reader :grpc

        def initialize enum
          @grpc = enum.peek
          # TODO: Shoud I handle error here similar to from_enum() in Results class?
        end

        def indexes
          if @grpc.status.code.zero?
            @grpc.indexes
          else
            begin
              raise Google::Cloud::Error.from_error @grpc.status
            rescue Google::Cloud::Error
              raise Google::Cloud::Spanner::BatchUpdateError.from_grpc @grpc
            end
          end
        end
      end
    end
  end
end
