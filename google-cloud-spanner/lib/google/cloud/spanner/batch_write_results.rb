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

require "google/cloud/errors"
require "google/cloud/spanner/convert"

module Google
  module Cloud
    module Spanner
      ##
      # Results of a batch write.
      #
      # This is a stream of {BatchWriteResults::BatchResult} objects, each of
      # which represents a set of mutation groups applied together.
      #
      # Use the Ruby Enumerable module to iterate over the results.
      #
      class BatchWriteResults
        ##
        # Result of a set of mutation groups applied together.
        #
        class BatchResult
          # @private
          def initialize grpc
            @grpc = grpc
          end

          ##
          # The indexes of the mutation groups applied together.
          #
          # @return [::Array<::Integer>]
          #
          def indexes
            @grpc.indexes.to_a
          end

          ##
          # The result of this set of mutation groups.
          #
          # @return [::Google::Rpc::Status]
          #
          def status
            @grpc.status
          end

          ##
          # Whether these mutation groups were successful.
          #
          # @return [::Boolean]
          #
          def ok?
            status.code.zero?
          end

          ##
          # Whether these mutation groups were unsuccessful.
          #
          # @return [::Boolean]
          #
          def error?
            !ok?
          end

          ##
          # The timestamp of the commit.
          #
          # @return [::Time]
          #
          def commit_timestamp
            Convert.timestamp_to_time @grpc.commit_timestamp
          end
        end

        # @private
        def initialize enum
          @enumerable = enum
        end

        ##
        # Iterate over the results.
        #
        # @yield [::Google::Cloud::Spanner::BatchWriteResults::BatchResult]
        #
        def each &block
          if defined? @results
            @results.each(&block)
          else
            results = []
            @enumerable.each do |grpc|
              result = BatchResult.new grpc
              results << result
              yield result
            end
            @results = results
          end
        rescue GRPC::BadStatus => e
          raise Google::Cloud::Error.from_error(e)
        end

        include Enumerable

        ##
        # Whether all mutation groups were successful.
        #
        # @return [::Boolean]
        #
        def ok?
          all?(&:ok?)
        end

        ##
        # Whether at least one mutation group encountered an error.
        #
        # @return [::Boolean]
        #
        def error?
          !ok?
        end

        ##
        # A list of the indexes of successful mutation groups.
        #
        # @return [::Array<::Integer>]
        #
        def ok_indexes
          flat_map { |batch_result| batch_result.ok? ? batch_result.indexes : [] }
        end
      end
    end
  end
end
