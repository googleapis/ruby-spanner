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

require "google/cloud/spanner/mutation_group"
require "google/cloud/spanner/v1"

module Google
  module Cloud
    module Spanner
      class BatchWrite
        # @private
        def initialize
          @mutation_groups = []
        end

        ##
        # Adds a groups of mutations
        def mutation_group
          mg = MutationGroup.new
          yield mg
          @mutation_groups << mg
        end

        def mutation_groups_grpc
          @mutation_groups.map do |mg|
            Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new(mutations: mg.mutations)
          end
        end
      end
    end
  end
end
