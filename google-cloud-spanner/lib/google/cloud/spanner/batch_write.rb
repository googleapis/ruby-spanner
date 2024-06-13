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
      ##
      # Part of the BatchWrite DSL. This object is passed as a parameter to the block
      # passed to {Google::Cloud::Spanner::Client#batch_write}. Use this object to add
      # mutation groups to the batch.
      #
      class BatchWrite
        # @private
        def initialize
          @mutation_groups = []
        end

        ##
        # Adds a groups of mutations
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.batch_write do |b|
        #     # First mutation group
        #     b.mutation_group do |mg|
        #       mg.upsert "Singers", [{ SingerId: 16, FirstName: "Charlie", LastName: "Terry" }]
        #     end
        #
        #     # Second mutation group
        #     b.mutation_group do |mg|
        #       mg.upsert "Singers", [{ SingerId: 17, FirstName: "Catalina", LastName: "Smith" }]
        #       mg.update "Albums", [{ SingerId: 17, AlbumId: 1, AlbumTitle: "Go Go Go" }]
        #     end
        #   end
        #
        def mutation_group
          mg = MutationGroup.new
          yield mg
          @mutation_groups << mg
        end

        # @private
        def mutation_groups_grpc
          @mutation_groups.map do |mg|
            Google::Cloud::Spanner::V1::BatchWriteRequest::MutationGroup.new mutations: mg.mutations
          end
        end
      end
    end
  end
end
