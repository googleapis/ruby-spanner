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
      class MutationGroup
        # @private Array of `Google::Cloud::Spanner:V1::Mutation`
        attr_reader :mutations

        def initialize
          @mutations = []
        end

        def upsert table, *rows
          rows = Array(rows).flatten
          return rows if rows.empty?
          rows.compact
          rows.delete_if(&:empty?)
          @mutations += rows.map do |row|
            V1::Mutation.new(
              insert_or_update: V1::Mutation::Write.new(
                table: table, columns: row.keys.map(&:to_s),
                values: [Convert.object_to_grpc_value(row.values).list_value]
              )
            )
          end
          rows
        end
      end
    end
  end
end
