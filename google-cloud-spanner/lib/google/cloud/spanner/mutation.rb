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

require "google/cloud/spanner/v1"

module Google
  module Cloud
    module Spanner
      class Mutation
        # @private
        attr_reader :grpc

        # @private
        V1 = Google::Cloud::Spanner::V1

        # @private
        def initialize grpc
          @grpc = grpc
        end

        # @private
        def to_grpc
          @grpc
        end

        def self.upsert table, row
          grpc = V1::Mutation.new(
            insert_or_update: V1::Mutation::Write.new(
              table: table, columns: row.keys.map(&:to_s),
              values: [Convert.object_to_grpc_value(row.values).list_value]
            )
          )
          new grpc
        end

        def self.insert table, row
          grpc = V1::Mutation.new(
            insert: V1::Mutation::Write.new(
              table: table, columns: row.keys.map(&:to_s),
              values: [Convert.object_to_grpc_value(row.values).list_value]
            )
          )
          new grpc
        end

        def self.update table, row
          grpc = V1::Mutation.new(
            update: V1::Mutation::Write.new(
              table: table, columns: row.keys.map(&:to_s),
              values: [Convert.object_to_grpc_value(row.values).list_value]
            )
          )
          new grpc
        end

        def self.replace table, row
          grpc = V1::Mutation.new(
            replace: V1::Mutation::Write.new(
              table: table, columns: row.keys.map(&:to_s),
              values: [Convert.object_to_grpc_value(row.values).list_value]
            )
          )
          new grpc
        end

        def self.delete table, keys = []
          grpc = V1::Mutation.new(
            delete: V1::Mutation::Delete.new(
              table: table, key_set: key_set(keys)
            )
          )
          new grpc
        end

        protected

        def self.key_set keys
          return V1::KeySet.new all: true if keys.nil?
          keys = [keys] unless keys.is_a? Array
          return V1::KeySet.new all: true if keys.empty?
          if keys_are_ranges? keys
            key_ranges = keys.map do |r|
              Convert.to_key_range r
            end
            return V1::KeySet.new ranges: key_ranges
          end
          key_list = keys.map do |key|
            key = [key] unless key.is_a? Array
            Convert.object_to_grpc_value(key).list_value
          end
          V1::KeySet.new keys: key_list
        end

        def self.keys_are_ranges? keys
          keys.each do |key|
            return true if key.is_a? ::Range
            return true if key.is_a? Google::Cloud::Spanner::Range
          end
          false
        end
      end
    end
  end
end
