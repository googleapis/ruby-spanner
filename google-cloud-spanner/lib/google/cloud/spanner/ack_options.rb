# Copyright 2026 Google LLC
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
      # # AckOptions
      #
      # Optional settings for acknowledging a queue message with
      # {Commit#ack_message} or {Transaction#ack_message}. A hash with the
      # same keys can be passed instead.
      #
      # @example
      #   require "google/cloud/spanner"
      #
      #   spanner = Google::Cloud::Spanner.new
      #
      #   db = spanner.client "my-instance", "my-database"
      #
      #   ack_options = Google::Cloud::Spanner::AckOptions.new ignore_not_found: true
      #
      #   db.commit do |c|
      #     c.ack_message "OrderQueue", 101, ack_options: ack_options
      #   end
      #
      class AckOptions
        ##
        # Whether the ack succeeds when the message doesn't exist.
        #
        # @return [Boolean]
        attr_reader :ignore_not_found

        ##
        # Creates a new AckOptions.
        #
        # @param [Boolean] ignore_not_found If `true`, the ack succeeds even
        #   if the message doesn't exist. Optional. The default is `false`,
        #   which fails the commit with `NOT_FOUND` when the message doesn't
        #   exist.
        #
        def initialize ignore_not_found: false
          @ignore_not_found = ignore_not_found
        end
      end
    end
  end
end
