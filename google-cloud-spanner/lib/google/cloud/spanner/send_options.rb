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
      # # SendOptions
      #
      # Optional settings for sending a message to a queue with
      # {Commit#send_message} or {Transaction#send_message}. A hash with the
      # same keys can be passed instead.
      #
      # @example
      #   require "google/cloud/spanner"
      #
      #   spanner = Google::Cloud::Spanner.new
      #
      #   db = spanner.client "my-instance", "my-database"
      #
      #   send_options = Google::Cloud::Spanner::SendOptions.new deliver_time: Time.now + 300
      #
      #   db.commit do |c|
      #     c.send_message "OrderQueue", 101, StringIO.new("Order payload"),
      #                    send_options: send_options
      #   end
      #
      class SendOptions
        ##
        # The time at which Spanner begins trying to deliver the message.
        #
        # @return [Time, nil]
        attr_reader :deliver_time

        ##
        # Creates a new SendOptions.
        #
        # @param [Time, nil] deliver_time The time at which Spanner begins
        #   trying to deliver the message. Optional. If not set, the message
        #   is delivered immediately. A time in the past is replaced with a
        #   time closer to the current time.
        #
        def initialize deliver_time: nil
          @deliver_time = deliver_time
        end
      end
    end
  end
end
