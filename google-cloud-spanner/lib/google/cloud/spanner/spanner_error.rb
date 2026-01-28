# Copyright 2025 Google LLC
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

# This is a monkey patch for Google::Cloud::Error to add support for the request_id method
# to keep this spanner exclusive method inside the spanner code.
module Google
  module Cloud
    class Error
      ##
      # The Spanner header ID if there was an error on the request.
      #
      # @return [String, nil]
      #
      def request_id
        return nil unless cause.instance_variable_defined? :@spanner_header_id
        cause.instance_variable_get :@spanner_header_id
      end
    end
  end
end
