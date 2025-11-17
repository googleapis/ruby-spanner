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

require "helper"

describe Google::Cloud::Spanner::Service, :get_sessions, :mock_spanner do
  let(:instance_id) { "my-instance-id" }
  let(:database_id) { "my-database-id" }
  let(:session_id) { "session123" }
  let(:session_full_path) { session_path(instance_id, database_id, session_id) }
  let(:service) {spanner.service}
  
  it "does not send header x-goog-spanner-route-to-leader when LAR is disabled" do
    mock = Minitest::Mock.new
    mock.expect :get_session, nil do |request, gapic_options|
      !gapic_options.metadata.key? "x-goog-spanner-route-to-leader"
    end
    service.mocked_service = mock
    service.enable_leader_aware_routing = false
    service.get_session session_full_path

    mock.verify
  end

  it "sends header x-goog-spanner-route-to-leader when LAR is enabled" do
    mock = Minitest::Mock.new
    mock.expect :get_session, nil do |request, gapic_options|
      gapic_options.metadata["x-goog-spanner-route-to-leader"] == 'true'
    end
    service.mocked_service = mock
    service.enable_leader_aware_routing = true

    service.get_session session_full_path

    mock.verify
  end
end
