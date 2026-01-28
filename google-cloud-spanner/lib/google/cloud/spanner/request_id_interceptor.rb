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


require "grpc"
require "securerandom"
require "mutex_m"
require "google/cloud/spanner/errors"

module Google
  module Cloud
    module Spanner
      class RequestIdInterceptor < GRPC::ClientInterceptor
        @client_id_counter = 0
        @client_mutex = Mutex.new
        @channel_id_counter = 0
        @channel_mutex = Mutex.new
        @request_id_counter = 0
        @request_id_mutex = Mutex.new
        @process_id = nil
        @process_id_mutex = Mutex.new

        def self.next_client_id
          @client_mutex.synchronize do
            @client_id_counter += 1
          end
        end

        def self.next_channel_id
          @channel_mutex.synchronize do
            @channel_id_counter += 1
          end
        end

        def self.get_process_id process_id = nil
          @process_id_mutex.synchronize do
            if process_id.nil? || !@process_id.nil?
              return @process_id ||= (SecureRandom.hex 8)
            end

            case process_id
            when Integer
              if process_id >= 0 && process_id.bit_length <= 64
                return process_id.to_s(16).rjust(16, "0")
              end
            when String
              if process_id =~ /\A[0-9a-fA-F]{16}\z/
                return process_id
              end
            end

            raise ArgumentError, "process_id must be a 64-bit integer or 16-character hex string"
          end
        end

        def initialize process_id: nil
          super
          @version = 1
          @process_id = self.class.get_process_id process_id
          @client_id = self.class.next_client_id
          @channel_id = self.class.next_channel_id
          @request_id_counter = 0
          @request_mutex = Mutex.new
        end

        def request_response method:, request:, call:, metadata:, &block
          # Unused. This is to avoid Rubocop's Lint/UnusedMethodArgument
          _method = method
          _request = request
          _call = call
          update_metadata_for_call metadata, &block
        end

        def client_streamer method:, request:, call:, metadata:, &block
          # Unused. This is to avoid Rubocop's Lint/UnusedMethodArgument
          _method = method
          _request = request
          _call = call
          update_metadata_for_call metadata, &block
        end

        def server_streamer method:, request:, call:, metadata:, &block
          # Unused. This is to avoid Rubocop's Lint/UnusedMethodArgument
          _method = method
          _request = request
          _call = call
          update_metadata_for_call metadata, &block
        end

        def bidi_streamer method:, request:, call:, metadata:, &block
          # Unused. This is to avoid Rubocop's Lint/UnusedMethodArgument
          _method = method
          _request = request
          _call = call
          update_metadata_for_call metadata, &block
        end

        private

        def update_metadata_for_call metadata
          request_id = nil
          attempt = 1

          if metadata.include? :"x-goog-spanner-request-id"
            request_id, attempt = get_header_info metadata[:"x-goog-spanner-request-id"]
          else
            request_id = @request_mutex.synchronize { @request_id_counter += 1 }
          end

          formatted_request_id = format_request_id request_id, attempt
          metadata[:"x-goog-spanner-request-id"] = formatted_request_id

          yield
        rescue StandardError => e
          e.instance_variable_set :@spanner_header_id, formatted_request_id
          raise e
        end

        def format_request_id request_id, attempt
          "#{@version}.#{@process_id}.#{@client_id}.#{@channel_id}.#{request_id}.#{attempt}"
        end

        def get_header_info header
          _, _, _, _, request_id, attempt = header.split "."
          [request_id, attempt.to_i + 1]
        end
      end
    end
  end
end
