# Copyright 2016 Google LLC
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


require "google/cloud/spanner/errors"
require "google/cloud/spanner/data"

module Google
  module Cloud
    module Spanner
      ##
      # # Results
      #
      # Represents the result set from an operation returning data.
      #
      # See {Google::Cloud::Spanner::Client#execute_query} and
      # {Google::Cloud::Spanner::Client#read}.
      #
      # @example
      #   require "google/cloud/spanner"
      #
      #   spanner = Google::Cloud::Spanner.new
      #
      #   db = spanner.client "my-instance", "my-database"
      #
      #   results = db.execute_query "SELECT * FROM users"
      #
      #   results.fields.pairs.each do |name, type|
      #     puts "Column #{name} is type #{type}"
      #   end
      #
      class Results
        ##
        # @private Object of type
        # Google::Cloud::Spanner::V1::ResultSetMetadata
        attr_reader :metadata

        ##
        # The read timestamp chosen for single-use snapshots (read-only
        # transactions).
        # @return [Time] The chosen timestamp.
        def timestamp
          return nil if @metadata.nil? || @metadata.transaction.nil?
          Convert.timestamp_to_time @metadata.transaction.read_timestamp
        end

        ##
        # Returns the configuration object ({Fields}) of the names and types of
        # the rows in the returned data.
        #
        # @return [Fields] The fields of the returned data.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.execute_query "SELECT * FROM users"
        #
        #   results.fields.pairs.each do |name, type|
        #     puts "Column #{name} is type #{type}"
        #   end
        #
        def fields
          @fields ||= Fields.from_grpc @metadata.row_type.fields
        end

        ##
        # @private
        # Returns a transaction if available
        def transaction
          @metadata&.transaction
        end

        # rubocop:disable all

        ##
        # The values returned from the request.
        #
        # @yield [row] An enumerator for the rows.
        # @yieldparam [Data] row object that contains the data values.
        #
        # @example
        #   require "google/cloud/spanner"
        #
        #   spanner = Google::Cloud::Spanner.new
        #
        #   db = spanner.client "my-instance", "my-database"
        #
        #   results = db.execute_query "SELECT * FROM users"
        #
        #   results.rows.each do |row|
        #     puts "User #{row[:id]} is #{row[:name]}"
        #   end
        #
        def rows
          return nil if @closed

          unless block_given?
            return enum_for(:rows)
          end

          fields = @metadata.row_type.fields
          values = []
          buffered_responses = []
          buffer_upper_bound = 10
          chunked_value = nil
          resume_token = nil
          should_resume_request = false
          should_retry_request = false

          # Cannot call Enumerator#each because it won't return the first
          # value that was already identified when calling Enumerator#peek.
          # Iterate only using Enumerator#next and break on StopIteration.
          loop do
            begin
              if should_resume_request
                @enum = resume_request(resume_token)
                buffered_responses = []
                should_resume_request = false
              elsif should_retry_request
                @enum = retry_request()
                buffered_responses = []
                should_retry_request = false
              end

              grpc = @enum.next
              # metadata should be set before the first iteration...
              @metadata ||= grpc.metadata
              @stats ||= grpc.stats

              buffered_responses << grpc

              if (grpc.resume_token && grpc.resume_token != "") ||
                buffered_responses.size >= buffer_upper_bound
                # This can set the resume_token to nil
                resume_token = grpc.resume_token

                if fields.count > 0
                  buffered_responses.each do |resp|
                    if chunked_value
                      resp.values.unshift merge(chunked_value, resp.values.shift)
                      chunked_value = nil
                    end
                    to_iterate = values + Array(resp.values)
                    chunked_value = to_iterate.pop if resp.chunked_value
                    values = to_iterate.pop(to_iterate.count % fields.count)
                    to_iterate.each_slice(fields.count) do |slice|
                      yield Data.from_grpc(slice, fields)
                    end
                  end
                end

                # Flush the buffered responses now that they are all handled
                buffered_responses = []
              end
            # TODO: once the generated client throws only Google Cloud errors, remove
            # the GRPC errors from the rescue block
            rescue GRPC::Aborted,
              GRPC::Cancelled,
              GRPC::DeadlineExceeded,
              GRPC::Internal,
              GRPC::ResourceExhausted,
              GRPC::Unauthenticated,
              GRPC::Unavailable,
              GRPC::Core::CallError,
              Google::Cloud::AbortedError,
              Google::Cloud::CanceledError,
              Google::Cloud::DeadlineExceededError,
              Google::Cloud::InternalError,
              Google::Cloud::ResourceExhaustedError,
              Google::Cloud::UnauthenticatedError,
              Google::Cloud::UnavailableError => err

              if resumable?(resume_token)
                should_resume_request = true
              elsif @service.retryable?(err)
                should_retry_request = true
              elsif err.is_a?(Google::Cloud::Error)
                raise err
              else
                raise Google::Cloud::Error.from_error(err)
              end

            # TODO: once the generated client throws only Google Cloud errors, remove
            # this rescue block (for GRPC::BadStatus)
            rescue GRPC::BadStatus => err
              raise Google::Cloud::Error.from_error(err)
            rescue StopIteration
              break
            end
          end

          # clear out any remaining values left over
          if fields.count > 0
            buffered_responses.each do |resp|
              if chunked_value
                resp.values.unshift merge(chunked_value, resp.values.shift)
                chunked_value = nil
              end
              to_iterate = values + Array(resp.values)
              chunked_value = to_iterate.pop if resp.chunked_value
              values = to_iterate.pop(to_iterate.count % fields.count)
              to_iterate.each_slice(fields.count) do |slice|
                yield Data.from_grpc(slice, fields)
              end
            end
            values.each_slice(fields.count) do |slice|
              yield Data.from_grpc(slice, fields)
            end
          end

          # If we get this far then we can release the session
          @closed = true
          nil
        end

        # rubocop:enable all

        ##
        # @private
        # Checks if a request can be resumed by inspecting the resume token
        def resumable? resume_token
          resume_token && !resume_token.empty?
        end

        ##
        # @private
        # Resumes a request, by re-executing it with a resume token.
        def resume_request resume_token
          if @execute_query_options
            @service.execute_streaming_sql(
              @session_path,
              @sql,
              **@execute_query_options.merge(resume_token: resume_token)
            )
          else
            @service.streaming_read_table(
              @session_path,
              @table,
              @columns,
              **@read_options.merge(resume_token: resume_token)
            )
          end
        end

        ##
        # @private
        # Retries a request, by re-executing it from scratch.
        def retry_request
          if @execute_query_options
            @service.execute_streaming_sql @session_path, @sql, **@execute_query_options
          else
            @service.streaming_read_table @session_path, @table, @columns, **@read_options
          end
        end

        ##
        # @private
        # Get row count from stats. This will be the exact row count for DML
        # statements, and the lower bound row count for PDML statements.
        def row_count
          return @stats.row_count_lower_bound if row_count_lower_bound?
          return @stats.row_count_exact       if row_count_exact?
          nil
        end

        ##
        # @private
        # Whether the row count is the lower bound row count for PDML
        # statements.
        def row_count_lower_bound?
          return nil if @stats.nil?
          @stats.row_count == :row_count_lower_bound
        end

        ##
        # @private
        # Whether the row count is the exact row count for DML statements.
        def row_count_exact?
          return nil if @stats.nil?
          @stats.row_count == :row_count_exact
        end

        # @private
        def self.from_enum enum, service
          grpc = enum.peek
          new.tap do |results|
            results.instance_variable_set :@metadata, grpc.metadata
            results.instance_variable_set :@stats,    grpc.stats
            results.instance_variable_set :@enum,     enum
            results.instance_variable_set :@service,  service
          end
        rescue GRPC::BadStatus => e
          raise Google::Cloud::Error.from_error(e)
        end

        # @private
        def self.from_execute_query_response response, service, session_path, sql, execute_query_options
          from_enum(response, service).tap do |results|
            results.instance_variable_set :@session_path, session_path
            results.instance_variable_set :@sql, sql
            results.instance_variable_set :@execute_query_options, execute_query_options
          end
        end

        # @private
        def self.from_read_response response, service, session_path, table, columns, read_options
          from_enum(response, service).tap do |results|
            results.instance_variable_set :@session_path, session_path
            results.instance_variable_set :@table, table
            results.instance_variable_set :@columns, columns
            results.instance_variable_set :@read_options, read_options
          end
        end

        # @private
        def to_s
          "(#{fields.inspect} streaming)"
        end

        # @private
        def inspect
          "#<#{self.class.name} #{self}>"
        end

        protected

        # rubocop:disable all

        # @private
        def merge left, right
          if left.kind != right.kind
            raise "Can't merge #{left.kind} and #{right.kind} values"
          end
          if left.kind == :string_value
            left.string_value = left.string_value + right.string_value
            return left
          elsif left.kind == :list_value
            left_val = left.list_value.values.pop
            right_val = right.list_value.values.shift
            if (left_val.kind == right_val.kind) &&
               (left_val.kind == :list_value || left_val.kind == :string_value)
              left.list_value.values << merge(left_val, right_val)
            else
              left.list_value.values << left_val
              left.list_value.values << right_val
            end
            right.list_value.values.each { |val| left.list_value.values << val }
            return left
          elsif left.kind == :struct_value
            # Don't worry about this yet since Spanner isn't return STRUCT
            raise "STRUCT not implemented yet"
          else
            raise "Can't merge #{left.kind} values"
          end
        end

        # rubocop:enable all
      end
    end
  end
end
