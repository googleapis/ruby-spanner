# Copyright 2017 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License")
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
      # # Interval
      #
      # Represents an interval of time by storing the time components
      # in months, days and nanoseconds.
      #
      # @example
      #   require "google/cloud/spanner"
      #
      #   iso_8601_string = "P1Y2M3DT4H5M6S"
      #   interval = Google::Cloud::Spanner::Interval::parse iso_8601_string
      #
      #   puts interval # "P1Y2M3DT4H5M6S"
      class Interval
        NANOSECONDS_IN_A_SECOND = 1_000_000_000
        NANOSECONDS_IN_A_MINUTE = NANOSECONDS_IN_A_SECOND * 60
        NANOSECONDS_IN_AN_HOUR = NANOSECONDS_IN_A_MINUTE * 60
        NANOSECONDS_IN_A_MILLISECOND = 1_000_000
        NANOSECONDS_IN_A_MICROSECOND = 1_000
        MAX_MONTHS = 120_000
        MIN_MONTHS = -MAX_MONTHS
        MAX_DAYS = 3_660_000
        MIN_DAYS = -MAX_DAYS
        MAX_NANOSECONDS = 316_224_000_000_000_000_000
        MIN_NANOSECONDS = -316_224_000_000_000_000_000

        private_constant :NANOSECONDS_IN_A_SECOND, :NANOSECONDS_IN_A_MINUTE, :NANOSECONDS_IN_AN_HOUR,
                         :NANOSECONDS_IN_A_MILLISECOND, :NANOSECONDS_IN_A_MICROSECOND, :MAX_MONTHS,
                         :MIN_MONTHS, :MAX_DAYS, :MIN_DAYS, :MAX_NANOSECONDS, :MIN_NANOSECONDS

        class << self
          # Parses an ISO8601 string and returns an Interval instance.
          #
          # The accepted format for the ISO8601 standard is:
          # `P[n]Y[n]M[n]DT[n]H[n]M[n[.fraction]]S`
          # where `n` represents an integer number.
          #
          # @param [String] An ISO8601 formatted string.
          # @return [Google::Cloud::Spanner::Interval]
          #
          # @example
          #   require "google/cloud/spanner"
          #
          #   iso_8601_string = "P1Y2M3DT4H5M6S"
          #   interval = Google::Cloud::Spanner::Interval::parse iso_8601_string
          #
          #   puts interval # "P1Y2M3DT4H5M6S"
          def parse interval_string
            pattern = /^P(?!$)((?<years>-?\d+)Y)?((?<months>-?\d+)M)?((?<days>-?\d+)D)?(T(?!$)((?<hours>-?\d+)H)?((?<minutes>-?\d+)M)?((?<seconds>-?(?!S)\d*([\.,]\d{1,9})?)S)?)?$/
            interval_months = 0
            interval_days = 0
            interval_nanoseconds = 0

            matches = interval_string.match pattern

            if matches.nil?
              raise ArgumentError, "The provided string does not follow ISO8601 standard."
            end

            if matches.captures.empty?
              raise ArgumentError, "The provided string does not follow ISO8601 standard."
            end

            if matches[:years]
              interval_months += matches[:years].to_i * 12
            end

            if matches[:months]
              interval_months += matches[:months].to_i
            end

            if matches[:days]
              interval_days = matches[:days].to_i
            end

            if matches[:hours]
              interval_nanoseconds += matches[:hours].to_i * NANOSECONDS_IN_AN_HOUR
            end

            if matches[:minutes]
              interval_nanoseconds += matches[:minutes].to_i * NANOSECONDS_IN_A_MINUTE
            end

            # Only seconds can be fractional. Both period and comma are valid inputs.
            if matches[:seconds]
              interval_nanoseconds += (matches[:seconds].gsub(",", ".").to_f * NANOSECONDS_IN_A_SECOND).to_i
            end

            Interval.new interval_months, interval_days, interval_nanoseconds
          end

          # Returns an Interval instance with the given months.
          #
          # @param [Integer]
          # @return [Interval]
          def from_months months
            Interval.new months, 0, 0
          end

          # Returns an Interval instance with the given days.
          #
          # @param [Integer]
          # @return [Interval]
          def from_days days
            Interval.new 0, days, 0
          end

          # Returns an Interval instance with the given seconds.
          #
          # @param [Integer]
          # @return [Interval]
          def from_seconds seconds
            nanoseconds = seconds * NANOSECONDS_IN_A_SECOND
            Interval.new 0, 0, nanoseconds
          end

          # Returns an Interval instance with the given milliseconds.
          #
          # @param [Integer]
          # @return [Interval]
          def from_milliseconds milliseconds
            nanoseconds = milliseconds * NANOSECONDS_IN_A_MILLISECOND
            Interval.new 0, 0, nanoseconds
          end

          # Returns an Interval instance with the given microseconds.
          #
          # @param [Integer]
          # @return [Interval]
          def from_microseconds microseconds
            nanoseconds = microseconds * NANOSECONDS_IN_A_MICROSECOND
            Interval.new 0, 0, nanoseconds
          end

          # Returns an Interval instance with the given nanoseconds.
          #
          # @param [Integer]
          # @return [Interval]
          def from_nanoseconds nanoseconds
            Interval.new 0, 0, nanoseconds
          end
        end


        # Converts the [Interval] to an ISO8601 Standard string.
        # @return [String] The interval's ISO8601 string representation.
        def to_s
          # Memoizing it as the logic can be a bit heavy.
          @to_s ||= to_string
        end

        ##
        # @private Creates a new Google::Cloud::Spanner instance.
        def initialize months, days, nanoseconds
          if months > MAX_MONTHS || months < MIN_MONTHS
            raise ArgumentError, "The Interval class supports months from #{MIN_MONTHS} to #{MAX_MONTHS}."
          end
          @months = months

          if days > MAX_DAYS || days < MIN_DAYS
            raise ArgumentError, "The Interval class supports days from #{MIN_DAYS} to #{MAX_DAYS}."
          end
          @days = days

          if nanoseconds > MAX_NANOSECONDS || nanoseconds < MIN_NANOSECONDS
            raise ArgumentError, "The Interval class supports nanoseconds from #{MIN_NANOSECONDS} to #{MAX_NANOSECONDS}"
          end
          @nanoseconds = nanoseconds
        end


        # @return [Integer] The numbers of months in the time interval.
        attr_reader :months

        # @return [Integer] The numbers of days in the time interval.
        attr_reader :days

        # @return [Integer] The numbers of nanoseconds in the time interval.
        attr_reader :nanoseconds


        ##
        # Standard value equality check for this object.
        #
        # @param [Object] other An object to compare with.
        # @return [Boolean]
        def eql? other
          other.is_a?(Interval) &&
            months == other.months &&
            days == other.days &&
            nanoseconds == other.nanoseconds
        end
        alias == eql?

        ##
        # Generate standard hash code for this object.
        #
        # @return [Integer]
        #
        def hash
          @hash ||= [@months, @days, @nanoseconds].hash
        end

        private

        def match_sign value
          value.negative? ? -1 : 1
        end

        # Converts [Interval] to an ISO8601 Standard string.
        # @return [String] The interval's ISO8601 string representation.
        def to_string
          # Months should be converted to years and months.
          years = @months.fdiv(12).truncate
          months = @months % (match_sign(@months) * 12)

          days = @days

          # Nanoseconds should be converted to hours, minutes and seconds components.
          remaining_nanoseconds = @nanoseconds

          hours = (remaining_nanoseconds.abs / NANOSECONDS_IN_AN_HOUR) * match_sign(remaining_nanoseconds)
          remaining_nanoseconds %= (match_sign(remaining_nanoseconds) * NANOSECONDS_IN_AN_HOUR)
          minutes = (remaining_nanoseconds.abs / NANOSECONDS_IN_A_MINUTE) * match_sign(remaining_nanoseconds)
          remaining_nanoseconds %= (match_sign(remaining_nanoseconds) * NANOSECONDS_IN_A_MINUTE)

          # Only seconds can be fractional, and can have a maximum of 9 characters after decimal. Therefore,
          # we convert the remaining nanoseconds to an integer for formatting.
          seconds = (remaining_nanoseconds.abs / NANOSECONDS_IN_A_SECOND) * match_sign(remaining_nanoseconds)
          nanoseconds = remaining_nanoseconds % (match_sign(remaining_nanoseconds) * NANOSECONDS_IN_A_SECOND)

          interval_string = ["P"]

          if years != 0
            interval_string.append "#{years}Y"
          end

          if months != 0
            interval_string.append "#{months}M"
          end

          if days != 0
            interval_string.append "#{days}D"
          end

          if hours != 0 || minutes != 0 || seconds != 0 || nanoseconds != 0
            interval_string.append "T"

            if hours != 0
              interval_string.append "#{hours}H"
            end

            if minutes != 0
              interval_string.append "#{minutes}M"
            end

            if seconds != 0 || nanoseconds != 0
              interval_string.append "#{format_seconds seconds, nanoseconds}S"
            end
          end

          if interval_string == ["P"]
            return "P0Y"
          end

          interval_string.join
        end

        # Formats decimal values to be in multiples of 3 length.
        # @return [String]
        def format_seconds seconds, nanoseconds
          return seconds if nanoseconds.zero?
          add_sign = seconds.zero? && nanoseconds.negative?

          nanoseconds_str = nanoseconds.abs.to_s.rjust 9, "0"
          nanoseconds_str = nanoseconds_str.gsub(/0+$/, "")

          target_length =
            if nanoseconds_str.length <= 3
              3
            elsif nanoseconds_str.length <= 6
              6
            else
              9
            end

          nanoseconds_str = (nanoseconds_str + ("0" * target_length))[0...target_length]
          "#{add_sign ? '-' : ''}#{seconds}.#{nanoseconds_str}"
        end
      end
    end
  end
end
