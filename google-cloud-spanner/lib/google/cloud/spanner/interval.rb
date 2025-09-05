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
          # rubocop:disable Metrics/AbcSize
          # rubocop:disable Metrics/MethodLength

          # Parses an ISO8601 string and returns an Interval instance.
          #
          # The accepted format for the ISO8601 standard is:
          # `P[n]Y[n]M[n]DT[n]H[n]M[n[.fraction]]S`
          # where `n` represents an integer number.
          #
          # @param interval_string [String] An ISO8601 formatted string.
          # @return [Google::Cloud::Spanner::Interval]
          #
          # @example
          #   require "google/cloud/spanner"
          #
          #   iso_8601_string = "P1Y2M3DT4H5M6S"
          #   interval = Google::Cloud::Spanner::Interval::parse iso_8601_string
          #
          #   puts interval # "P1Y2M3DT4H5M6S"
          #
          def parse interval_string
            pattern = /^
              P(?!$)
              (?:(?<years>-?\d+)Y)?
              (?:(?<months>-?\d+)M)?
              (?:(?<days>-?\d+)D)?
              (?:T(?!$)
              (?:(?<hours>-?\d+)H)?
              (?:(?<minutes>-?\d+)M)?
              (?:(?<seconds>-?(?!S)\d*(?:[.,]\d{1,9})?)S)?)?
              $
            /x
            interval_months = 0
            interval_days = 0
            interval_nanoseconds = 0

            matches = interval_string.match pattern

            raise ArgumentError, "The provided string does not follow ISO8601 standard." if matches.nil?

            raise ArgumentError, "The provided string does not follow ISO8601 standard." if matches.captures.empty?

            interval_months += matches[:years].to_i * 12 if matches[:years]

            interval_months += matches[:months].to_i if matches[:months]

            interval_days = matches[:days].to_i if matches[:days]

            interval_nanoseconds += matches[:hours].to_i * NANOSECONDS_IN_AN_HOUR if matches[:hours]

            interval_nanoseconds += matches[:minutes].to_i * NANOSECONDS_IN_A_MINUTE if matches[:minutes]

            # Only seconds can be fractional. Both period and comma are valid inputs.
            if matches[:seconds]
              interval_nanoseconds += (matches[:seconds].gsub(",", ".").to_f * NANOSECONDS_IN_A_SECOND).to_i
            end

            Interval.new interval_months, interval_days, interval_nanoseconds
          end

          # rubocop:enable Metrics/AbcSize
          # rubocop:enable Metrics/MethodLength

          # Returns an Interval instance with the given months.
          #
          # @param months [Integer]
          # @return [Interval]
          def from_months months
            Interval.new months, 0, 0
          end

          # Returns an Interval instance with the given days.
          #
          # @param days [Integer]
          # @return [Interval]
          def from_days days
            Interval.new 0, days, 0
          end

          # Returns an Interval instance with the given seconds.
          #
          # @param seconds [Integer]
          # @return [Interval]
          def from_seconds seconds
            nanoseconds = seconds * NANOSECONDS_IN_A_SECOND
            Interval.new 0, 0, nanoseconds
          end

          # Returns an Interval instance with the given milliseconds.
          #
          # @param milliseconds [Integer]
          # @return [Interval]
          def from_milliseconds milliseconds
            nanoseconds = milliseconds * NANOSECONDS_IN_A_MILLISECOND
            Interval.new 0, 0, nanoseconds
          end

          # Returns an Interval instance with the given microseconds.
          #
          # @param microseconds [Integer]
          # @return [Interval]
          def from_microseconds microseconds
            nanoseconds = microseconds * NANOSECONDS_IN_A_MICROSECOND
            Interval.new 0, 0, nanoseconds
          end

          # Returns an Interval instance with the given nanoseconds.
          #
          # @param nanoseconds [Integer]
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

        # rubocop:disable Metrics/AbcSize
        # rubocop:disable Metrics/CyclomaticComplexity
        # rubocop:disable Metrics/PerceivedComplexity

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

          interval_string.append "#{years}Y" if years.nonzero?

          interval_string.append "#{months}M" if months.nonzero?

          interval_string.append "#{days}D" if days.nonzero?

          if hours.nonzero? || minutes.nonzero? || seconds.nonzero? || nanoseconds.nonzero?
            interval_string.append "T"

            interval_string.append "#{hours}H" if hours.nonzero?

            interval_string.append "#{minutes}M" if minutes.nonzero?

            if seconds.nonzero? || nanoseconds.nonzero?
              interval_string.append "#{format_seconds seconds, nanoseconds}S"
            end
          end

          return "P0Y" if interval_string == ["P"]

          interval_string.join
        end

        # rubocop:enable Metrics/AbcSize
        # rubocop:enable Metrics/CyclomaticComplexity
        # rubocop:enable Metrics/PerceivedComplexity

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
