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
      #   interval = Google::Cloud::Spanner::Interval::parse(iso_8601_string)
      #
      #   print(interval) # "P1Y2M3DT4H5M6S"
      #
      class Interval
        NANOSECONDS_IN_A_SECOND = 1000000000
        NANOSECONDS_IN_A_MINUTE = NANOSECONDS_IN_A_SECOND * 60
        NANOSECONDS_IN_AN_HOUR = NANOSECONDS_IN_A_MINUTE * 60
        NANOSECONDS_IN_A_MILLISECOND = 1000000
        NANOSECONDS_IN_A_MICROSECOND = 1000
        MAX_MONTHS = 120000
        MIN_MONTHS = -MAX_MONTHS
        MAX_DAYS = 3660000
        MIN_DAYS = -MAX_DAYS
        MAX_NANOSECONDS = 316224000000000000000
        MIN_NANOSECONDS = -316224000000000000000

        private_constant :NANOSECONDS_IN_A_SECOND, :NANOSECONDS_IN_A_MINUTE, :NANOSECONDS_IN_AN_HOUR, :NANOSECONDS_IN_A_MILLISECOND, :NANOSECONDS_IN_A_MICROSECOND, :MAX_MONTHS, :MIN_MONTHS, :MAX_DAYS, :MIN_DAYS, :MAX_NANOSECONDS, :MIN_NANOSECONDS

        class << self
          # Parses an ISO8601 string and returns an Interval instance.
          # The accepted format for the ISO8601 format is:
          # P[n]Y[n]M[n]DT[n]H[n]M[n[.fraction]]S
          # where n represents an integer number.
          #
          # @param [String] An ISO8601 formatted string.
          # @return [Interval]
          #
          # @example
          #   require "google/cloud/spanner"
          #
          #   iso_8601_string = "P1Y2M3DT4H5M6S"
          #   interval = Google::Cloud::Spanner::Interval::parse(iso_8601_string)
          #
          #   print(interval) # "P1Y2M3DT4H5M6S"
          def parse interval_string
            pattern = /(?!$)(?<years>-?\d+Y)?(?<months>-?\d+M)?(?<days>-?\d+D)?(T(?=-?.?\d)(?<hours>-?\d+H)?(?<minutes>-?\d+M)?(?<seconds>-?(((\d*)((\.|,)\d{1,9})?)|(\.\d{1,9}))S)?)?$/
            interval_months = 0
            interval_days = 0
            interval_nanoseconds = 0

            matches = interval_string.match(pattern)
            if matches.captures.empty?
              raise ArgumentError, "The ISO8601 provided was not in the correct format"
            end

            if matches[:years]
              interval_months += self::years_to_months matches[:years].to_i
            end

            if matches[:months]
              interval_months += matches[:months].to_i
            end

            if matches[:days]
              interval_days = matches[:days].to_i
            end

            if matches[:hours]
              interval_nanoseconds += self::hours_to_nanoseconds matches[:hours].to_i
            end

            if matches[:minutes]
              interval_nanoseconds += self::minutes_to_nanoseconds matches[:minutes].to_i
            end

            if matches[:seconds]
              interval_nanoseconds += self::seconds_to_nanoseconds matches[:seconds].to_f
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
            nanoseconds = Interval.seconds_to_nanoseconds seconds
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

          private

          def years_to_months years
            Integer(years) * 12
          end

          def hours_to_nanoseconds hours
            Integer(hours) * NANOSECONDS_IN_AN_HOUR
          end

          def minutes_to_nanoseconds minutes
            Integer(minutes) * NANOSECONDS_IN_A_MINUTE
          end

          def seconds_to_nanoseconds seconds
            # We only support up to nanoseconds of precision
            split_seconds = seconds.to_s.split '.'
            if split_seconds.length > 2 || split_seconds.length > 1 && split_seconds[1].length > 9
              raise ArgumentError, "The seconds portion of the interval only supports up to nanoseconds."
            end

            Float(seconds) * NANOSECONDS_IN_A_SECOND
          end
        end

        def to_s
          # Memoizing it as the logic can be a bit heavy
          @string_representation ||= self.to_string
        end

        private

        def initialize months, days, nanoseconds
          if (months > MAX_MONTHS || months < MIN_MONTHS)
            raise ArgumentError, "The Interval class supports months from #{MIN_MONTHS} to #{MAX_MONTHS}."
          end
          @months = months

          if (days > MAX_DAYS || days < MIN_DAYS)
            raise ArgumentError, "The Interval class supports days from #{MIN_DAYS} to #{MAX_DAYS}."
          end
          @days = days

          if (nanoseconds > MAX_NANOSECONDS || nanoseconds < MIN_NANOSECONDS)
            raise ArgumentError, "The Interval class supports nanoseconds from #{MIN_NANOSECONDS} to #{MAX_NANOSECONDS}"
          end
          @nanoseconds = nanoseconds
        end

        def to_string
          years = 0
          months = 0
          days = @days
          hours = 0
          minutes = 0
          seconds = 0
          remaining_nanoseconds = @nanoseconds

          years = @months / 12
          months = @months % 12
          hours = Integer(remaining_nanoseconds / 3_600_000_000_000)
          remaining_nanoseconds %= 3_600_000_000_000
          minutes = Integer(remaining_nanoseconds / 60_000_000_000)
          remaining_nanoseconds %= 60_000_000_000
          seconds = remaining_nanoseconds / 1_000_000_000

          interval_string = "P"

          if years != 0
            interval_string += "#{years}Y"
          end

          if months != 0
            interval_string += "#{months}M"
          end

          if days != 0
            interval_string += "#{days}D"
          end

          if hours != 0 || minutes != 0 || seconds != 0
            interval_string += "T"

            if hours != 0
              interval_string += "#{hours}H"
            end

            if minutes != 0
              interval_string += "#{minutes}M"
            end

            if seconds != 0
              if (seconds % 1).zero?
                interval_string += "#{Integer(seconds)}S"
              else
                interval_string += "#{seconds}S"
              end
            end
          end

          if interval_string == "P"
            return "P0Y"
          end

          interval_string
        end
      end
    end
  end
end
