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
      class Interval
        NANOSECONDS_IN_A_SECOND = 1000000000
        NANOSECONDS_IN_A_MINUTE = NANOSECONDS_IN_A_SECOND * 60
        NANOSECONDS_IN_AN_HOUR = NANOSECONDS_IN_A_MINUTE * 60
        YEARS = 0
        MONTHS = 1
        DAYS = 2
        HOURS = 4
        MINUTES = 5
        SECONDS = 6

        def self.fromIso8601 interval_string
          pattern = /
            (?!$)(-?\d+Y)?(-?\d+M)?(-?\d+D)?(T(?=-?.?\d)(-?\d+H)?(-?\d+M)?(-?(((\d*)((\.|,)\d{1,9})?)|(\.\d{1,9}))S)?)?$
            /
          interval_months = 0
          interval_days = 0
          interval_nanoseconds = 0

          captures = interval_string.match(pattern).captures
          if captures.empty?
            raise ArgumentError, "The ISO8601 provided was not in the correct format"
          end

          filter = Set[YEARS, MONTHS, DAYS, HOURS, MINUTES, SECONDS]
          captures.each_with_index do |value, index|
            unless filter.include? index
              next
            end

            numeric_value = value.gsub(/[^0-9.,-]/, "")

            case index
            when YEARS
              interval_months += Interval.years_to_months numeric_value
            when MONTHS
              interval_months += Integer numeric_value
            when DAYS
              interval_days = Integer numeric_value
            when HOURS
              interval_nanoseconds += Interval.hours_to_nanoseconds numeric_value
            when MINUTES
              interval_nanoseconds += Interval.minutes_to_nanoseconds numeric_value
            when SECONDS
              interval_nanoseconds += Interval.seconds_to_nanoseconds numeric_value
            end
          end

          Interval.new interval_months, interval_days, interval_nanoseconds
        end

        def self.years_to_months years
          Integer(years) * 12
        end

        def self.hours_to_nanoseconds hours
          Integer(hours) * NANOSECONDS_IN_AN_HOUR
        end

        def self.minutes_to_nanoseconds minutes
          Integer(minutes) * NANOSECONDS_IN_A_MINUTE
        end

        def self.seconds_to_nanoseconds seconds
          # We only support up to nanoseconds of precision
          split_seconds = seconds.split "."
          if split_seconds.length > 2 && split_seconds[1].length > 9
            raise ArgumentError, "The seconds portion of the interval only supports up to nanoseconds."
          end

          Float(seconds) * NANOSECONDS_IN_A_SECOND
        end

        def self.from_months months
          Interval.new months, 0, 0
        end

        def self.from_days days
          Interval.new 0, days, 0
        end

        def self.from_seconds seconds
          nanoseconds = Interval.seconds_to_nanoseconds seconds
          Interval.new 0, 0, nanoseconds
        end

        def self.from_nanoseconds nanoseconds
          Interval.new 0, 0, nanoseconds
        end

        def to_s
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

        private

        def initialize months, days, nanoseconds
          @months = months
          @days = days
          @nanoseconds = nanoseconds
        end
      end
    end
  end
end
