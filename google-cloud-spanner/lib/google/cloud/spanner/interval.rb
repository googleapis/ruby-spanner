# Copyright 2025 Google LLC
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
        MAX_MONTHS = 120000;
        MIN_MONTHS = -Interval::MAX_MONTHS;
        MAX_DAYS = 3660000;
        MIN_DAYS = -Interval::MAX_DAYS;
        MAX_NANOSECONDS = 316224000000000000000;
        MIN_NANOSECONDS = -316224000000000000000;

        IntervalParsingState = Struct.new(
          :after_p,
          :after_y,
          :after_month,
          :after_d,
          :after_t,
          :after_h,
          :after_mins,
          :next_allowed,
          :start,
          :is_time,
          :may_be_terminal,
          :is_terminal,
          :is_valid_resolution,
          :years,
          :months,
          :days,
          :hours,
          :minutes,
          :seconds,
        )

        # Static Methods
        class << self
          def parse text
            if text.nil? || text.empty?
              raise 'The given interval is empty'
            end

            state = IntervalParsingState.new(
              /(Y|M|D|T)/,
              /(M|D|T)/,
              /(D|T)/,
              /(T)/,
              /(H|M|S)/,
              /(M|S)/,
              /(S)/,
              /(P)/,
              0,
              false,
              false,
              false,
              false,
              0,
              0,
              0,
              0,
              0,
              0
            )

            current = -1

            while state.start < text.length && !state.is_terminal do
              current = text.index state.next_allowed, state.start

              if current.nil?
                raise ArgumentError, "unsupported format: #{text}"
              end

              case text[current]
              when 'P'
                state.may_be_terminal = false
                state.is_terminal = false
                state.is_time = false
                state.is_valid_resolution = true
                state.next_allowed = state.after_p
              when 'Y'
                state.may_be_terminal = true;
                state.is_terminal = false;
                state.is_valid_resolution = true;
                state.years = Integer text[state.start, current - state.start]
                state.next_allowed = state.after_y;
              when 'M'
                if state.is_time
                  state.may_be_terminal = true;
                  state.is_terminal = false;
                  state.is_valid_resolution = true;
                  state.minutes = Integer text[state.start, current - state.start]
                  state.next_allowed = state.after_mins;
                else
                  state.may_be_terminal = true;
                  state.is_terminal = false;
                  state.is_valid_resolution = true;
                  state.months = Integer text[state.start, current - state.start]
                  state.next_allowed = state.after_month;
                end
              when 'D'
                state.may_be_terminal = true;
                state.is_terminal = false;
                state.is_valid_resolution = true;
                state.days = Integer text[state.start, current - state.start]
                state.next_allowed = state.after_d;
              when 'T'
                state.may_be_terminal = false;
                state.is_terminal = false;
                state.is_time = true;
                state.is_valid_resolution = true;
                state.next_allowed = state.after_t;
              when 'H'
                state.may_be_terminal = true
                state.is_terminal = false
                state.is_valid_resolution = true
                state.hours = Integer text[state.start, current - state.start]
                state.next_allowed = state.after_h
              when 'S'
                state.may_be_terminal = true
                state.is_terminal = true
                state.is_valid_resolution = self.is_valid_resolution text[state.start, current - state.start]
                state.seconds = Float text[state.start, current - state.start]
                state.next_allowed = nil;
              else
                raise ArgumentError, "unsupported format: #{text}"
              end

              state.start = current + 1
            end

            if state.is_terminal && state.start < text.length
              raise ArgumentError, "unsupported format: #{text}"
            end

            unless state.may_be_terminal
              raise ArgumentError, "unsupported format: #{text}"
            end

            unless state.is_valid_resolution
              raise ArgumentError, 'the interval class only supports a resolution up to nanoseconds'
            end

            total_months = self.years_to_months(state.years) + state.months
            total_nanoseconds = self.hours_to_nanoseconds(state.hours) + self.minutes_to_nanoseconds(state.minutes) + self.seconds_to_nanoseconds(state.seconds)

            Interval.new total_months, state.days, total_nanoseconds
          end

          private

          def years_to_months years
            years * 12
          end

          def hours_to_nanoseconds hours
            hours * self::NANOSECONDS_IN_AN_HOUR
          end

          def minutes_to_nanoseconds minutes
            minutes * self::NANOSECONDS_IN_A_MINUTE
          end

          def seconds_to_nanoseconds seconds
            seconds * self::NANOSECONDS_IN_A_SECOND
          end

          def is_valid_resolution text_value
            integer_value, decimal_value = text_value.gsub(',', '.').split('.')

            # not a decimal, so is valid
            if decimal_value.nil? || decimal_value.empty?
              return true
            end

            # more than 9 digits after the decimal point, not supported
            if decimal_value.length > 9
              return false
            end

            true
          end
        end

        def initialize
          @internal_variable = 1;
        end

        def to_s
          @string_representation ||= self.to_string
        end

        private

        def initialize months, days, nanoseconds
          if months > self.class::MAX_MONTHS || months < self.class::MIN_MONTHS
            raise "The Interval class supports a range from #{self.class::MIN_MONTHS} to #{self.class::MAX_MONTHS} months"
          end
          @months = months

          if days > self.class::MAX_DAYS || days < self.class::MIN_DAYS
            raise "The Interval class supports a range from #{self.class::MIN_MONTHS} to #{self.class::MAX_MONTHS} days"
          end
          @days = days

          if nanoseconds > self.class::MAX_NANOSECONDS || nanoseconds < self.class::MIN_NANOSECONDS
            raise "The Interval class supports a range from #{self.class::MIN_NANOSECONDS} to #{self.class::MAX_NANOSECONDS} nanoseconds"
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

          years, months = @months.divmod 12
          hours, remaining_nanoseconds = remaining_nanoseconds.divmod self.class::NANOSECONDS_IN_AN_HOUR
          minutes, remaining_nanoseconds = remaining_nanoseconds.divmod self.class::NANOSECONDS_IN_A_MINUTE
          seconds = remaining_nanoseconds / self.class::NANOSECONDS_IN_A_SECOND

          interval_string = 'P';

          if years != 0
            interval_string += "#{years}Y";
          end

          if months != 0
            interval_string += "#{months}M";
          end

          if days != 0
            interval_string += "#{days}D";
          end

          if hours != 0 || minutes != 0 || seconds != 0
            interval_string += 'T';

            if hours != 0
                interval_string += "#{hours}H";
            end

            if minutes != 0
                interval_string += "#{minutes}M";
            end

            if seconds != 0
                interval_string += "#{seconds}S";
            end
          end

          if interval_string == 'P'
              return 'P0Y';
          end

          interval_string;
        end
      end
    end
  end
end
