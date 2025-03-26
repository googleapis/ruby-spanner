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
          :afterP,
          :afterY,
          :afterMonth,
          :afterD,
          :afterT,
          :afterH,
          :afterMins,
          :nextAllowed,
          :start,
          :isTime,
          :mayBeTerminal,
          :isTerminal,
          :isValidResolution,
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

            while state.start < text.length && !state.isTerminal do
              current = text.index state.nextAllowed, state.start

              if current.nil?
                raise ArgumentError, "Unsupported Format: #{text}"
              end

              case text[current]
              when 'P'
                state.mayBeTerminal = false
                state.isTerminal = false
                state.isTime = false
                state.isValidResolution = true
                state.nextAllowed = state.afterP
              when 'Y'
                state.mayBeTerminal = true;
                state.isTerminal = false;
                state.isValidResolution = true;
                state.years = Integer text[state.start, current - state.start]
                state.nextAllowed = state.afterY;
              when 'M'
                if state.isTime
                  state.mayBeTerminal = true;
                  state.isTerminal = false;
                  state.isValidResolution = true;
                  state.minutes = Integer text[state.start, current - state.start]
                  state.nextAllowed = state.afterMins;
                else
                  state.mayBeTerminal = true;
                  state.isTerminal = false;
                  state.isValidResolution = true;
                  state.months = Integer text[state.start, current - state.start]
                  state.nextAllowed = state.afterMonth;
                end
              when 'D'
                state.mayBeTerminal = true;
                state.isTerminal = false;
                state.isValidResolution = true;
                state.days = Integer text[state.start, current - state.start]
                state.nextAllowed = state.afterD;
              when 'T'
                state.mayBeTerminal = false;
                state.isTerminal = false;
                state.isTime = true;
                state.isValidResolution = true;
                state.nextAllowed = state.afterT;
              when 'H'
                state.mayBeTerminal = true
                state.isTerminal = false
                state.isValidResolution = true
                state.hours = Integer text[state.start, current - state.start]
                state.nextAllowed = state.afterH
              when 'S'
                state.mayBeTerminal = true
                state.isTerminal = true
                state.isValidResolution = self.isValidResolution text[state.start, current - state.start]
                state.seconds = Float text[state.start, current - state.start]
                state.nextAllowed = nil;
              else
                raise ArgumentError, "Unsupported Format: #{text}"
              end

              state.start = current + 1
            end

            if state.isTerminal && state.start < text.length
              raise ArgumentError, "Unsupported format: #{text}"
            end

            unless state.mayBeTerminal
              raise ArgumentError, "Unsupported format: #{text}"
            end

            unless state.isValidResolution
              raise ArgumentError, 'The interval class only supports a resolution up to nanoseconds'
            end

            totalMonths = self.yearsToMonths(state.years) + state.months
            totalNanoseconds = self.hoursToNanoseconds(state.hours) + self.minutesToNanoseconds(state.minutes) + self.secondsToNanoseconds(state.seconds)

            Interval.new totalMonths, state.days, totalNanoseconds
          end

          private

          def yearsToMonths years
            years * 12
          end

          def hoursToNanoseconds hours
            hours * self::NANOSECONDS_IN_AN_HOUR
          end

          def minutesToNanoseconds minutes
            minutes * self::NANOSECONDS_IN_A_MINUTE
          end

          def secondsToNanoseconds seconds
            seconds * self::NANOSECONDS_IN_A_SECOND
          end

          def isValidResolution textValue
            integer_value, decimal_value = textValue.gsub(',', '.').split('.')

            # Not a decimal, so is valid
            if decimal_value.nil? || decimal_value.empty?
              return true
            end

            # More than 9 digits after the decimal point, not supported
            if decimal_value.length > 9
              return false
            end

            true
          end
        end

        def initialize
          @internalVariable = 1;
        end

        def to_s
          @stringRepresentation ||= self.to_string
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
          remainingNanoseconds = @nanoseconds

          years, months = @months.divmod 12
          hours, remainingNanoseconds = remainingNanoseconds.divmod self.class::NANOSECONDS_IN_AN_HOUR
          minutes, remainingNanoseconds = remainingNanoseconds.divmod self.class::NANOSECONDS_IN_A_MINUTE
          seconds = remainingNanoseconds / self.class::NANOSECONDS_IN_A_SECOND

          intervalString = 'P';

          if years != 0
            intervalString += "#{years}Y";
          end

          if months != 0
            intervalString += "#{months}M";
          end

          if days != 0
            intervalString += "#{days}D";
          end

          if hours != 0 || minutes != 0 || seconds != 0
            intervalString += 'T';

            if hours != 0
                intervalString += "#{hours}H";
            end

            if minutes != 0
                intervalString += "#{minutes}M";
            end

            if seconds != 0
                intervalString += "#{seconds}S";
            end
          end

          if intervalString == 'P'
              return 'P0Y';
          end

          intervalString;
        end
      end
    end
  end
end
