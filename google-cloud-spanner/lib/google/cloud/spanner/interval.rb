# Copyright 2017 Google LLC
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

                def self.fromIso8601(intervalString)
                    pattern =  /(?!$)(-?\d+Y)?(-?\d+M)?(-?\d+D)?(T(?=-?.?\d)(-?\d+H)?(-?\d+M)?(-?(((\d*)((\.|,)\d{1,9})?)|(\.\d{1,9}))S)?)?$/
                    intervalMonths = 0
                    intervalDays = 0
                    intervalNanoseconds = 0

                    captures = intervalString.match(pattern).captures
                    if captures.length == 0
                        raise ArgumentError, "The ISO8601 provided was not in the correct format"
                    end

                    filter = Set[YEARS, MONTHS, DAYS, HOURS, MINUTES, SECONDS]
                    captures.each_with_index do |value, index|
                        if !filter.include?(index)
                            next
                        end

                        numericValue = value.gsub(/[^0-9.,-]/, "")

                        case index
                        when YEARS
                            intervalMonths += Interval.years_to_months(numericValue)
                        when MONTHS
                            intervalMonths += Integer(numericValue)
                        when DAYS
                            intervalDays = Integer(numericValue)
                        when HOURS
                            intervalNanoseconds += Interval.hours_to_nanoseconds(numericValue)
                        when MINUTES
                            intervalNanoseconds += Interval.minutes_to_nanoseconds(numericValue)
                        when SECONDS
                            intervalNanoseconds += Interval.seconds_to_nanoseconds(numericValue)
                        end
                    end

                    Interval.new(intervalMonths, intervalDays, intervalNanoseconds);
                end

                def self.years_to_months(years)
                    Integer(years) * 12
                end

                def self.hours_to_nanoseconds(hours)
                    Integer(hours) * NANOSECONDS_IN_AN_HOUR
                end

                def self.minutes_to_nanoseconds(minutes)
                    Integer(minutes) * NANOSECONDS_IN_A_MINUTE
                end

                def self.seconds_to_nanoseconds(seconds)
                    # We only support up to nanoseconds of precision
                    splitSeconds = seconds.split('.')
                    if splitSeconds.length > 2 && splitSeconds[1].length > 9
                        raise ArgumentError, "The seconds portion of the interval only supports up to nanoseconds."
                    end

                    Float(seconds) * NANOSECONDS_IN_A_SECOND
                end

                def self.from_months(months)
                    Interval.new(months, 0, 0)
                end

                def self.from_days(days)
                    Interval.new(0, days, 0)
                end

                def self.from_seconds(seconds)
                    nanoseconds = Interval.seconds_to_nanoseconds(seconds)
                    Interval.new(0, 0, nanoseconds);
                end

                def self.from_nanoseconds(nanoseconds)
                    Interval.new(0, 0, nanoseconds)
                end

                def to_s
                    years = 0
                    months = 0
                    days = @days;
                    hours = 0
                    minutes = 0
                    seconds = 0
                    remainingNanoseconds = @nanoseconds;

                    years = @months / 12;
                    months = @months % 12;
                    hours = Integer(remainingNanoseconds / 3600000000000);
                    remainingNanoseconds %= 3600000000000;
                    minutes = Integer(remainingNanoseconds / 60000000000);
                    remainingNanoseconds %= 60000000000;
                    seconds = remainingNanoseconds / 1000000000;

                    intervalString = "P";

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
                        intervalString += "T";

                        if hours != 0
                            intervalString += "#{hours}H";
                        end

                        if minutes != 0
                            intervalString += "#{minutes}M";
                        end

                        if seconds != 0
                            if seconds % 1 == 0
                                intervalString += "#{Integer(seconds)}S";
                            else
                                intervalString += "#{seconds}S";
                            end
                        end
                    end

                    if intervalString == "P"
                        return "P0Y";
                    end

                    return intervalString;
                end

                private
                def initialize(months, days, nanoseconds)
                    @months = months
                    @days = days
                    @nanoseconds = nanoseconds
                end
            end
        end
    end
end