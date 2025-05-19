# Copyright 2025 Google LLC
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
require "google/cloud/spanner/interval"

describe "Spanner Client", :interval, :spanner do

  it "parses for negative minutes" do
    interval = Interval.parse "P1Y2M3DT13H-48M6S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 14
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 43_926_000_000_000
  end

  it "parses with missing time part" do
    interval = Interval.parse "P1Y2M3D"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 14
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 0

  end 

  it "parses with missing day and time part" do
    interval = Interval.parse "P1Y2M" 
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 14
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 0

  end

  it "parses with year only" do
    interval = Interval.parse "P1Y"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 12
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 0
  end

  it "parses with month only" do
    interval = Interval.parse "P2M"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 2
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 0
  end

  it "parses with day only" do
    interval = Interval.parse "P3D"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 0
  end

  it "parses with time only" do
    interval = Interval.parse "PT4H25M6.7890001S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 15_906_789_000_100
  end

  it "parses with time only, no decimal point" do
    interval = Interval.parse "PT4H25M6S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 15_906_000_000_000
  end

  it "parses with no minute in time part" do
    interval = Interval.parse "PT4H30S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 14_430_000_000_000

  end

  it "parses with no second in time part" do
    interval = Interval.parse "PT4H1M"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 14_460_000_000_000
  end

  it "parses with only minute in time part" do
    interval = Interval.parse "PT5M"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 300_000_000_000
  end

  it "parses with only second in time part" do
    interval = Interval.parse "PT6.789S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 6_789_000_000
  end

  it "parses with only fractional second" do
    interval = Interval.parse "PT0.123S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 123_000_000 

  end

  it "parses with no 0 before decimal point" do
    interval = Interval.parse "PT.000000123S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 123
  end

  it "parses with no zero before decimal point" do
    interval = Interval.parse "PT.000000123S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 123
  end

  it "parses an interval of zero duration" do
    interval = Interval.parse "P0Y"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 0
  end

  it "parses with negative signs in each part" do
    interval = Interval.parse "P-1Y-2M-3DT-12H-12M-6.789000123S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal -14
    _(interval.instance_variable_get(:@days)).must_equal -3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal -43_926_789_000_123
  end

  it "parses with mixed signs" do
    interval = Interval.parse "P1Y-2M3DT13H-51M6.789S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 10
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 43_746_789_000_000 

  end

  it "parses with mixed signs (2)" do
    interval = Interval.parse "P-1Y2M-3DT-13H49M-6.789S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal -10
    _(interval.instance_variable_get(:@days)).must_equal -3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal -43_866_789_000_000

  end

  it "parses with mixed signs (3)" do
    interval = Interval.parse "P1Y2M3DT-4H25M-6.7890001S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 14
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal -12_906_789_000_100
  end

  it "parses with mixed signs (4)" do
    interval = Interval.parse "P-1Y2M3DT12H-30M1.234S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal -10
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 41_401_234_000_000
  end

  it "parses without normal bounds" do
    interval = Interval.parse "PT100H100M100.5S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 366_100_500_000_000
  end

  it "parses with date and time, no seconds" do
    interval = Interval.parse "P1Y2M3DT12H30M"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 14
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 45_000_000_000_000
  end

  it "parses fractional seconds with max digits" do
    interval = Interval.parse "PT0.123456789S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 123_456_789

  end

  it "parses hours and fractional seconds" do 
    interval = Interval.parse "PT1H0.5S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 3_600_500_000_000
  end

  it "parses with full interval representation" do
    interval = Interval.parse "P1Y2M3DT12H30M1.23456789S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 14
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 45_001_234_567_890 
  end

  it "parses comma as decimal point" do 
    interval = Interval.parse "P1Y2M3DT12H30M1,23456789S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 14
    _(interval.instance_variable_get(:@days)).must_equal 3
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 45_001_234_567_890

  end

  it "parses with trailing zeros after decimal" do 
    interval = Interval.parse "PT1.234000S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 1_234_000_000

  end

  it "parses with all zeros after decimal" do
    interval = Interval.parse "PT1.000S"
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.instance_variable_get(:@months)).must_equal 0
    _(interval.instance_variable_get(:@days)).must_equal 0
    _(interval.instance_variable_get(:@nanoseconds)).must_equal 1_000_000_000

  end
end


 
