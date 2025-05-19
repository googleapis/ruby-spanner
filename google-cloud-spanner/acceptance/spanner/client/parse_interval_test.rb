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

require "spanner_helper"
require "google/cloud/spanner/interval"

describe "Spanner Client", :interval, :spanner do

  it "parses for negative minutes" do
    interval = Interval.parse "P1Y2M3DT13H-48M6S"
  end

  it "parses with missing time part" do
    interval = Interval.parse "P1Y2M3D"
  end 

  it "parses with missing day and time part" do
    interval = Interval.parse "P1Y2M" 
  end

  it "parses with year only" do
    interval = Interval.parse "P1Y"
  end

  it "parses with month only" do
    interval = Interval.parse "P2M"
  end

  it "parses with day only" do
    interval = Interval.parse "P3D"
  end

  it "parses with time only" do
    interval = Interval.parse "PT4H25M6.7890001S"
  end

  it "parses with time only, no decimal point" do
    interval = Interval.parse "PT4H25M6S"
  end

  it "parses with no minute in time part" do
    interval = Interval.parse "PT4H30S"
  end

  it "parses with no second in time part" do
    interval = Interval.parse "PT4H1M"
  end

  it "parses with only minute in time part" do
    interval = Interval.parse "PT5M"
  end

  it "parses with only second in time part" do
    interval = Interval.parse "PT6.789S"
  end


  it "parses with only fractional second" do
    interval = Interval.parse "PT0.123S"
  end

  it "parses with no 0 before decimal point" do
    interval = Interval.parse "PT.000000123S"
  end

  it "parses with no zero before decimal point" do
    interval = Interval.parse "PT.000000123S"
  end


  it "parses an interval of zero duration" do
    interval = Interval.parse "P0Y"
  end

  it "parses with negative signs in each part" do
    interval = Interval.parse "P-1Y-2M-3DT-12H-12M-6.789000123S"
  end

  it "parses with mixed signs" do
    interval = Interval.parse "P1Y-2M3DT13H-51M6.789S"
  end

  it "parses with mixed signs (2)" do
    interval = Interval.parse "P-1Y2M-3DT-13H49M-6.789S"
  end

  it "parses with mixed signs (3)" do
    interval = Interval.parse "P1Y2M3DT-4H25M-6.7890001S"
  end

  it "parses with mixed signs (3)" do
    interval = Interval.parse "P1Y2M3DT-4H25M-6.7890001S"
  end

  it "parses with date and time, no seconds" do
    interval = Interval.parse "P1Y2M3DT12H30M"
  end

  it "parses fractional seconds with max digits" do
    interval = Interval.parse "PT0.123456789S"
  end

  it "parses hours and fractional seconds" do 
    interval = Interval.parse "PT1H0.5S"
  end

  it "parses with full interval representation" do
    interval = Interval.parse "P1Y2M3DT12H30M1.23456789S"
  end

  it "parses comma as decimal point" do 
    interval = Interval.parse "P1Y2M3DT12H30M1,23456789S"
  end

  it "parses with trailing zeros after decimal" do 
    interval = Interval.parse "PT1.234000S"
  end

  it "parses with all zeros after decimal" do
    interval = Interval.parse "PT1.000S"
  end
end


    
