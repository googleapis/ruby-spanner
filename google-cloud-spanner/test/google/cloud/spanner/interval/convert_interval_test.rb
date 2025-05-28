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

describe Google::Cloud::Spanner::Interval, :convert_interval do

  Interval = Google::Cloud::Spanner::Interval

  it "converts interval to ISO8601" do
    interval = Interval.new 14, 3, 43_926_789_000_123
    _(interval.to_s).must_equal "P1Y2M3DT12H12M6.789000123S"
  end

  it "converts interval to ISO8601 (2)" do
    interval = Interval.new 14, 3, 14706789000000
    _(interval.to_s).must_equal "P1Y2M3DT4H5M6.789S"
  end

  it "converts interval to ISO8601 (3)" do
    interval = Interval.new 10, 3, 43746789100000
    _(interval.to_s).must_equal "P10M3DT12H9M6.789100S"
  end


  it "converts interval to ISO8601, without nanoseconds" do
    interval = Interval.new 14, 3, 0
    _(interval.to_s).must_equal "P1Y2M3D"
  end

  it "converts interval to ISO8601, months to year and months" do
    interval = Interval.new 14, 0, 0
    _(interval.to_s).must_equal "P1Y2M"
  end

  it "converts interval to ISO8601, months to year" do
    interval = Interval.new 12, 0, 0
    _(interval.to_s).must_equal "P1Y"
  end

  it "converts interval to ISO8601, months only" do
    interval = Interval.new 2, 0, 0
    _(interval.to_s).must_equal "P2M"
  end

  it "converts interval to ISO8601, days only" do
    interval = Interval.new 0, 3, 0
    _(interval.to_s).must_equal "P3D"
  end

  it "converts interval to ISO8601, nanoseconds only" do
    interval = Interval.new 0, 0, 15906789000000 
    _(interval.to_s).must_equal "PT4H25M6.789S"
  end

  it "converts interval to ISO8601, nanoseconds only (2)" do
    interval = Interval.new 0, 0, 14430000000000
    _(interval.to_s).must_equal "PT4H30S"
  end

  it "converts interval to ISO8601, nanoseconds only (3)" do
    interval = Interval.new 0, 0, 300000000000
    _(interval.to_s).must_equal "PT5M"
  end

  it "converts interval to ISO8601, nanoseconds only (4)" do
    interval = Interval.new 0, 0, 6789000000
    _(interval.to_s).must_equal "PT6.789S"
  end

  it "converts interval to ISO8601, nanoseconds only (5)" do
    interval = Interval.new 0, 0, 123000000    
    _(interval.to_s).must_equal "PT0.123S"
  end

  it "converts interval to ISO8601, nanoseconds only (6)" do
    interval = Interval.new 0, 0, 123
    _(interval.to_s).must_equal "PT0.000000123S"
  end

  it "converts interval to ISO8601, nanoseconds only (7)" do
    interval = Interval.new 0, 0, 100000000
    _(interval.to_s).must_equal "PT0.100S"
  end

  it "converts interval to ISO8601, nanoseconds only (8)" do
    interval = Interval.new 0, 0, 100100000
    _(interval.to_s).must_equal "PT0.100100S"
  end

  it "converts interval to ISO8601, nanoseconds only (9)" do
    interval = Interval.new 0, 0, 100100100
    _(interval.to_s).must_equal "PT0.100100100S"
  end

  it "converts interval to ISO8601, nanoseconds only (10)" do
    interval = Interval.new 0, 0, 9
    _(interval.to_s).must_equal "PT0.000000009S"
  end

  it "converts interval to ISO8601, nanoseconds only (11)" do
    interval = Interval.new 0, 0, 9000
    _(interval.to_s).must_equal "PT0.000009S"
  end

  it "converts interval to ISO8601, nanoseconds only (12)" do
    interval = Interval.new 0, 0, 9000000
    _(interval.to_s).must_equal "PT0.009S"
  end

  it "converts an empty interval to ISO8601" do
    interval = Interval.new 0, 0, 0
    _(interval.to_s).must_equal "P0Y"
  end

  it "converts interval to ISO8601, with negative values" do
    interval = Interval.new -14, -3, -43926789000123
    _(interval.to_s).must_equal "P-1Y-2M-3DT-12H-12M-6.789000123S"
  end

  it "converts interval to ISO8601, with negative values (2)" do
    interval = Interval.new -10, -3, -43866789010000
    _(interval.to_s).must_equal "P-10M-3DT-12H-11M-6.789010S"
  end

  it "converts interval to ISO8601, with negative values (3)" do
    interval = Interval.new 14, 3, -12906662400000
    _(interval.to_s).must_equal "P1Y2M3DT-3H-35M-6.662400S"
  end

  it "converts interval to ISO8601, with fractional seconds" do
    interval = Interval.new 0, 0, 500000000
    _(interval.to_s).must_equal "PT0.500S"
  end

  it "converts interval to ISO8601, with negative fractional seconds" do
    interval = Interval.new 0, 0, -500000000
    _(interval.to_s).must_equal "PT-0.500S"
  end

  it "converts interval to ISO8601, with large values" do
    interval = Interval.new 0, 0, 316224000000000000000
    _(interval.to_s).must_equal "PT87840000H"
  end

  it "converts interval to ISO8601, with large values (2)" do
    interval = Interval.new 25, 15, 316223999999999999999 
    _(interval.to_s).must_equal "P2Y1M15DT87839999H59M59.999999999S"
  end

  it "converts interval to ISO8601, with large negative values" do
    interval = Interval.new 0, 0, -316224000000000000000
    _(interval.to_s).must_equal "PT-87840000H"
  end

  it "converts interval to ISO8601, with large negative values (2)" do
    interval = Interval.new 25, 15, -316223999999999999999
    _(interval.to_s).must_equal "P2Y1M15DT-87839999H-59M-59.999999999S"
  end

  it "converts interval to ISO8601, with nanoseconds normalized to hours" do
    interval = Interval.new 0, 0, 86400000000000
    _(interval.to_s).must_equal "PT24H"
  end

  it "converts interval to ISO8601, without normalizing days to months" do
    interval = Interval.new 0, 31, 0
    _(interval.to_s).must_equal "P31D"
  end

  it "converts interval to ISO8601, normalizing negative months to years" do
    interval = Interval.new -12, 0, 0
    _(interval.to_s).must_equal "P-1Y"
  end
end
