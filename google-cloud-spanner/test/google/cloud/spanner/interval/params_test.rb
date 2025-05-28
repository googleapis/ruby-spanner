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

describe Google::Cloud::Spanner::Interval, :params do

  it "initializes using `from_months`" do
    interval = Interval.from_months 14
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.months).must_equal 14
    _(interval.days).must_equal 0
    _(interval.nanoseconds).must_equal 0
    _(interval.to_s).must_equal "P1Y2M"
  end

  it "initializes using `from_days`" do
    interval = Interval.from_days 36
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.months).must_equal 0
    _(interval.days).must_equal 36
    _(interval.nanoseconds).must_equal 0
    _(interval.to_s).must_equal "P36D"
  end

  it "initializes using `from_seconds`" do
    interval = Interval.from_seconds 360
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.months).must_equal 0
    _(interval.days).must_equal 0
    _(interval.nanoseconds).must_equal 360_000_000_000
    _(interval.to_s).must_equal "PT6M"
  end

  it "initializes using `from_milliseconds`" do
    interval = Interval.from_milliseconds 2000
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.months).must_equal 0
    _(interval.days).must_equal 0
    _(interval.nanoseconds).must_equal 2_000_000_000
    _(interval.to_s).must_equal "PT2S"
  end

  it "initializes using `from_microseconds`" do
    interval = Interval.from_microseconds 5000
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.months).must_equal 0
    _(interval.days).must_equal 0
    _(interval.nanoseconds).must_equal 5_000_000
    _(interval.to_s).must_equal "PT0.005S"
  end

  it "initializes using `from_nanoseconds`" do
    interval = Interval.from_nanoseconds -2_500_000
    _(interval).must_be_kind_of Google::Cloud::Spanner::Interval
    _(interval.months).must_equal 0
    _(interval.days).must_equal 0
    _(interval.nanoseconds).must_equal -2_500_000
    _(interval.to_s).must_equal "PT-0.002500S"
  end

  it "equals another interval if nanoseconds match" do
    interval = Interval.from_seconds 3_600
    other_interval = Interval.from_milliseconds 3_600_000
    _(interval).must_equal other_interval
  end

  it "does not equal another interval if using days vs seconds" do
    interval = Interval.from_seconds 86400
    other_interval = Interval.from_days 1
    refute_equal interval, other_interval
  end
end
