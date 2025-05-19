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

describe "Spanner Interval parsing", :interval, :spanner do

  it "fails on arbitrary strings" do
    assert_raises ArgumentError do
      Interval.parse "invalid"
    end
  end

  it "fails on incomplete format" do
    assert_raises ArgumentError do
      Interval.parse "P"
    end
  end

  it "fails on incomplete format (2)" do
    assert_raises ArgumentError do
      Interval.parse "PT"
    end
  end

  it "fails on incomplete format (3)" do
    assert_raises ArgumentError do
      Interval.parse "P1YM"
    end
  end

  it "fails if missing T separator" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3D4H5M6S"
    end
  end

  it "fails if missing value after decimal point" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3DT4H5M6.S"
    end
  end

  it "fails with an extra seconds character" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3DT4H5M6.789SS"
    end
  end

  it "fails with non-digit characters after decimal point" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3DT4H5M6.ABC"
    end
  end

  it "fails when missing unit specifier" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3"
    end
  end

  it "fails when missing time components after T" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3DT"
    end
  end


  it "fails with invalid sign position" do
    assert_raises ArgumentError do
      Interval.parse "P-T1H"
    end
  end

  it "fails with invalid sign position (2)" do
    assert_raises ArgumentError do
      Interval.parse "PT1H-"
    end
  end

  it "fails with too many decimals (maximum of 9)" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3DT4H5M6.789123456789S"
    end
  end

  it "fails with multiple decimal points" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3DT4H5M6.123.456S"
    end
  end

  it "fails if both period and comma are used" do
    assert_raises ArgumentError do
      Interval.parse "P1Y2M3DT4H5M6.,789S"
    end
  end

end

